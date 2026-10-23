"""/group: start a 2-4 player Token Arcade group from Discord.

/group shows a game picker. Picking a game posts a group card in the channel
with Join and Leave buttons. The buttons keep working after a bot restart
(their group id lives in the button's custom id), and the arcade API stays
the source of truth for who is in each group.
"""
import logging
import os

import aiohttp
import discord

log = logging.getLogger('arcade-groups')

# Games that support multiplayer groups. Keys match the arcade's games.json ids.
GAMES = {
    'goldeneye007': {'title': 'GoldenEye 007', 'emoji': '🔫', 'max': 4},
    'wcwvsnworevenge': {'title': 'WCW vs NWO Revenge', 'emoji': '🤼', 'max': 4},
}
GROUP_ID = r'(?P<gid>[A-Za-z0-9_-]{8,24})'
COLOR = 0x2ED7E9


def api_base():
    explicit = os.getenv('ARCADE_API_URL', '').rstrip('/')
    if explicit:
        return explicit
    relay = os.getenv('ARCADE_RELAY_URL', '')
    if relay.endswith('/relay'):
        return relay[:-len('/relay')]
    return 'https://api.discordarcade.com/api/arcade'


def site_url():
    return os.getenv('ARCADE_SITE_URL', 'https://discordarcade.com').rstrip('/')


def group_url(gid):
    return f'{site_url()}/?group={gid}'


class ArcadeError(Exception):
    def __init__(self, status, detail):
        super().__init__(detail)
        self.status = status
        self.detail = detail


async def call(method, path, payload=None):
    secret = os.getenv('ARCADE_RELAY_SECRET', '')
    if len(secret) < 32:
        raise ArcadeError(503, 'Groups are not set up yet (ARCADE_RELAY_SECRET).')
    timeout = aiohttp.ClientTimeout(total=15)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.request(method, api_base() + path, json=payload,
                                       headers={'Authorization': 'Bearer ' + secret},
                                       allow_redirects=False) as response:
                try:
                    data = await response.json(content_type=None)
                except ValueError:
                    data = {}
                if response.status >= 400:
                    raise ArcadeError(response.status, (data or {}).get('detail') or 'The arcade is unavailable.')
                return data
    except aiohttp.ClientError:
        raise ArcadeError(502, 'The arcade is unavailable right now. Try again shortly.') from None


def player_payload(user: discord.abc.User):
    return {'user': {'id': str(user.id), 'name': getattr(user, 'display_name', None) or user.name,
                     'avatar': str(user.display_avatar.url)}}


def card(group):
    game = GAMES.get(group.get('game'), {'title': group.get('title', 'Game'), 'emoji': '🎮', 'max': 4})
    players = group.get('players', [])
    ended = group.get('status') == 'ended'
    by_slot = {p['slot']: p for p in players}
    lines = []
    for slot in range(1, game['max'] + 1):
        player = by_slot.get(slot)
        if player:
            tag = ' · host' if player['id'] == group.get('host') else ''
            lines.append(f"**P{slot}** <@{player['id']}>{tag}")
        elif not ended:
            lines.append(f'**P{slot}** open')
    if ended:
        description = 'This group has ended.'
    elif group.get('status') == 'playing':
        description = 'Game in progress. Open slots can still join.'
    else:
        description = ('2–4 players · Premium required.\n'
                       'Press **Join**, then **Open arcade** to play in your browser.')
    embed = discord.Embed(title=f"{game['emoji']} {game['title']} · Multiplayer group",
                          description=description, color=0x6B6B7B if ended else COLOR)
    embed.add_field(name=f"Players {len(players)}/{game['max']}", value='\n'.join(lines) or '—', inline=False)
    embed.set_footer(text='Token Arcade')
    return embed


def card_view(group):
    view = discord.ui.View(timeout=None)
    gid = group['id']
    ended = group.get('status') == 'ended'
    full = len(group.get('players', [])) >= GAMES.get(group.get('game'), {}).get('max', 4)
    view.add_item(JoinButton(gid, disabled=ended or full))
    view.add_item(LeaveButton(gid, disabled=ended))
    if not ended:
        view.add_item(discord.ui.Button(label='Open arcade', style=discord.ButtonStyle.link, url=group_url(gid)))
    return view


def link_view(gid):
    view = discord.ui.View(timeout=None)
    view.add_item(discord.ui.Button(label='Open arcade', style=discord.ButtonStyle.link, url=group_url(gid)))
    return view


async def refresh_card(interaction: discord.Interaction, group):
    try:
        await interaction.message.edit(embed=card(group), view=card_view(group))
    except discord.HTTPException as error:
        log.warning('group card edit failed: %s', error)


def friendly(error: ArcadeError):
    if error.status == 403 and 'Premium' in error.detail:
        return 'You need Token Premium to play in groups.'
    if error.status in (404, 410):
        return 'This group has ended. Start a new one with `/group`.'
    if error.status == 409:
        return 'This group is full.'
    return error.detail


class JoinButton(discord.ui.DynamicItem[discord.ui.Button], template=r'arcade-group:join:' + GROUP_ID):
    def __init__(self, gid: str, disabled: bool = False):
        super().__init__(discord.ui.Button(label='Join', style=discord.ButtonStyle.success,
                                           custom_id=f'arcade-group:join:{gid}', disabled=disabled))
        self.gid = gid

    @classmethod
    async def from_custom_id(cls, interaction, item, match):
        return cls(match['gid'])

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            group = await call('POST', f'/bot/groups/{self.gid}/join', player_payload(interaction.user))
        except ArcadeError as error:
            if error.status in (404, 410):
                await refresh_card(interaction, {'id': self.gid, 'status': 'ended', 'players': []})
            await interaction.followup.send(friendly(error), ephemeral=True)
            return
        await refresh_card(interaction, group)
        slot = next((p['slot'] for p in group['players'] if p['id'] == str(interaction.user.id)), None)
        await interaction.followup.send(f"You're **Player {slot}**. Open the arcade to play:",
                                        view=link_view(self.gid), ephemeral=True)


class LeaveButton(discord.ui.DynamicItem[discord.ui.Button], template=r'arcade-group:leave:' + GROUP_ID):
    def __init__(self, gid: str, disabled: bool = False):
        super().__init__(discord.ui.Button(label='Leave', style=discord.ButtonStyle.secondary,
                                           custom_id=f'arcade-group:leave:{gid}', disabled=disabled))
        self.gid = gid

    @classmethod
    async def from_custom_id(cls, interaction, item, match):
        return cls(match['gid'])

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            group = await call('POST', f'/bot/groups/{self.gid}/leave', player_payload(interaction.user))
        except ArcadeError as error:
            if error.status in (404, 410):
                await refresh_card(interaction, {'id': self.gid, 'status': 'ended', 'players': []})
            await interaction.followup.send(friendly(error), ephemeral=True)
            return
        await refresh_card(interaction, group)
        ended = group.get('status') == 'ended'
        await interaction.followup.send('You ended the group.' if ended else 'You left the group.', ephemeral=True)


class GamePicker(discord.ui.View):
    def __init__(self, owner_id: int):
        super().__init__(timeout=120)
        self.owner_id = owner_id
        select = discord.ui.Select(placeholder='Choose a multiplayer game', options=[
            discord.SelectOption(label=g['title'], value=key, emoji=g['emoji'], description=f"2–{g['max']} players")
            for key, g in GAMES.items()])
        select.callback = self.picked
        self.add_item(select)

    async def picked(self, interaction: discord.Interaction):
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message('Run `/group` to start your own group.', ephemeral=True)
            return
        game = interaction.data['values'][0]
        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            group = await call('POST', '/bot/groups', {
                'game': game, 'guild_id': str(interaction.guild_id), 'channel_id': str(interaction.channel_id),
                **player_payload(interaction.user)})
        except ArcadeError as error:
            await interaction.followup.send(friendly(error), ephemeral=True)
            return
        try:
            await interaction.channel.send(embed=card(group), view=card_view(group),
                                           allowed_mentions=discord.AllowedMentions.none())
        except discord.HTTPException:
            await interaction.followup.send("I couldn't post in this channel. Check my Send Messages permission.",
                                            ephemeral=True)
            return
        self.stop()
        await interaction.followup.send("Your group is open. You're **Player 1** (host). "
                                        'Open the arcade and press **Start** when your friends are in:',
                                        view=link_view(group['id']), ephemeral=True)


def install_groups(client: discord.Client, tree: discord.app_commands.CommandTree):
    client.add_dynamic_items(JoinButton, LeaveButton)

    @tree.command(name='group', description='Start a multiplayer game group in Token Arcade')
    @discord.app_commands.guild_only()
    async def group_cmd(interaction: discord.Interaction):
        await interaction.response.send_message('Which game do you want to play together?',
                                                view=GamePicker(interaction.user.id), ephemeral=True)

    return group_cmd

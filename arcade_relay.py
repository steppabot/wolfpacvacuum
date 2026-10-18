"""Bounded multi-server invalidation relay. Never archives or deletes messages."""
import asyncio
import logging
import os
from collections import OrderedDict
from functools import wraps
import aiohttp
import discord

log = logging.getLogger('arcade-relay')

def install_relay(client):
    if getattr(client, 'arcade_relay_task', None):
        return
    url = os.getenv('ARCADE_RELAY_URL', '')
    secret = os.getenv('ARCADE_RELAY_SECRET', '')
    if not url or not secret:
        log.warning('Arcade relay disabled: configure ARCADE_RELAY_URL and ARCADE_RELAY_SECRET.')
        return
    if not url.startswith('https://') or len(secret) < 32:
        raise RuntimeError('Invalid arcade relay configuration.')
    pending = OrderedDict()
    wake = asyncio.Event()

    def changed(guild_id, channel_id):
        if not guild_id or not channel_id:
            return
        channel = client.get_channel(int(channel_id))
        if not isinstance(channel, discord.TextChannel) or channel.guild.id != int(guild_id) or channel.is_nsfw():
            return
        me = channel.guild.me
        if not me:
            return
        perms = channel.permissions_for(me)
        if not perms.view_channel or not perms.read_message_history:
            return
        key = (str(guild_id), str(channel_id))
        pending[key] = None
        if len(pending) > 2000:
            pending.popitem(last=False)  # Website polling repairs dropped invalidations.
        wake.set()

    async def message_event(message):
        if message.guild:
            changed(message.guild.id, message.channel.id)

    async def raw_event(payload):
        changed(payload.guild_id, payload.channel_id)

    def attach(name, handler):
        previous = getattr(client, name, None)
        async def combined(*args, **kwargs):
            try:
                await handler(*args, **kwargs)
            finally:
                if previous:
                    await previous(*args, **kwargs)
        combined.__name__ = name
        client.event(combined)

    attach('on_message', message_event)

    for name in (
        'on_raw_message_edit',
        'on_raw_message_delete',
        'on_raw_bulk_message_delete',
        'on_raw_reaction_add',
        'on_raw_reaction_remove',
        'on_raw_reaction_clear',
        'on_raw_reaction_clear_emoji',
    ):
        attach(name, raw_event)

    async def worker():
        delay = 1
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
            while True:
                await wake.wait()
                await asyncio.sleep(0.25)
                if not pending:
                    wake.clear()
                    continue
                (guild, channel), _ = pending.popitem(last=False)
                if not pending:
                    wake.clear()
                try:
                    async with session.post(url, headers={'Authorization': 'Bearer ' + secret},
                        json={'guild_id': guild, 'channel_id': channel}, allow_redirects=False) as response:
                        if response.status != 200:
                            raise RuntimeError('Relay HTTP ' + str(response.status))
                    delay = 1
                except (aiohttp.ClientError, asyncio.TimeoutError, RuntimeError):
                    log.warning('Arcade relay unavailable; retrying in %ss', delay)
                    changed(guild, channel)
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, 30)

    client.arcade_relay_task = asyncio.create_task(worker())

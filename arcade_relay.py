"""Bounded, retrying invalidation relay. No messages are archived or deleted here."""
import asyncio
import logging
import os
import time
import aiohttp

log = logging.getLogger('arcade-relay')


def install_relay(client):
    url = os.getenv('ARCADE_RELAY_URL', '')
    secret = os.getenv('ARCADE_RELAY_SECRET', '')
    guild = os.getenv('ARCADE_GUILD_ID', '')
    channels = set(filter(None, (x.strip() for x in os.getenv('ARCADE_CHANNEL_IDS', '').split(','))))
    if not url or not secret:
        log.warning('Arcade relay disabled: configure ARCADE_RELAY_URL and ARCADE_RELAY_SECRET.')
        return
    if not url.startswith('https://') or len(secret) < 32 or not guild.isdigit() or not channels or len(channels) > 20 or not all(x.isdigit() for x in channels):
        raise RuntimeError('Invalid arcade relay configuration.')
    pending = set()
    wake = asyncio.Event()

    def changed(guild_id, channel_id):
        channel = str(channel_id)
        if str(guild_id) == guild and channel in channels:
            pending.add(channel)
            wake.set()

    @client.event
    async def on_message(message):
        # Include bot messages: website sends also need to appear for everyone.
        if message.guild:
            changed(message.guild.id, message.channel.id)

    @client.event
    async def on_raw_message_edit(payload):
        changed(payload.guild_id, payload.channel_id)

    @client.event
    async def on_raw_message_delete(payload):
        changed(payload.guild_id, payload.channel_id)

    @client.event
    async def on_raw_bulk_message_delete(payload):
        changed(payload.guild_id, payload.channel_id)

    async def worker():
        delay = 1
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
            while True:
                await wake.wait()
                await asyncio.sleep(0.25)
                channel = next(iter(pending))
                pending.discard(channel)
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

    # Hold a reference to the task for the lifetime of the client.
    client.arcade_relay_task = asyncio.create_task(worker())
    # Refresh connected websites after startup/reconnect gaps via their fallback timer.

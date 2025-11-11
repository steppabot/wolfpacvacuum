import asyncio
import os
import sys
import logging
import datetime as dt
from zoneinfo import ZoneInfo

import asyncpg
import discord
import boto3
from botocore.config import Config as BotoConfig
from discord import Intents
from discord.ext import tasks
from dotenv import load_dotenv

load_dotenv()

# basic logging
logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                    format='[%(asctime)s] %(levelname)s %(name)s: %(message)s')
log = logging.getLogger("archivebot")

# ----------------------- Env -----------------------
TOKEN = os.getenv("DISCORD_TOKEN")
DB_URL = os.getenv("DATABASE_URL")
ARCHIVE_TZ = os.getenv("ARCHIVE_TZ", "America/Chicago")
TZ = ZoneInfo(ARCHIVE_TZ)

MIRROR_ATTACHMENTS = os.getenv("MIRROR_ATTACHMENTS", "true").lower() in {"1", "true", "yes", "on"}
MAX_ATTACHMENT_BYTES = int(os.getenv("MAX_ATTACHMENT_BYTES", str(12 * 1024 * 1024)))  # 12 MB
S3_PREFIX = os.getenv("S3_PREFIX", "discord-archive/").strip()
S3_PUBLIC_URL_BASE = os.getenv("S3_PUBLIC_URL_BASE", "").rstrip("/")  # optional CDN base

# Prefer Stackhero MinIO vars; fallback to AWS if you ever switch
STACKHERO_ENDPOINT = os.getenv("STACKHERO_S3_ENDPOINT") or os.getenv("STACKHERO_MINIO_HOST")
STACKHERO_ACCESS_KEY = os.getenv("STACKHERO_S3_ACCESS_KEY") or os.getenv("STACKHERO_MINIO_ROOT_ACCESS_KEY")
STACKHERO_SECRET_KEY = os.getenv("STACKHERO_S3_SECRET_KEY") or os.getenv("STACKHERO_MINIO_ROOT_SECRET_KEY")
STACKHERO_BUCKET     = os.getenv("STACKHERO_S3_BUCKET") or os.getenv("STACKHERO_MINIO_BUCKET")
if STACKHERO_ENDPOINT and not STACKHERO_ENDPOINT.startswith("http"):
    STACKHERO_ENDPOINT = f"https://{STACKHERO_ENDPOINT}"

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET = os.getenv("S3_BUCKET")

# ---------------------- Discord ---------------------
intents = Intents.default()
intents.message_content = True
intents.members = True
intents.guilds = True

client = discord.Client(intents=intents)
tree = discord.app_commands.CommandTree(client)
_pool: asyncpg.Pool | None = None

# ----------------------- SQL ------------------------
CREATE_SQL = """
CREATE TABLE IF NOT EXISTS archived_messages (
    message_id      BIGINT PRIMARY KEY,
    guild_id        BIGINT NOT NULL,
    channel_id      BIGINT NOT NULL,
    user_id         BIGINT NOT NULL,
    display_name    TEXT   NOT NULL,
    ts_utc          TIMESTAMPTZ NOT NULL,
    ts_local_date   DATE   NOT NULL,
    ts_local_time   TEXT   NOT NULL,
    role_color_1    TEXT   NULL,
    role_color_2    TEXT   NULL,
    avatar_url      TEXT   NULL,
    content         TEXT   NULL
);

CREATE TABLE IF NOT EXISTS archived_attachments (
    message_id      BIGINT NOT NULL REFERENCES archived_messages(message_id) ON DELETE CASCADE,
    attachment_id   BIGINT NOT NULL,
    filename        TEXT   NOT NULL,
    content_type    TEXT   NULL,
    size_bytes      BIGINT NOT NULL,
    url             TEXT   NULL,
    proxy_url       TEXT   NULL,
    sha256_hex      TEXT   NULL,
    s3_key          TEXT   NULL,
    s3_url          TEXT   NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (message_id, attachment_id)
);

CREATE TABLE IF NOT EXISTS archived_embeds (
    message_id  BIGINT NOT NULL REFERENCES archived_messages(message_id) ON DELETE CASCADE,
    embed_idx   INT    NOT NULL,
    embed       JSONB  NOT NULL,
    PRIMARY KEY (message_id, embed_idx)
);

CREATE INDEX IF NOT EXISTS idx_archived_messages_by_date
ON archived_messages (ts_local_date, guild_id, channel_id);
"""

UPSERT_SQL = """
INSERT INTO archived_messages (
    message_id, guild_id, channel_id, user_id,
    display_name, ts_utc, ts_local_date, ts_local_time,
    role_color_1, role_color_2, avatar_url, content
) VALUES (
    $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12
)
ON CONFLICT (message_id) DO UPDATE SET
    display_name = EXCLUDED.display_name,
    role_color_1 = EXCLUDED.role_color_1,
    role_color_2 = EXCLUDED.role_color_2,
    avatar_url   = EXCLUDED.avatar_url,
    content      = EXCLUDED.content;
"""

# --------------------- Helpers ----------------------

def hex_color(c: discord.Color | None):
    if not c or c.value == 0:
        return None
    return f"#{c.value:06x}"

async def get_role_colors(member: discord.Member):
    roles = [r for r in sorted(member.roles, key=lambda r: r.position) if r.color and r.color.value != 0]
    if not roles:
        return None, None
    primary = hex_color(roles[-1].color)
    secondary = None
    for r in reversed(roles[:-1]):
        if hex_color(r.color) != primary:
            secondary = hex_color(r.color)
            break
    return primary, secondary

def local_day_bounds(date_local: dt.date, tz: ZoneInfo):
    start_local = dt.datetime.combine(date_local, dt.time.min).replace(tzinfo=tz)
    end_local = start_local + dt.timedelta(days=1)
    return start_local.astimezone(dt.timezone.utc), end_local.astimezone(dt.timezone.utc)

async def init_db():
    if not DB_URL:
        raise RuntimeError("DATABASE_URL is not set")
    global _pool
    _pool = await asyncpg.create_pool(DB_URL)
    async with _pool.acquire() as conn:
        await conn.execute(CREATE_SQL)

# ------------------ Object Storage ------------------

def _s3_client():
    # Prefer Stackhero/MinIO
    if STACKHERO_ENDPOINT and STACKHERO_ACCESS_KEY and STACKHERO_SECRET_KEY and (STACKHERO_BUCKET or S3_BUCKET):
        return boto3.client(
            "s3",
            endpoint_url=STACKHERO_ENDPOINT,
            aws_access_key_id=STACKHERO_ACCESS_KEY,
            aws_secret_access_key=STACKHERO_SECRET_KEY,
            config=BotoConfig(s3={"addressing_style": "path"}),
        )
    # Fallback AWS S3
    if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY and S3_BUCKET:
        return boto3.client(
            "s3",
            region_name=AWS_REGION,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            config=BotoConfig(s3={"addressing_style": "virtual"}),
        )
    raise RuntimeError("Object storage not configured: set STACKHERO_* or AWS_* env vars")

def _safe_key_component(s: str) -> str:
    return "".join(c if c.isalnum() or c in ("-", "_", ".") else "_" for c in s)[:180]

def _build_s3_key(guild_id: int, channel_id: int, date_local: dt.date, message_id: int, attachment_id: int, filename: str) -> str:
    fname = _safe_key_component(filename)
    return f"{S3_PREFIX}{guild_id}/{channel_id}/{date_local.isoformat()}/{message_id}/{attachment_id}_{fname}"

def _public_url_for_key(key: str) -> str | None:
    if not S3_PUBLIC_URL_BASE:
        return None
    return f"{S3_PUBLIC_URL_BASE}/{key}"

def _bucket_name() -> str:
    return (STACKHERO_BUCKET or S3_BUCKET or "discord-archive").strip()

async def ensure_bucket_exists():
    try:
        s3 = _s3_client()
        b = _bucket_name()
        try:
            s3.create_bucket(Bucket=b)
        except Exception as e:
            m = str(e).lower()
            if "exist" not in m and "already" not in m:
                raise
    except Exception as e:
        print(f"[s3] ensure_bucket_exists failed: {e}")

# --------------- Attachment allowlist ---------------
_ALLOWED_EXTS = {".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp", ".tiff"}

def _is_allowed_attachment(filename: str, content_type: str | None) -> bool:
    if content_type:
        return content_type.startswith("image/")
    return os.path.splitext(filename)[1].lower() in _ALLOWED_EXTS

# -------------------- Archiving ---------------------
async def archive_range_for_channel(
    channel: discord.TextChannel,
    start_utc: dt.datetime,
    end_utc: dt.datetime,
    date_local: dt.date,
    delete_after: bool = False,
) -> int:
    if _pool is None:
        raise RuntimeError("DB not ready")
    count = 0

    perms = channel.permissions_for(channel.guild.me)
    can_delete = perms.manage_messages

    async for msg in channel.history(limit=None, after=start_utc, before=end_utc, oldest_first=True):
        try:
            if msg.pinned:
                continue
            if msg.content and "⭐" in msg.content:
                continue
            if msg.author.bot:
                continue

            member = msg.author if isinstance(msg.author, discord.Member) else channel.guild.get_member(msg.author.id)
            display_name = member.display_name if isinstance(member, discord.Member) else msg.author.display_name
            primary, secondary = (None, None)
            if isinstance(member, discord.Member):
                primary, secondary = await get_role_colors(member)

            local_dt = msg.created_at.astimezone(TZ)
            local_time_str = local_dt.strftime("%-I:%M %p") if os.name != "nt" else local_dt.strftime("%#I:%M %p")
            # Build a small static avatar URL (PNG); older param `static=True` is invalid.
            try:
                asset = msg.author.display_avatar
                if hasattr(asset, "is_animated") and asset.is_animated():
                    asset = asset.with_static_format("png")
                avatar_url = str(asset.replace(size=128))
            except Exception as e:
                print(f"[archive][{channel.id}] msg {msg.id} avatar error: {e}")
                avatar_url = None

            async with _pool.acquire() as conn:
                await conn.execute(
                    UPSERT_SQL,
                    msg.id,
                    channel.guild.id,
                    channel.id,
                    msg.author.id,
                    display_name,
                    msg.created_at,
                    date_local,
                    local_time_str,
                    primary,
                    secondary,
                    avatar_url,
                    msg.content or None,
                )

                # Embeds → JSONB
                if msg.embeds:
                    await conn.execute("DELETE FROM archived_embeds WHERE message_id = $1", msg.id)
                    for i, em in enumerate(msg.embeds):
                        await conn.execute(
                            "INSERT INTO archived_embeds (message_id, embed_idx, embed) VALUES ($1,$2,$3)",
                            msg.id, i, em.to_dict(),
                        )

                # Attachments → S3 + metadata (images only)
                if msg.attachments:
                    s3 = _s3_client() if MIRROR_ATTACHMENTS else None
                    for a in msg.attachments:
                        filename = a.filename
                        content_type = getattr(a, 'content_type', None)
                        if not _is_allowed_attachment(filename, content_type):
                            continue
                        size = a.size or 0
                        url = a.url
                        proxy_url = a.proxy_url

                        s3_key = None
                        s3_url = None
                        sha256_hex = None

                        if MIRROR_ATTACHMENTS and size <= MAX_ATTACHMENT_BYTES and s3 is not None:
                            try:
                                data = await a.read()
                                import hashlib
                                sha256_hex = hashlib.sha256(data).hexdigest()
                                # Dedupe: reuse existing upload by hash
                                rec = await conn.fetchrow(
                                    "SELECT s3_key, s3_url FROM archived_attachments WHERE sha256_hex = $1 AND s3_key IS NOT NULL LIMIT 1",
                                    sha256_hex,
                                )
                                if rec and rec["s3_key"]:
                                    s3_key = rec["s3_key"]
                                    s3_url = rec["s3_url"]
                                else:
                                    s3_key = _build_s3_key(channel.guild.id, channel.id, date_local, msg.id, int(a.id), filename)
                                    extra = {"ContentType": content_type} if content_type else None
                                    if extra:
                                        s3.put_object(Bucket=(STACKHERO_BUCKET or S3_BUCKET), Key=s3_key, Body=data, **extra)
                                    else:
                                        s3.put_object(Bucket=(STACKHERO_BUCKET or S3_BUCKET), Key=s3_key, Body=data)
                                    s3_url = _public_url_for_key(s3_key)
                            except Exception as e:
                                print(f"[attach] upload failed {a.id}: {e}")

                        await conn.execute(
                            """
                            INSERT INTO archived_attachments (
                                message_id, attachment_id, filename, content_type, size_bytes,
                                url, proxy_url, sha256_hex, s3_key, s3_url
                            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                            ON CONFLICT (message_id, attachment_id) DO UPDATE SET
                                filename = EXCLUDED.filename,
                                content_type = EXCLUDED.content_type,
                                size_bytes = EXCLUDED.size_bytes,
                                url = EXCLUDED.url,
                                proxy_url = EXCLUDED.proxy_url,
                                sha256_hex = COALESCE(EXCLUDED.sha256_hex, archived_attachments.sha256_hex),
                                s3_key = COALESCE(EXCLUDED.s3_key, archived_attachments.s3_key),
                                s3_url = COALESCE(EXCLUDED.s3_url, archived_attachments.s3_url)
                            """,
                            msg.id, int(a.id), filename, content_type, size, url, proxy_url, sha256_hex, s3_key, s3_url,
                        )

            count += 1

            if delete_after and can_delete:
                try:
                    await msg.delete()
                    await asyncio.sleep(0.2)
                except discord.Forbidden:
                    pass
                except discord.HTTPException:
                    pass
        except Exception as e:
            print(f"[archive][{channel.id}] msg {msg.id} error: {e}")
            continue
    return count

async def archive_yesterday_for_guild(guild: discord.Guild, delete_after: bool = False):
    today_local = dt.datetime.now(TZ).date()
    yday = today_local - dt.timedelta(days=1)
    s, e = local_day_bounds(yday, TZ)
    total = 0
    for ch in guild.text_channels:
        perms = ch.permissions_for(guild.me)
        if not (perms.view_channel and perms.read_message_history):
            continue
        try:
            total += await archive_range_for_channel(ch, s, e, yday, delete_after=delete_after)
        except discord.Forbidden:
            continue
    return total

# -------------------- Scheduler ---------------------
@tasks.loop(hours=24)
async def daily_archive():
    print("[daily] running...")
    total = 0
    for g in client.guilds:
        try:
            total += await archive_yesterday_for_guild(g, delete_after=False)
        except Exception as e:
            print(f"error {e}")
    print(f"[daily] archived {total} msgs")

# -------------------- Commands ----------------------
@tree.command(name="archive_manual", description="Archive yesterday (admins only). Optional delete.")
@discord.app_commands.describe(delete_after="If true, delete messages after archiving")
async def archive_manual(inter: discord.Interaction, delete_after: bool = False):
    if not inter.user.guild_permissions.administrator:
        await inter.response.send_message("Admins only.", ephemeral=True)
        return
    await inter.response.defer(ephemeral=True)
    count = await archive_yesterday_for_guild(inter.guild, delete_after=delete_after)
    await inter.followup.send(f"Archived {count} messages from yesterday. delete_after={delete_after}.", ephemeral=True)

@tree.command(name="archive_range", description="Backfill a local date range (admins only). Optional delete.")
@discord.app_commands.describe(start_date="YYYY-MM-DD (ARCHIVE_TZ)", end_date="YYYY-MM-DD inclusive (ARCHIVE_TZ)", delete_after="Delete after archiving")
async def archive_range_cmd(inter: discord.Interaction, start_date: str, end_date: str, delete_after: bool = False):
    if not inter.user.guild_permissions.administrator:
        await inter.response.send_message("Admins only.", ephemeral=True)
        return
    try:
        d0 = dt.date.fromisoformat(start_date)
        d1 = dt.date.fromisoformat(end_date)
        if d1 < d0:
            raise ValueError("end before start")
    except Exception:
        await inter.response.send_message("Invalid dates. Use YYYY-MM-DD.", ephemeral=True)
        return

    await inter.response.defer(ephemeral=True, thinking=True)
    total = 0
    cur = d0
    while cur <= d1:
        s_utc, e_utc = local_day_bounds(cur, TZ)
        for g in [inter.guild]:
            for ch in g.text_channels:
                perms = ch.permissions_for(g.me)
                if not (perms.view_channel and perms.read_message_history):
                    continue
                try:
                    total += await archive_range_for_channel(ch, s_utc, e_utc, cur, delete_after=delete_after)
                except discord.Forbidden:
                    continue
        cur += dt.timedelta(days=1)
    await inter.followup.send(f"Backfill complete: archived {total} messages from {d0} to {d1}. delete_after={delete_after}.", ephemeral=True)

# --------------------- Events -----------------------
@client.event
async def on_ready():
    log.info("gateway: on_ready as %s (id=%s)", client.user, getattr(client.user, 'id', '?'))
    await ensure_bucket_exists()
    await tree.sync()
    daily_archive.start()

@client.event
async def on_connect():
    log.info("gateway: on_connect")

@client.event
async def on_resumed():
    log.info("gateway: on_resumed")

# ---------------------- Main ------------------------
async def main():
    # Sanity log (no secrets)
    log.info("boot: starting main()")
    if not TOKEN:
        raise RuntimeError("DISCORD_TOKEN is not set")
    if not DB_URL:
        raise RuntimeError("DATABASE_URL is not set")
    log.info("boot: env ok | tz=%s | mirror=%s | stackhero_endpoint=%s | bucket=%s",
             ARCHIVE_TZ, MIRROR_ATTACHMENTS, bool(STACKHERO_ENDPOINT), (STACKHERO_BUCKET or S3_BUCKET))
    await init_db()
    log.info("boot: db ready, connecting to Discord gateway…")
    try:
        await client.start(TOKEN)
    except Exception as e:
        log.exception("client.start failed: %s", e)
        raise

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        log.exception("fatal: %s", e)
        raise

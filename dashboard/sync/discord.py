"""Discord HTTP boundary. Mentions are inert; rate limits are honored."""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
from urllib.parse import quote

import aiohttp

API = "https://discord.com/api/v10"
FORUM_CHANNEL = 15
DISCORD_EPOCH_MS = 1420070400000
MESSAGE_LIMIT = 1900
TITLE_LIMIT = 100
MAX_TAGS = 5
NO_MENTIONS = {"parse": []}


def chunks(text, limit=MESSAGE_LIMIT):
    """Split text into message-sized parts. Discord counts UTF-16 code units, not code points."""
    parts = []
    current = []
    units = 0
    for char in text or "\u200b":
        width = len(char.encode("utf-16-le")) // 2
        if units + width > limit:
            parts.append("".join(current))
            current = []
            units = 0
        current.append(char)
        units += width
    parts.append("".join(current))
    return parts


def created_at(snowflake):
    """Unix time in seconds that a Discord id was created."""
    return ((int(snowflake) >> 22) + DISCORD_EPOCH_MS) / 1000


def _nonce(key):
    # Discord drops a second message with the same nonce, so retries never post twice.
    return str(int.from_bytes(hashlib.sha256(key.encode()).digest()[:8]))


class DiscordError(RuntimeError):
    def __init__(self, status):
        self.status = status
        super().__init__(f"Discord returned HTTP {status}")


class Discord:
    def __init__(self, http, token, base=API):
        self.http = http
        self.token = token
        self.base = base.rstrip("/")
        self.guild = None

    async def request(self, method, path, *, body=None, form=None, missing_ok=False):
        for _ in range(6):
            options = {
                "headers": {"Authorization": f"Bot {self.token}", "User-Agent": "HermesDiscordSync/0.1"},
                "allow_redirects": False,
                "timeout": aiohttp.ClientTimeout(total=60),
            }
            if form is not None:
                options["data"] = form()
            elif body is not None:
                options["json"] = body
            async with self.http.request(method, self.base + path, **options) as response:
                if response.status == 429:
                    wait = await response.json()
                    await asyncio.sleep(min(float(wait.get("retry_after", 1)), 120))
                    continue
                if response.status == 404 and missing_ok:
                    return None
                if response.status >= 300:
                    raise DiscordError(response.status)
                if response.status == 204:
                    return {}
                return await response.json()
        raise DiscordError(429)

    async def validate(self, forum):
        channel = await self.request("GET", f"/channels/{forum}")
        if channel.get("type") != FORUM_CHANNEL:
            raise ValueError("Sync target must be a Discord forum channel")
        self.guild = channel["guild_id"]
        return channel

    async def create_post(self, forum, title, session):
        return await self.request(
            "POST",
            f"/channels/{forum}/threads",
            body={
                "name": (title or session)[:TITLE_LIMIT],
                "auto_archive_duration": 10080,
                "message": {"content": f"hermes session: `{session}`", "allowed_mentions": NO_MENTIONS},
            },
        )

    async def typing(self, channel):
        # Cosmetic only: a failed typing indicator must never fail the turn.
        with contextlib.suppress(DiscordError, aiohttp.ClientError):
            await self.request("POST", f"/channels/{channel}/typing", missing_ok=True)

    async def reply(self, channel, mid, text, *, key=None):
        return await self.request(
            "POST",
            f"/channels/{channel}/messages",
            body={
                "content": text,
                "allowed_mentions": NO_MENTIONS,
                "nonce": _nonce(key or f"reply:{mid}"),
                "enforce_nonce": True,
                "message_reference": {"message_id": str(mid), "fail_if_not_exists": False},
            },
        )

    async def react(self, channel, mid, emoji):
        # Cosmetic, like typing: a missing message or a refused reaction never fails a turn.
        path = f"/channels/{channel}/messages/{mid}/reactions/{quote(emoji)}/@me"
        with contextlib.suppress(DiscordError, aiohttp.ClientError):
            await self.request("PUT", path, missing_ok=True)

    async def unreact(self, channel, mid, emoji):
        path = f"/channels/{channel}/messages/{mid}/reactions/{quote(emoji)}/@me"
        with contextlib.suppress(DiscordError, aiohttp.ClientError):
            await self.request("DELETE", path, missing_ok=True)

    async def ensure_tag(self, forum, name):
        channel = await self.request("GET", f"/channels/{forum}")
        tags = channel.get("available_tags", [])
        found = next((tag for tag in tags if tag["name"] == name), None)
        if found:
            return found["id"]
        # Moderated: only the bot and moderators can apply it.
        updated = await self.request(
            "PATCH", f"/channels/{forum}", body={"available_tags": [*tags, {"name": name, "moderated": True}]}
        )
        return next(tag["id"] for tag in updated["available_tags"] if tag["name"] == name)

    async def set_tag(self, thread, tag, on):
        channel = await self.request("GET", f"/channels/{thread}")
        tags = [applied for applied in channel.get("applied_tags", []) if applied != tag]
        if on:
            tags.append(tag)
        await self.request("PATCH", f"/channels/{thread}", body={"applied_tags": tags[:MAX_TAGS]})

    async def active_threads(self):
        found = await self.request("GET", f"/guilds/{self.guild}/threads/active")
        return found.get("threads", [])

    async def message(self, channel, mid):
        return await self.request("GET", f"/channels/{channel}/messages/{mid}", missing_ok=True)

    async def messages_after(self, thread, cursor):
        """Every message newer than the cursor, oldest first; pages back past 100 messages."""
        collected = []
        before = ""
        while True:
            suffix = f"&before={before}" if before else ""
            batch = await self.request("GET", f"/channels/{thread}/messages?limit=100{suffix}")
            newer = [message for message in batch if int(message["id"]) > int(cursor)]
            collected.extend(newer)
            reached_cursor = len(newer) != len(batch)
            if len(batch) < 100 or reached_cursor:
                break
            before = min(batch, key=lambda message: int(message["id"]))["id"]
        return sorted(collected, key=lambda message: int(message["id"]))

    async def send(self, thread, text, *, key, components=None):
        body = {"content": text, "allowed_mentions": NO_MENTIONS, "nonce": _nonce(key), "enforce_nonce": True}
        if components is not None:
            body["components"] = components
        return await self.request("POST", f"/channels/{thread}/messages", body=body)

    async def edit(self, thread, mid, text, *, components=None):
        body = {"content": text, "allowed_mentions": NO_MENTIONS}
        if components is not None:
            body["components"] = components
        return await self.request("PATCH", f"/channels/{thread}/messages/{mid}", body=body, missing_ok=True)

    async def delete(self, thread, mid):
        await self.request("DELETE", f"/channels/{thread}/messages/{mid}", missing_ok=True)

    async def rename(self, thread, title):
        await self.request("PATCH", f"/channels/{thread}", body={"name": title[:TITLE_LIMIT]})

    async def unarchive(self, thread):
        channel = await self.request("GET", f"/channels/{thread}")
        if channel.get("thread_metadata", {}).get("archived"):
            await self.request("PATCH", f"/channels/{thread}", body={"archived": False})

    async def upload(self, thread, filename, content, *, key):
        def form():
            data = aiohttp.FormData()
            payload = {
                "allowed_mentions": NO_MENTIONS,
                "nonce": _nonce(key),
                "enforce_nonce": True,
                "attachments": [{"id": 0, "filename": filename}],
            }
            data.add_field("payload_json", json.dumps(payload))
            data.add_field("files[0]", content, filename=filename, content_type="application/octet-stream")
            return data

        return await self.request("POST", f"/channels/{thread}/messages", form=form)

    async def download(self, url, limit):
        """Fetch an attachment, refusing redirects and anything over `limit` bytes."""
        async with self.http.get(url, allow_redirects=False) as response:
            if response.status != 200:
                raise ValueError("Could not download Discord attachment")
            data = bytearray()
            async for chunk in response.content.iter_chunked(64 * 1024):
                data.extend(chunk)
                if len(data) > limit:
                    raise ValueError("Attachment is over 10 MiB")
        return bytes(data)

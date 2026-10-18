"""Discord HTTP boundary. Mentions are inert; SDK-style pagination and rate limits."""
from __future__ import annotations

import asyncio
import hashlib
import json

import aiohttp


def chunks(text, limit=1900):
    # Discord counts UTF-16 code units, not Python unicode code points.
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


class DiscordError(RuntimeError):
    def __init__(self, status):
        self.status = status
        super().__init__(f"Discord returned HTTP {status}")


class Discord:
    def __init__(self, http, token, base="https://discord.com/api/v10"):
        self.http = http
        self.token = token
        self.base = base.rstrip("/")

    async def request(self, method, path, *, body=None, form=None, missing_ok=False):
        for _ in range(6):
            kwargs = {"headers": {"Authorization": f"Bot {self.token}", "User-Agent": "HermesDiscordSync/0.1"},
                      "allow_redirects": False, "timeout": aiohttp.ClientTimeout(total=60)}
            if form is not None:
                kwargs["data"] = form()
            elif body is not None:
                kwargs["json"] = body
            async with self.http.request(method, self.base + path, **kwargs) as response:
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
        if channel.get("type") != 15:
            raise ValueError("Sync target must be a Discord forum channel")
        self.guild = channel["guild_id"]
        return channel

    async def create_thread(self, forum, title, session):
        return await self.request("POST", f"/channels/{forum}/threads", body={
            "name": (title or session)[:100], "auto_archive_duration": 10080,
            "message": {"content": f"hermes session: `{session}`", "allowed_mentions": {"parse": []}},
        })

    async def typing(self, channel):
        await self.request("POST", f"/channels/{channel}/typing", missing_ok=True)

    async def reply(self, channel, mid, text):
        return await self.request("POST", f"/channels/{channel}/messages", body={
            "content": text, "allowed_mentions": {"parse": []}, "nonce": f"m{mid}"[-25:],
            "message_reference": {"message_id": str(mid), "fail_if_not_exists": False},
        })

    async def ensure_tag(self, forum, name):
        channel = await self.request("GET", f"/channels/{forum}")
        tags = channel.get("available_tags", [])
        found = next((tag for tag in tags if tag["name"] == name), None)
        if found:
            return found["id"]
        updated = await self.request("PATCH", f"/channels/{forum}", body={
            "available_tags": [*tags, {"name": name, "moderated": True}]})
        return next(tag["id"] for tag in updated["available_tags"] if tag["name"] == name)

    async def set_tag(self, thread, tag, on):
        channel = await self.request("GET", f"/channels/{thread}")
        tags = [t for t in channel.get("applied_tags", []) if t != tag]
        if on:
            tags.append(tag)
        await self.request("PATCH", f"/channels/{thread}", body={"applied_tags": tags[:5]})

    async def active_threads(self):
        found = await self.request("GET", f"/guilds/{self.guild}/threads/active")
        return found.get("threads", [])

    async def message(self, channel, mid):
        return await self.request("GET", f"/channels/{channel}/messages/{mid}", missing_ok=True)

    async def messages_after(self, thread, cursor):
        # Paginate backwards until the durable cursor; no 100-message reconnect gap.
        collected = []
        before = ""
        while True:
            suffix = f"&before={before}" if before else ""
            batch = await self.request("GET", f"/channels/{thread}/messages?limit=100{suffix}")
            newer = [message for message in batch if int(message["id"]) > int(cursor)]
            collected.extend(newer)
            if len(batch) < 100 or len(newer) != len(batch):
                break
            before = min(batch, key=lambda message: int(message["id"]))["id"]
        return sorted(collected, key=lambda message: int(message["id"]))

    async def send(self, thread, text, *, key):
        nonce = str(int.from_bytes(hashlib.sha256(key.encode()).digest()[:8]))
        return await self.request("POST", f"/channels/{thread}/messages", body={
            "content": text, "allowed_mentions": {"parse": []}, "nonce": nonce, "enforce_nonce": True,
        })

    async def edit(self, thread, mid, text):
        return await self.request("PATCH", f"/channels/{thread}/messages/{mid}", body={
            "content": text, "allowed_mentions": {"parse": []},
        }, missing_ok=True)

    async def delete(self, thread, mid):
        await self.request("DELETE", f"/channels/{thread}/messages/{mid}", missing_ok=True)

    async def title(self, thread, title):
        await self.request("PATCH", f"/channels/{thread}", body={"name": title[:100]})

    async def unarchive(self, thread):
        channel = await self.request("GET", f"/channels/{thread}")
        if channel.get("thread_metadata", {}).get("archived"):
            await self.request("PATCH", f"/channels/{thread}", body={"archived": False})

    async def upload(self, thread, filename, content, *, key):
        nonce = str(int.from_bytes(hashlib.sha256(key.encode()).digest()[:8]))

        def form():
            data = aiohttp.FormData()
            data.add_field("payload_json", json.dumps({
                "allowed_mentions": {"parse": []}, "nonce": nonce, "enforce_nonce": True,
                "attachments": [{"id": 0, "filename": filename}],
            }))
            data.add_field("files[0]", content, filename=filename, content_type="application/octet-stream")
            return data

        return await self.request("POST", f"/channels/{thread}/messages", form=form)

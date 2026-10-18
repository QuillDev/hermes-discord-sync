"""One conversation, one execution owner; Discord is another live client."""
from __future__ import annotations

import asyncio
import base64
import contextlib
import hashlib
import logging
import re
import time
from pathlib import Path
from urllib.parse import urlparse

from .discord import chunks
from .rpc import RpcError

log = logging.getLogger("hermes.discord-sync")
MEDIA = re.compile(r"(?m)^MEDIA:(/[^\r\n]+)$")
MAX_FILE = 10 * 1024 * 1024


class Bridge:
    def __init__(self, rpc, discord, store, forum, allowed_users, *, notify=None, guard=None,
                 auto_link=True, heartbeat=None, pinned=None, set_pin=None, tag_events=None,
                 inbound=None):
        self.rpc = rpc
        self.discord = discord
        self.store = store
        self.forum = str(forum)
        self.allowed_users = {str(user) for user in allowed_users}
        if not self.allowed_users or any(not user.isdecimal() for user in self.allowed_users):
            raise ValueError("Set an explicit numeric Discord user allowlist")
        self.notify = notify or (lambda session: None)
        self.guard = guard or (lambda: True)
        self.auto_link = auto_link
        self.heartbeat = heartbeat
        self.pinned = pinned
        self.set_pin = set_pin
        self.tag_events = tag_events
        # Discord messages pushed by the gateway's listener: no per-thread polling lag.
        self.inbound = inbound
        self.working = {}
        self.poll_index = 0
        self.last_stream = 0
        self.last_typing = 0
        self.sessions_changed = False
        self.pin_tag = None
        self.last_pins = 0
        self.last_full = {}
        # Only sessions started after sync was first enabled are auto-linked,
        # so enabling it never floods the forum with the whole history.
        self.auto_since = float(store.meta("auto_link_since", time.time()))
        self.adopt_since = time.time()
        self.ignored_threads = set()
        self.last_scan = 0
        self.runtime = {}
        self.streams = {}
        self.requests = {}
        self.last_seq = {}
        self.epoch = None
        self.lock = asyncio.Lock()
        self.connected = False
        self.last_error = None
        self.task = None

    async def attach(self, link):
        snapshot = await self.rpc.call("session.resume", {
            "session_id": link["session"], "lazy": True,
            "defer_history": True, "inline_images": False, "close_on_disconnect": False,
        })
        sid = snapshot["session_id"]
        self.runtime[link["session"]] = sid
        inflight = snapshot.get("inflight") or {}
        if inflight.get("streaming") and inflight.get("assistant"):
            self.streams[sid] = inflight["assistant"]
        replay = await self.rpc.call("session.events.since", {"session_id": sid, "last_seen": 0})
        # Reconcile durable rows, then start at the snapshot's boundary. A new
        # transport can have already queued some of the same replay frames.
        self.last_seq[sid] = replay.get("latest_seq", 0)
        for request in replay.get("open_requests", []):
            await self.request(request)
        await self.reconcile(link)

    async def link(self, session):
        async with self.lock:
            return await self._link(session)

    def _find(self, session):
        return next((link for link in self.store.links() if link["session"] == session), None)

    async def _link(self, session, row=None):
        existing = self._find(session)
        if existing:
            return existing
        if self.store.known(session=session):
            raise ValueError("This session was unlinked; it stays off Discord")
        if row is None:
            # Resolve before publishing anything. Only desktop conversations in
            # this profile may be exposed; no title-based or foreign fallback.
            listing = await self.rpc.call("session.list", {"limit": 10000})
            row = next((row for row in listing["sessions"] if row["id"] == session), None)
        if row is None or row.get("source") not in {"desktop", "tui"}:
            raise ValueError("Select a desktop session in the default profile")
        topic = await self.discord.create_thread(self.forum, row.get("title") or session, session)
        thread = topic["id"]
        starter = (topic.get("message") or {}).get("id", thread)
        self.store.link(session, thread, row.get("title") or session, starter)
        link = self._find(session)
        await self.attach(link)
        return link

    async def adopt(self, thread, starter=None):
        """A new forum post becomes a real desktop session, not a gateway chat."""
        tid = str(thread["id"])
        if tid in self.ignored_threads or self.store.known(thread=tid):
            return
        created_at = ((int(tid) >> 22) + 1420070400000) / 1000
        if starter is None:
            starter = await self.discord.message(tid, tid)
        author = (starter or {}).get("author") or {}
        # Posts from before this connection were answered by the normal gateway.
        if (created_at < self.adopt_since or starter is None or author.get("bot")
                or str(author.get("id")) not in self.allowed_users):
            self.ignored_threads.add(tid)
            return
        title = thread.get("name") or "discord session"
        created = await self.rpc.call("session.create", {
            "source": "desktop", "title": title, "close_on_disconnect": False,
            "idempotency_key": f"discord-sync:{tid}",
        })
        session = created["stored_session_id"]
        # Cursor sits just before the starter, so the post itself is the first turn.
        self.store.link(session, tid, title, str(int(tid) - 1))
        self.runtime[session] = created["session_id"]
        self.last_seq[created["session_id"]] = 0
        # Submit the post itself right away instead of waiting for a poll pass.
        link = self._find(session)
        await self.receive(link, starter)
        self.store.update(session, cursor=tid)
        self.notify(session)

    async def mention(self, event):
        """An @mention from the user starts a synced forum post instead of a gateway thread."""
        message = event["message"]
        author = message.get("author") or {}
        if author.get("bot") or str(author.get("id")) not in self.allowed_users:
            return
        mid = str(message["id"])
        if self.store.inbox_status(event["channel"], mid):
            return
        self.store.received(event["channel"], mid, "sending")
        text = re.sub(r"<@!?\d+>", "", message.get("content", "")).strip()
        title = (text.splitlines() or ["discord session"])[0][:90] or "discord session"
        created = await self.rpc.call("session.create", {
            "source": "desktop", "title": title, "close_on_disconnect": False,
            "idempotency_key": f"discord-sync:mention:{mid}",
        })
        session = created["stored_session_id"]
        topic = await self.discord.create_thread(self.forum, title, session)
        thread = topic["id"]
        starter = (topic.get("message") or {}).get("id", thread)
        self.store.link(session, thread, title, starter)
        self.runtime[session] = created["session_id"]
        self.last_seq[created["session_id"]] = 0
        await self.discord.reply(event["channel"], mid, f"on it in <#{thread}>")
        link = self._find(session)
        await self.discord.typing(thread)
        refs = await self.inbound_attachments(message, created["session_id"])
        result = await self.rpc.call("prompt.submit", {
            "session_id": created["session_id"], "text": "\n".join([text, *refs]).strip(), "surface": "desktop",
        })
        self.store.received(event["channel"], mid, "accepted", result.get("user_row_id"))
        self.working[created["session_id"]] = link
        await self.reconcile(link)

    async def drain(self):
        """Handle messages the gateway pushed; returns quickly when there are none."""
        for event in (self.inbound() if self.inbound else []):
            try:
                if event.get("kind") == "mention":
                    await self.mention(event)
                    continue
                message = event["message"]
                link = next((link for link in self.store.links() if link["thread"] == event["thread"]), None)
                if link is None and str(message["id"]) == event["thread"]:
                    await self.adopt({"id": event["thread"], "name": event.get("name")}, starter=message)
                    link = next((link for link in self.store.links() if link["thread"] == event["thread"]), None)
                if link is None:
                    continue
                await self.receive(link, message)
                if int(message["id"]) > int(link["cursor"]):
                    self.store.update(link["session"], cursor=str(message["id"]))
            except (RpcError, ValueError) as error:
                log.warning("Discord sync could not handle a pushed message: %s", error)

    async def scan(self):
        if time.monotonic() - self.last_scan < (5 if self.sessions_changed else 30):
            return
        self.sessions_changed = False
        self.last_scan = time.monotonic()
        for thread in await self.discord.active_threads():
            if str(thread.get("parent_id")) == self.forum:
                await self.adopt(thread)
        if not self.auto_link:
            return
        listing = await self.rpc.call("session.list", {"limit": 500})
        for row in listing["sessions"]:
            if (row.get("source") in {"desktop", "tui"} and float(row.get("started_at") or 0) >= self.auto_since
                    and not self.store.known(session=row["id"])):
                # One per scan: a backfill must not hold the loop (and the gateway
                # reservation heartbeat) while it copies a long history.
                await self._link(row["id"], row)
                return

    async def upsert(self, link, address, body):
        projection = self.store.get_projection(link["session"]).get(address)
        if projection and projection["body"] == body:
            return
        if projection and not projection["owned"]:
            # A bot cannot edit a human's message. Corrections are a separate
            # visible projection, never a rewrite of the original phone input.
            projection = None
        ids = projection["ids"][:] if projection else []
        parts = chunks(body)
        result_ids = []
        for index, part in enumerate(parts):
            if self.heartbeat is not None:
                self.heartbeat()  # long backfills keep the gateway reservation alive
            sent = None
            if index < len(ids):
                sent = await self.discord.edit(link["thread"], ids[index], part)
            if sent is None:
                sent = await self.discord.send(link["thread"], part,
                                               key=f'{link["session"]}:{address}:{index}')
            result_ids.append(sent["id"])
        for stale in ids[len(parts):]:
            await self.discord.delete(link["thread"], stale)
        self.store.project(link["session"], address, result_ids, body)

    async def remove(self, link, address):
        projection = self.store.get_projection(link["session"]).get(address)
        if projection and projection["owned"]:
            for mid in projection["ids"]:
                await self.discord.delete(link["thread"], mid)
        self.store.forget(link["session"], address)

    async def reconcile(self, link):
        sid = self.runtime[link["session"]]
        history = await self.rpc.call("session.history", {"session_id": sid})
        active = set()
        for message in history["messages"]:
            role = message.get("role")
            rid = message.get("row_id")
            text = message.get("text") or ""
            if rid is None or message.get("display_kind") == "hidden" or role not in {"user", "assistant"}:
                continue
            if not text.strip():
                continue
            address = f"row:{rid}"
            active.add(address)
            body = f"you: {text}" if role == "user" else text
            await self.upsert(link, address, body)
            if role == "assistant":
                await self.artifacts(link, address, text)
        for address in self.store.get_projection(link["session"]):
            if address.startswith("row:") and address not in active:
                await self.remove(link, address)
        self.notify(link["session"])

    async def artifacts(self, link, address, text):
        for raw in MEDIA.findall(text):
            path = Path(raw)
            key = f"file:{address}:{hashlib.sha256(raw.encode()).hexdigest()[:16]}"
            if key in self.store.get_projection(link["session"]):
                continue
            # Only explicit assistant deliverables, never tool args, reasoning,
            # system prompts, or arbitrary paths mentioned in normal prose.
            if not path.is_file() or path.stat().st_size > MAX_FILE:
                continue
            data = await asyncio.to_thread(path.read_bytes)
            sent = await self.discord.upload(link["thread"], path.name, data,
                                              key=f'{link["session"]}:{key}')
            self.store.project(link["session"], key, [sent["id"]], raw)

    async def inbound_attachments(self, message, sid):
        refs = []
        for attachment in message.get("attachments", []):
            url = attachment["url"]
            parsed = urlparse(url)
            if (parsed.scheme != "https" or parsed.hostname not in {"cdn.discordapp.com", "media.discordapp.net"}
                    or attachment.get("size", MAX_FILE + 1) > MAX_FILE):
                raise ValueError("Attachment must be a Discord-hosted file under 10 MiB")
            async with self.discord.http.get(url, allow_redirects=False) as response:
                if response.status != 200:
                    raise ValueError("Could not download Discord attachment")
                data = bytearray()
                async for chunk in response.content.iter_chunked(64 * 1024):
                    data.extend(chunk)
                    if len(data) > MAX_FILE:
                        raise ValueError("Attachment is over 10 MiB")
            mime = attachment.get("content_type") or "application/octet-stream"
            encoded = base64.b64encode(data).decode()
            if mime.startswith("image/"):
                await self.rpc.call("image.attach_bytes", {
                    "session_id": sid, "filename": attachment["filename"],
                    "content_base64": encoded,
                })
            else:
                result = await self.rpc.call("file.attach", {
                    "session_id": sid, "name": attachment["filename"],
                    "data_url": f"data:{mime};base64,{encoded}",
                })
                refs.append(result["ref_text"])
        return refs

    async def receive(self, link, message):
        if not self.guard():
            raise ConnectionError("Gateway reservation unavailable")
        author = message.get("author") or {}
        if author.get("bot") or message.get("webhook_id") or str(author.get("id")) not in self.allowed_users:
            return
        mid = str(message["id"])
        previous = self.store.inbox_status(link["thread"], mid)
        if previous:
            if previous["status"] == "sending":
                await self.upsert(link, f"uncertain:{mid}",
                                  "delivery could not be confirmed after a disconnect. check desktop before resending.")
                self.store.received(link["thread"], mid, "uncertain")
            return
        text = message.get("content", "")
        sid = self.runtime[link["session"]]
        self.store.received(link["thread"], mid, "sending")
        try:
            if text.startswith("!sync ") or text in {"!stop", "!unlink"} or text.startswith(("!approve ", "!answer ")):
                await self.command(link, text)
                self.store.received(link["thread"], mid, "accepted")
                return
            refs = await self.inbound_attachments(message, sid)
            prompt = "\n".join([text, *refs]).strip()
            if not prompt:
                self.store.received(link["thread"], mid, "ignored")
                return
            # The normal shared-session submit takes the same turn lock as
            # Desktop. Never write messages or acquire a second session lease.
            result = await self.rpc.call("prompt.submit", {
                "session_id": sid, "text": prompt, "surface": "desktop",
            })
            row_id = result.get("user_row_id")
            self.store.received(link["thread"], mid, "accepted", row_id)
            self.working[sid] = link
            await self.discord.typing(link["thread"])
            if row_id is not None:
                self.store.project(link["session"], f"row:{row_id}", [mid], f"you: {prompt}", owned=False)
            self.notify(link["session"])
        except RpcError as error:
            self.store.received(link["thread"], mid, "rejected")
            await self.upsert(link, f"rejected:{mid}", f"message was not accepted: {error}")
        except ValueError as error:
            self.store.received(link["thread"], mid, "rejected")
            await self.upsert(link, f"rejected:{mid}", str(error))
        # Transport failures deliberately keep the durable `sending` intent.
        # There is no prompt idempotency key in the host's current RPC contract:
        # an unacknowledged send MUST NOT be blindly replayed after reconnect.

    async def command(self, link, text):
        sid = self.runtime[link["session"]]
        if text == "!stop":
            await self.rpc.call("session.interrupt", {"session_id": sid})
            return
        if text == "!unlink":
            self.store.update(link["session"], enabled=0)
            await self.upsert(link, "unlinked", "sync stopped. the desktop conversation is unchanged.")
            return
        if text.startswith("!approve "):
            _, rid, choice = text.split(maxsplit=2)
            request = self.requests.get(rid)
            if not request or request["method"] != "approval" or request["params"]["session_id"] != sid:
                raise ValueError("That approval is no longer open in this session")
            if choice not in request["params"].get("choices", []):
                raise ValueError("Choose one of the offered approval options")
            await self.rpc.call("request.answer", {"id": rid, "result": {"choice": choice}})
            return
        if text.startswith("!answer "):
            _, rid, number, answer = text.split(maxsplit=3)
            request = self.requests.get(rid)
            if not request or request["method"] != "clarify" or request["params"]["session_id"] != sid:
                raise ValueError("That question is no longer open in this session")
            questions = request["params"]["questions"]
            index = int(number) - 1
            if index < 0 or index >= len(questions):
                raise ValueError("Question number is out of range")
            question = questions[index]
            choices = question.get("choices") or []
            if answer.isdecimal() and 0 < int(answer) <= len(choices):
                answer = choices[int(answer) - 1]
            result = await self.rpc.call("clarify.lock", {
                "request_id": rid, "question_id": question["qid"], "answer": answer,
            })
            request.setdefault("answers", {})[question["qid"]] = answer
            if result.get("remaining") == 0:
                await self.rpc.call("request.answer", {"id": rid, "result": {"answers": request["answers"]}})
            return
        raise ValueError("commands: !stop, !unlink, !approve <request> <choice>, !answer <request> <question> <answer>")

    async def request(self, frame):
        if frame.get("method") == "event":
            return
        rid = str(frame.get("id", ""))
        params = frame.get("params") or {}
        link = self.by_runtime(params.get("session_id"))
        if link is None or not rid:
            return
        self.requests[rid] = frame
        if frame["method"] == "approval":
            body = (params.get("description") or "approval needed") + "\n" + params.get("command", "")
            for choice in params.get("choices", []):
                body += f"\n`!approve {rid} {choice}`"
        elif frame["method"] == "clarify":
            body = ""
            for index, question in enumerate(params.get("questions", []), 1):
                body += f"{index}. {question['question']}\n"
                for number, choice in enumerate(question.get("choices") or [], 1):
                    body += f"   {number}: {choice}\n"
                body += f"`!answer {rid} {index} your answer`\n"
        else:
            body = f"{frame['method']} needs the desktop app. secrets and desktop-only actions are not sent through discord."
        await self.upsert(link, f"request:{rid}", body)

    def by_runtime(self, sid):
        return next((link for link in self.store.links() if self.runtime.get(link["session"]) == sid), None)

    async def event(self, frame):
        if frame.get("method") == "bridge.disconnected":
            raise ConnectionError("Desktop connection closed")
        if frame.get("method") != "event":
            await self.request(frame)
            return
        event = frame.get("params") or {}
        if event.get("type") == "sessions.changed":
            self.sessions_changed = True
            return
        sid = event.get("session_id")
        seq = event.get("seq", 0)
        if seq and seq <= self.last_seq.get(sid, 0):
            return
        if seq:
            self.last_seq[sid] = seq
        link = self.by_runtime(sid)
        if link is None:
            return
        kind = event.get("type")
        payload = event.get("payload") or {}
        if kind == "message.delta":
            self.streams[sid] = self.streams.get(sid, "") + payload.get("text", "")
        elif kind == "message.start":
            self.streams[sid] = ""
            self.working[sid] = link
        elif kind in {"message.complete", "message.interim"}:
            await self.reconcile(link)
            if kind == "message.complete":
                self.streams.pop(sid, None)
                self.working.pop(sid, None)
                await self.remove(link, "stream")
                await self.remove(link, "activity")
                if payload.get("status") in {"error", "interrupted"}:
                    await self.upsert(link, "outcome", payload.get("error") or payload["status"])
            else:
                self.streams[sid] = ""
                await self.remove(link, "stream")
        elif kind == "tool.start":
            await self.upsert(link, "activity", f"working: {payload.get('name', 'tool')}")
        elif kind == "tool.complete":
            await self.remove(link, "activity")
        elif kind == "session.title":
            title = payload.get("title")
            if title and title != link["title"]:
                await self.discord.title(link["thread"], title)
                self.store.update(link["session"], title=title)
        elif kind == "request.cancel":
            rid = str(payload.get("id") or payload.get("request_id") or "")
            self.requests.pop(rid, None)
            await self.remove(link, f"request:{rid}")
        elif kind == "error":
            await self.upsert(link, "outcome", payload.get("message") or "session error")

    async def sync_pins(self):
        """Mirror pins both ways. Discord allows one pinned post, so a tag stands in."""
        if self.pinned is None:
            return
        if self.pin_tag is None:
            self.pin_tag = await self.discord.ensure_tag(self.forum, "pinned")
        # Discord -> desktop: tag edits arrive from the gateway's listener.
        for event in (self.tag_events() if self.tag_events else []):
            link = next((link for link in self.store.links() if link["thread"] == event["thread"]), None)
            if link is None or self.set_pin is None:
                continue
            want = "1" if self.pin_tag in event["tags"] else "0"
            current = self.store.get_projection(link["session"]).get("pin")
            if current and current["body"] == want:
                continue
            self.set_pin(link["session"], want == "1")
            self.store.project(link["session"], "pin", [], want)
        # Desktop -> Discord: one indexed query, cheap enough to run every second.
        pinned = self.pinned()
        for link in self.store.links():
            want = "1" if link["session"] in pinned else "0"
            current = self.store.get_projection(link["session"]).get("pin")
            if current and current["body"] == want:
                continue
            await self.discord.set_tag(link["thread"], self.pin_tag, want == "1")
            self.store.project(link["session"], "pin", [], want)

    async def tick(self):
        async with self.lock:
            await self.drain()
            await self.sync_pins()
            await self.scan()
            now = time.monotonic()
            if now - self.last_stream >= 1:
                self.last_stream = now
                for session, stream in list(self.streams.items()):
                    link = self.by_runtime(session)
                    if link and stream:
                        await self.upsert(link, "stream", stream)
            if now - self.last_typing >= 8:
                self.last_typing = now
                for link in list(self.working.values()):
                    await self.discord.typing(link["thread"])
            # Safety net for anything the push path missed (backend restarts,
            # edits): one thread per tick, so the full cycle stays cheap.
            links = self.store.links()
            if not links or now - self.last_full.get("_poll", 0) < 1:
                return
            self.last_full["_poll"] = now
            link = links[self.poll_index % len(links)]
            self.poll_index += 1
            await self.discord.unarchive(link["thread"])
            for message in await self.discord.messages_after(link["thread"], link["cursor"]):
                await self.receive(link, message)
                self.store.update(link["session"], cursor=str(message["id"]))
                if not self._find(link["session"]):
                    return
            await self.reconcile(link)

    async def beat(self):
        # Separate from the tick so a slow Discord pass never lets the gateway
        # reservation lapse and the normal bot answer a synced post twice.
        while True:
            if self.heartbeat is not None:
                self.heartbeat()
            await asyncio.sleep(2)

    async def serve(self):
        delay = 1
        while True:
            if not self.guard():
                await asyncio.sleep(1)
                continue
            try:
                await self.discord.validate(self.forum)
                await self.rpc.connect()
                self.runtime.clear()
                self.requests.clear()
                self.streams.clear()
                self.last_seq.clear()
                async with self.lock:
                    for link in self.store.links():
                        await self.attach(link)
                self.connected = True
                self.last_error = None
                beating = asyncio.create_task(self.beat())
                delay = 1
                deadline = time.monotonic()
                while True:
                    if not self.guard():
                        raise ConnectionError("Gateway reservation unavailable")
                    timeout = max(0, deadline - time.monotonic())
                    try:
                        frame = await asyncio.wait_for(self.rpc.events.get(), timeout)
                    except TimeoutError:
                        await self.tick()
                        deadline = time.monotonic() + 0.25
                    else:
                        async with self.lock:
                            await self.event(frame)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self.last_error = str(error) if isinstance(error, (RpcError, ValueError)) else type(error).__name__
                log.warning("Discord sync disconnected: %s", self.last_error)
            finally:
                self.connected = False
                if "beating" in locals():
                    beating.cancel()
                await self.rpc.close()
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30)

    async def close(self):
        if self.task:
            self.task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.task
        await self.rpc.close()

"""One conversation, one execution owner; Discord is another live client."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import logging
import re
import time
from pathlib import Path
from urllib.parse import urlparse

from .discord import chunks, created_at
from .rpc import RpcError

log = logging.getLogger("hermes.discord-sync")

MEDIA = re.compile(r"(?m)^MEDIA:(/[^\r\n]+)$")
MENTION = re.compile(r"<@!?\d+>")
MAX_FILE = 10 * 1024 * 1024
DISCORD_HOSTS = {"cdn.discordapp.com", "media.discordapp.net"}
DESKTOP_SOURCES = {"desktop", "tui"}
DEFAULT_TITLE = "discord session"
PIN_TAG = "pinned"
DESKTOP_ONLY = "needs the desktop app. secrets and desktop-only actions are not sent through discord."
USAGE = "commands: !stop, !unlink, !approve <request> <choice>, !answer <request> <question> <answer>"

# Cadences, in seconds.
STREAM_EDIT_EVERY = 1
TYPING_EVERY = 8
POLL_EVERY = 1
SCAN_EVERY = 30
SCAN_AFTER_CHANGE = 5
HEARTBEAT_EVERY = 2
TICK_EVERY = 0.25


class Cadence:
    """Rate limit for periodic work: due() is true at most once per interval."""

    def __init__(self, interval, clock):
        self.interval = interval
        self.clock = clock
        self.last = float("-inf")

    def due(self, interval=None):
        now = self.clock()
        if now - self.last < (interval or self.interval):
            return False
        self.last = now
        return True


def _is_command(text):
    return text in {"!stop", "!unlink"} or text.startswith(("!approve ", "!answer "))


def _is_published(message):
    """Only real user and assistant turns reach Discord: no hidden context, tools or system rows."""
    has_row = message.get("row_id") is not None
    hidden = message.get("display_kind") == "hidden"
    conversational = message.get("role") in {"user", "assistant"}
    has_text = bool((message.get("text") or "").strip())
    return has_row and not hidden and conversational and has_text


def _is_deliverable(path):
    return path.is_file() and path.stat().st_size <= MAX_FILE


def _is_discord_file(attachment):
    url = urlparse(attachment["url"])
    discord_hosted = url.scheme == "https" and url.hostname in DISCORD_HOSTS
    small_enough = attachment.get("size", MAX_FILE + 1) <= MAX_FILE
    return discord_hosted and small_enough


def _title_from(text):
    first_line = (text.splitlines() or [""])[0]
    return first_line[:90] or DEFAULT_TITLE


def _approval_text(rid, params):
    lines = [params.get("description") or "approval needed", params.get("command", "")]
    lines += [f"`!approve {rid} {choice}`" for choice in params.get("choices", [])]
    return "\n".join(lines)


def _clarify_text(rid, params):
    lines = []
    for index, question in enumerate(params.get("questions", []), 1):
        lines.append(f"{index}. {question['question']}")
        lines += [f"   {number}: {choice}" for number, choice in enumerate(question.get("choices") or [], 1)]
        lines.append(f"`!answer {rid} {index} your answer`")
    return "\n".join(lines) + "\n"


def _describe(error):
    if isinstance(error, (RpcError, ValueError)):
        return str(error)
    return type(error).__name__


class Bridge:
    def __init__(
        self,
        rpc,
        discord,
        store,
        forum,
        allowed_users,
        *,
        inbox=None,
        pins=None,
        notify=None,
        guard=None,
        heartbeat=None,
        auto_link=True,
        clock=time.monotonic,
    ):
        self.rpc = rpc
        self.discord = discord
        self.store = store
        self.forum = str(forum)
        self.allowed_users = {str(user) for user in allowed_users}
        all_numeric = all(user.isdecimal() for user in self.allowed_users)
        if not self.allowed_users or not all_numeric:
            raise ValueError("Set an explicit numeric Discord user allowlist")
        self.inbox = inbox
        self.pins = pins
        self.notify = notify or (lambda session: None)
        self.guard = guard or (lambda: True)
        self.heartbeat = heartbeat or (lambda: None)
        self.auto_link = auto_link
        # Only sessions started after sync was first enabled are auto-linked,
        # so enabling it never floods the forum with the whole history.
        self.auto_since = float(store.meta("auto_link_since", time.time()))
        # Posts from before this process started were answered by the normal gateway.
        self.adopt_since = time.time()
        self.ignored_threads = set()
        self.pin_tag = None
        self.lock = asyncio.Lock()
        self.connected = False
        self.last_error = None
        self.scan_cadence = Cadence(SCAN_EVERY, clock)
        self.stream_cadence = Cadence(STREAM_EDIT_EVERY, clock)
        self.typing_cadence = Cadence(TYPING_EVERY, clock)
        self.poll_cadence = Cadence(POLL_EVERY, clock)
        self.poll_index = 0
        self.sessions_changed = False
        # Per backend connection. Stored session id -> runtime id; the rest keyed by runtime id.
        self.runtime = {}
        self.streams = {}
        self.working = {}
        self.requests = {}
        self.last_seq = {}

    # Lookups

    def _by_session(self, session):
        return next((link for link in self.store.links() if link["session"] == session), None)

    def _by_thread(self, thread):
        return next((link for link in self.store.links() if link["thread"] == str(thread)), None)

    def _by_runtime(self, runtime):
        return next(
            (link for link in self.store.links() if self.runtime.get(link["session"]) == runtime), None
        )

    def _from_allowed_user(self, message):
        author = message.get("author") or {}
        automated = author.get("bot") or message.get("webhook_id")
        return not automated and str(author.get("id")) in self.allowed_users

    # Linking sessions and posts

    async def attach(self, link):
        snapshot = await self.rpc.call(
            "session.resume",
            {
                "session_id": link["session"],
                "lazy": True,
                "defer_history": True,
                "inline_images": False,
                "close_on_disconnect": False,
            },
        )
        runtime = snapshot["session_id"]
        self.runtime[link["session"]] = runtime
        inflight = snapshot.get("inflight") or {}
        if inflight.get("streaming") and inflight.get("assistant"):
            self.streams[runtime] = inflight["assistant"]
        replay = await self.rpc.call("session.events.since", {"session_id": runtime, "last_seen": 0})
        # Reconcile durable rows, then start at the snapshot's boundary. A new
        # transport can have already queued some of the same replay frames.
        self.last_seq[runtime] = replay.get("latest_seq", 0)
        for request in replay.get("open_requests", []):
            await self.show_request(request)
        await self.reconcile(link)

    async def link(self, session):
        async with self.lock:
            return await self._link(session)

    async def _link(self, session, row=None):
        existing = self._by_session(session)
        if existing:
            return existing
        if self.store.known(session=session):
            raise ValueError("This session was unlinked; it stays off Discord")
        if row is None:
            row = await self._session_row(session)
        if row is None or row.get("source") not in DESKTOP_SOURCES:
            raise ValueError("Select a desktop session in the default profile")
        await self._open_post(session, row.get("title") or session)
        link = self._by_session(session)
        await self.attach(link)
        return link

    async def _session_row(self, session):
        # Resolve before publishing anything. Only desktop conversations in
        # this profile may be exposed; no title-based or foreign fallback.
        listing = await self.rpc.call("session.list", {"limit": 10000})
        return next((row for row in listing["sessions"] if row["id"] == session), None)

    async def _open_post(self, session, title):
        """Create the forum post for a session and record the link. Returns the post's thread id."""
        post = await self.discord.create_post(self.forum, title, session)
        thread = post["id"]
        starter = (post.get("message") or {}).get("id", thread)
        self.store.link(session, thread, title, starter)
        return thread

    async def _new_session(self, title, key):
        created = await self.rpc.call(
            "session.create",
            {"source": "desktop", "title": title, "close_on_disconnect": False, "idempotency_key": key},
        )
        session = created["stored_session_id"]
        runtime = created["session_id"]
        self.runtime[session] = runtime
        self.last_seq[runtime] = 0
        return session

    async def adopt(self, thread, starter=None):
        """A new forum post becomes a real desktop session, not a gateway chat."""
        tid = str(thread["id"])
        already_seen = tid in self.ignored_threads or self.store.known(thread=tid)
        if already_seen:
            return
        if starter is None:
            starter = await self.discord.message(tid, tid)
        if not self._is_new_post_from_user(tid, starter):
            self.ignored_threads.add(tid)
            return
        title = thread.get("name") or DEFAULT_TITLE
        session = await self._new_session(title, f"discord-sync:{tid}")
        # The cursor sits just before the starter, so the post itself is the first turn.
        self.store.link(session, tid, title, str(int(tid) - 1))
        await self.receive(self._by_session(session), starter)
        self.store.update(session, cursor=tid)
        self.notify(session)

    def _is_new_post_from_user(self, tid, starter):
        if starter is None:
            return False
        posted_since_start = created_at(tid) >= self.adopt_since
        return posted_since_start and self._from_allowed_user(starter)

    async def mention(self, event):
        """An @mention from an allowed user starts a synced forum post instead of a gateway thread."""
        message = event["message"]
        channel = event["channel"]
        mid = str(message["id"])
        if not self._from_allowed_user(message):
            return
        if self.store.inbox_status(channel, mid):
            return
        self.store.received(channel, mid, "sending")
        text = MENTION.sub("", message.get("content", "")).strip()
        title = _title_from(text)
        session = await self._new_session(title, f"discord-sync:mention:{mid}")
        thread = await self._open_post(session, title)
        await self.discord.reply(channel, mid, f"on it in <#{thread}>")
        link = self._by_session(session)
        submitted = await self._submit(link, text, message)
        status = "ignored" if submitted is None else "accepted"
        row_id = None if submitted is None else submitted["row_id"]
        self.store.received(channel, mid, status, row_id)
        await self.reconcile(link)

    async def scan(self):
        interval = SCAN_AFTER_CHANGE if self.sessions_changed else SCAN_EVERY
        if not self.scan_cadence.due(interval):
            return
        self.sessions_changed = False
        for thread in await self.discord.active_threads():
            if str(thread.get("parent_id")) == self.forum:
                await self.adopt(thread)
        if self.auto_link:
            await self._auto_link_next()

    async def _auto_link_next(self):
        listing = await self.rpc.call("session.list", {"limit": 500})
        candidate = next((row for row in listing["sessions"] if self._should_auto_link(row)), None)
        if candidate is None:
            return
        # One per scan: a backfill must not hold the loop while it copies a long history.
        await self._link(candidate["id"], candidate)

    def _should_auto_link(self, row):
        from_desktop = row.get("source") in DESKTOP_SOURCES
        started_since_enabled = float(row.get("started_at") or 0) >= self.auto_since
        return from_desktop and started_since_enabled and not self.store.known(session=row["id"])

    # Writing to Discord

    async def upsert(self, link, address, body):
        projection = self.store.get_projection(link["session"]).get(address)
        if projection and projection["body"] == body:
            return
        if projection and not projection["owned"]:
            # A bot cannot edit a human's message. Corrections are a separate
            # visible projection, never a rewrite of the original phone input.
            projection = None
        previous_ids = projection["ids"] if projection else []
        parts = chunks(body)
        sent_ids = []
        for index, part in enumerate(parts):
            # Long backfills keep the gateway reservation alive.
            self.heartbeat()
            sent = await self._write_part(link, address, index, part, previous_ids)
            sent_ids.append(sent["id"])
        for stale in previous_ids[len(parts) :]:
            await self.discord.delete(link["thread"], stale)
        self.store.project(link["session"], address, sent_ids, body)

    async def _write_part(self, link, address, index, part, previous_ids):
        """Edit the message already holding this part, or send a new one when there is none."""
        edited = None
        if index < len(previous_ids):
            edited = await self.discord.edit(link["thread"], previous_ids[index], part)
        if edited is not None:
            return edited
        return await self.discord.send(link["thread"], part, key=f"{link['session']}:{address}:{index}")

    async def remove(self, link, address):
        projection = self.store.get_projection(link["session"]).get(address)
        owned_ids = projection["ids"] if projection and projection["owned"] else []
        for mid in owned_ids:
            await self.discord.delete(link["thread"], mid)
        self.store.forget(link["session"], address)

    async def reconcile(self, link):
        runtime = self.runtime[link["session"]]
        history = await self.rpc.call("session.history", {"session_id": runtime})
        published = set()
        for message in filter(_is_published, history["messages"]):
            address = f"row:{message['row_id']}"
            published.add(address)
            text = message["text"]
            if message["role"] == "user":
                await self.upsert(link, address, f"you: {text}")
                continue
            await self.upsert(link, address, text)
            await self.artifacts(link, address, text)
        for address in self.store.get_projection(link["session"]):
            if address.startswith("row:") and address not in published:
                await self.remove(link, address)
        self.notify(link["session"])

    async def artifacts(self, link, address, text):
        # Only explicit MEDIA: deliverables in assistant text, never tool args,
        # reasoning, system prompts, or paths mentioned in normal prose.
        for raw in MEDIA.findall(text):
            key = f"file:{address}:{hashlib.sha256(raw.encode()).hexdigest()[:16]}"
            path = Path(raw)
            already_sent = key in self.store.get_projection(link["session"])
            if already_sent or not _is_deliverable(path):
                continue
            data = await asyncio.to_thread(path.read_bytes)
            sent = await self.discord.upload(link["thread"], path.name, data, key=f"{link['session']}:{key}")
            self.store.project(link["session"], key, [sent["id"]], raw)

    # Reading from Discord

    async def receive(self, link, message):
        if not self.guard():
            raise ConnectionError("Gateway reservation unavailable")
        if not self._from_allowed_user(message):
            return
        thread = link["thread"]
        mid = str(message["id"])
        previous = self.store.inbox_status(thread, mid)
        if previous:
            await self._warn_if_unconfirmed(link, mid, previous)
            return
        self.store.received(thread, mid, "sending")
        try:
            status, row_id = await self._accept(link, message)
        except RpcError as error:
            await self._reject(link, mid, f"message was not accepted: {error}")
            return
        except ValueError as error:
            await self._reject(link, mid, str(error))
            return
        self.store.received(thread, mid, status, row_id)
        # Transport failures deliberately keep the durable `sending` intent.
        # There is no prompt idempotency key in the host's current RPC contract:
        # an unacknowledged send MUST NOT be blindly replayed after reconnect.

    async def _accept(self, link, message):
        """Run a command or submit a prompt. Returns the inbox status and the user row id."""
        text = message.get("content", "")
        if _is_command(text):
            await self.command(link, text)
            return "accepted", None
        submitted = await self._submit(link, text, message)
        if submitted is None:
            return "ignored", None
        row_id = submitted["row_id"]
        if row_id is not None:
            # The human's own message already shows this row; the bot never reposts it.
            body = f"you: {submitted['prompt']}"
            self.store.project(link["session"], f"row:{row_id}", [str(message["id"])], body, owned=False)
        self.notify(link["session"])
        return "accepted", row_id

    async def _submit(self, link, text, message):
        """Send a prompt into the shared session. Returns None when there is nothing to send."""
        runtime = self.runtime[link["session"]]
        refs = await self.inbound_attachments(message, runtime)
        prompt = "\n".join([text, *refs]).strip()
        if not prompt:
            return None
        # The normal shared-session submit takes the same turn lock as
        # Desktop. Never write messages or acquire a second session lease.
        result = await self.rpc.call(
            "prompt.submit", {"session_id": runtime, "text": prompt, "surface": "desktop"}
        )
        self.working[runtime] = link
        await self.discord.typing(link["thread"])
        return {"prompt": prompt, "row_id": result.get("user_row_id")}

    async def _reject(self, link, mid, reason):
        self.store.received(link["thread"], mid, "rejected")
        await self.upsert(link, f"rejected:{mid}", reason)

    async def _warn_if_unconfirmed(self, link, mid, previous):
        if previous["status"] != "sending":
            return
        warning = "delivery could not be confirmed after a disconnect. check desktop before resending."
        await self.upsert(link, f"uncertain:{mid}", warning)
        self.store.received(link["thread"], mid, "uncertain")

    async def inbound_attachments(self, message, runtime):
        refs = []
        for attachment in message.get("attachments", []):
            if not _is_discord_file(attachment):
                raise ValueError("Attachment must be a Discord-hosted file under 10 MiB")
            data = await self.discord.download(attachment["url"], MAX_FILE)
            ref = await self._attach_file(runtime, attachment, data)
            if ref:
                refs.append(ref)
        return refs

    async def _attach_file(self, runtime, attachment, data):
        """Attach to the session the way Desktop does. Returns the prompt reference for non-images."""
        mime = attachment.get("content_type") or "application/octet-stream"
        encoded = base64.b64encode(data).decode()
        if mime.startswith("image/"):
            await self.rpc.call(
                "image.attach_bytes",
                {"session_id": runtime, "filename": attachment["filename"], "content_base64": encoded},
            )
            return None
        result = await self.rpc.call(
            "file.attach",
            {
                "session_id": runtime,
                "name": attachment["filename"],
                "data_url": f"data:{mime};base64,{encoded}",
            },
        )
        return result["ref_text"]

    async def drain(self):
        """Handle what the gateway pushed: forum messages, @mentions and tag edits."""
        if self.inbox is None:
            return
        handlers = {"forum": self._forum_message, "mention": self.mention, "tags": self._pin_from_tags}
        for handle, event in self.inbox.pending():
            handler = handlers.get(event.get("kind"))
            try:
                if handler is not None:
                    await handler(event)
            except (RpcError, ValueError) as error:
                log.warning("Discord sync could not handle a pushed %s: %s", event.get("kind"), error)
            # Transport errors propagate and leave the event queued for after the reconnect.
            self.inbox.done(handle)

    async def _forum_message(self, event):
        message = event["message"]
        thread = event["thread"]
        link = self._by_thread(thread)
        is_starter = str(message["id"]) == thread
        if link is None and is_starter:
            await self.adopt({"id": thread, "name": event.get("name")}, starter=message)
            return
        if link is None:
            return
        await self.receive(link, message)
        self._advance_cursor(link, message)

    def _advance_cursor(self, link, message):
        current = self._by_session(link["session"])
        is_newer = current is not None and int(message["id"]) > int(current["cursor"])
        if is_newer:
            self.store.update(link["session"], cursor=str(message["id"]))

    # Commands from Discord

    async def command(self, link, text):
        runtime = self.runtime[link["session"]]
        if text == "!stop":
            await self.rpc.call("session.interrupt", {"session_id": runtime})
            return
        if text == "!unlink":
            self.store.update(link["session"], enabled=0)
            await self.upsert(link, "unlinked", "sync stopped. the desktop conversation is unchanged.")
            return
        if text.startswith("!approve "):
            await self._approve(runtime, text)
            return
        await self._answer(runtime, text)

    async def _approve(self, runtime, text):
        parts = text.split(maxsplit=2)
        if len(parts) != 3:
            raise ValueError(USAGE)
        _, rid, choice = parts
        request = self._open_request(
            rid, "approval", runtime, "That approval is no longer open in this session"
        )
        if choice not in request["params"].get("choices", []):
            raise ValueError("Choose one of the offered approval options")
        await self.rpc.call("request.answer", {"id": rid, "result": {"choice": choice}})

    async def _answer(self, runtime, text):
        parts = text.split(maxsplit=3)
        if len(parts) != 4 or not parts[2].isdecimal():
            raise ValueError(USAGE)
        _, rid, number, answer = parts
        request = self._open_request(
            rid, "clarify", runtime, "That question is no longer open in this session"
        )
        questions = request["params"]["questions"]
        index = int(number) - 1
        if not 0 <= index < len(questions):
            raise ValueError("Question number is out of range")
        question = questions[index]
        choices = question.get("choices") or []
        picked_by_number = answer.isdecimal() and 0 < int(answer) <= len(choices)
        if picked_by_number:
            answer = choices[int(answer) - 1]
        locked = await self.rpc.call(
            "clarify.lock", {"request_id": rid, "question_id": question["qid"], "answer": answer}
        )
        request.setdefault("answers", {})[question["qid"]] = answer
        if locked.get("remaining") == 0:
            await self.rpc.call("request.answer", {"id": rid, "result": {"answers": request["answers"]}})

    def _open_request(self, rid, method, runtime, missing):
        request = self.requests.get(rid)
        is_open = (
            request is not None and request["method"] == method and request["params"]["session_id"] == runtime
        )
        if not is_open:
            raise ValueError(missing)
        return request

    # Backend events

    async def show_request(self, frame):
        rid = str(frame.get("id", ""))
        params = frame.get("params") or {}
        link = self._by_runtime(params.get("session_id"))
        if link is None or not rid:
            return
        self.requests[rid] = frame
        method = frame["method"]
        if method == "approval":
            body = _approval_text(rid, params)
        elif method == "clarify":
            body = _clarify_text(rid, params)
        else:
            body = f"{method} {DESKTOP_ONLY}"
        await self.upsert(link, f"request:{rid}", body)

    async def event(self, frame):
        method = frame.get("method")
        if method == "bridge.disconnected":
            raise ConnectionError("Desktop connection closed")
        if method != "event":
            await self.show_request(frame)
            return
        event = frame.get("params") or {}
        if event.get("type") == "sessions.changed":
            self.sessions_changed = True
            return
        if not self._is_new_event(event):
            return
        link = self._by_runtime(event.get("session_id"))
        if link is None:
            return
        handlers = {
            "message.start": self._on_message_start,
            "message.delta": self._on_message_delta,
            "message.interim": self._on_message_interim,
            "message.complete": self._on_message_complete,
            "tool.start": self._on_tool_start,
            "tool.complete": self._on_tool_complete,
            "session.title": self._on_title,
            "request.cancel": self._on_request_cancel,
            "error": self._on_error,
        }
        handler = handlers.get(event.get("type"))
        if handler is not None:
            await handler(link, event["session_id"], event.get("payload") or {})

    def _is_new_event(self, event):
        runtime = event.get("session_id")
        seq = event.get("seq", 0)
        if not seq:
            return True
        if seq <= self.last_seq.get(runtime, 0):
            return False
        self.last_seq[runtime] = seq
        return True

    async def _on_message_start(self, link, runtime, payload):
        self.streams[runtime] = ""
        self.working[runtime] = link

    async def _on_message_delta(self, link, runtime, payload):
        self.streams[runtime] = self.streams.get(runtime, "") + payload.get("text", "")

    async def _on_message_interim(self, link, runtime, payload):
        await self.reconcile(link)
        self.streams[runtime] = ""
        await self.remove(link, "stream")

    async def _on_message_complete(self, link, runtime, payload):
        await self.reconcile(link)
        self.streams.pop(runtime, None)
        self.working.pop(runtime, None)
        await self.remove(link, "stream")
        await self.remove(link, "activity")
        if payload.get("status") in {"error", "interrupted"}:
            await self.upsert(link, "outcome", payload.get("error") or payload["status"])

    async def _on_tool_start(self, link, runtime, payload):
        await self.upsert(link, "activity", f"working: {payload.get('name', 'tool')}")

    async def _on_tool_complete(self, link, runtime, payload):
        await self.remove(link, "activity")

    async def _on_title(self, link, runtime, payload):
        title = payload.get("title")
        if not title or title == link["title"]:
            return
        await self.discord.rename(link["thread"], title)
        self.store.update(link["session"], title=title)

    async def _on_request_cancel(self, link, runtime, payload):
        rid = str(payload.get("id") or payload.get("request_id") or "")
        self.requests.pop(rid, None)
        await self.remove(link, f"request:{rid}")

    async def _on_error(self, link, runtime, payload):
        await self.upsert(link, "outcome", payload.get("message") or "session error")

    # Pins. Discord allows one pinned post per forum, so a tag stands in.

    async def _pin_tag(self):
        if self.pin_tag is None:
            self.pin_tag = await self.discord.ensure_tag(self.forum, PIN_TAG)
        return self.pin_tag

    async def push_pins(self):
        """Desktop to Discord: tag the posts of pinned sessions."""
        if self.pins is None:
            return
        tag = await self._pin_tag()
        pinned = self.pins.pinned()
        for link in self.store.links():
            want = link["session"] in pinned
            if self.store.pin(link["session"]) == want:
                continue
            await self.discord.set_tag(link["thread"], tag, want)
            self.store.set_pin(link["session"], want)

    async def _pin_from_tags(self, event):
        """Discord to desktop: adding or removing the tag pins or unpins the session."""
        link = self._by_thread(event["thread"])
        if link is None or self.pins is None:
            return
        tag = await self._pin_tag()
        want = tag in event["tags"]
        if self.store.pin(link["session"]) == want:
            return
        self.pins.set(link["session"], want)
        self.store.set_pin(link["session"], want)

    # Main loop

    async def tick(self):
        async with self.lock:
            await self.drain()
            await self.push_pins()
            await self.scan()
            await self._flush_streams()
            await self._keep_typing()
            await self._poll_next_thread()

    async def _flush_streams(self):
        if not self.stream_cadence.due():
            return
        for runtime, text in list(self.streams.items()):
            link = self._by_runtime(runtime)
            if link and text:
                await self.upsert(link, "stream", text)

    async def _keep_typing(self):
        if not self.typing_cadence.due():
            return
        for link in list(self.working.values()):
            await self.discord.typing(link["thread"])

    async def _poll_next_thread(self):
        """Safety net for anything the push path missed: one thread per pass keeps a cycle cheap."""
        links = self.store.links()
        if not links or not self.poll_cadence.due():
            return
        link = links[self.poll_index % len(links)]
        self.poll_index += 1
        await self.discord.unarchive(link["thread"])
        for message in await self.discord.messages_after(link["thread"], link["cursor"]):
            await self.receive(link, message)
            self._advance_cursor(link, message)
            if self._by_session(link["session"]) is None:
                return
        await self.reconcile(link)

    async def beat(self):
        # Separate from the tick so a slow Discord pass never lets the gateway
        # reservation lapse and the normal bot answer a synced post twice.
        while True:
            self.heartbeat()
            await asyncio.sleep(HEARTBEAT_EVERY)

    async def serve(self):
        delay = 1
        while True:
            if not self.guard():
                await asyncio.sleep(1)
                continue
            beating = None
            try:
                await self._connect()
                beating = asyncio.create_task(self.beat())
                delay = 1
                await self._run()
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self.last_error = _describe(error)
                log.warning("Discord sync disconnected: %s", self.last_error)
            finally:
                self.connected = False
                if beating is not None:
                    beating.cancel()
                await self.rpc.close()
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30)

    async def _connect(self):
        await self.discord.validate(self.forum)
        await self.rpc.connect()
        self.runtime.clear()
        self.requests.clear()
        self.streams.clear()
        self.working.clear()
        self.last_seq.clear()
        async with self.lock:
            for link in self.store.links():
                await self.attach(link)
        self.connected = True
        self.last_error = None

    async def _run(self):
        deadline = time.monotonic()
        while True:
            if not self.guard():
                raise ConnectionError("Gateway reservation unavailable")
            timeout = max(0, deadline - time.monotonic())
            try:
                frame = await asyncio.wait_for(self.rpc.events.get(), timeout)
            except TimeoutError:
                await self.tick()
                deadline = time.monotonic() + TICK_EVERY
                continue
            async with self.lock:
                await self.event(frame)

    async def close(self):
        await self.rpc.close()

"""One conversation, one execution owner; Discord is another live client."""

from __future__ import annotations

import asyncio
import base64
import contextlib
import hashlib
import json
import logging
import re
import time
from collections import deque
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

from . import wording as words
from .discord import DiscordError, chunks, created_at
from .rpc import RpcError

log = logging.getLogger("hermes.discord-sync")

MEDIA = re.compile(r"(?m)^MEDIA:(/[^\r\n]+)$")
MENTION = re.compile(r"<@!?\d+>")
# A Hermes session reference, optionally `code`-wrapped: @session:<profile>/<id> or @session:<id>.
SESSION_REF = re.compile(r"`?@session:(?:[\w.-]+/)?(\d{8}_\d{6}_[0-9a-f]+)`?")
MAX_FILE = 10 * 1024 * 1024
DISCORD_HOSTS = {"cdn.discordapp.com", "media.discordapp.net"}
DESKTOP_SOURCES = {"desktop", "tui"}
DEFAULT_TITLE = "discord session"
PIN_TAG = "pinned"
# Where each post stands, shown as a tag in the forum list.
WORKING = "working"
NEEDS_YOU = "needs you"
DONE = "done"
STATUS_TAGS = (WORKING, NEEDS_YOU, DONE)
TURN_PING = "ping:turn"
DELETES_PER_PASS = 5
REQUEST_PINGS = {"approval": "needs your ok", "clarify": "has a question"}
DESKTOP_ONLY = "needs the desktop app. secrets and desktop-only actions are not sent through discord."
USAGE = "commands: !stop, !unlink, !approve <request> <choice>, !answer <request> <question> <answer>"
BUTTON_PREFIX = "hds:"
MAX_BUTTONS = 25
ACTIVITY_LINES = 6
PREVIEW_LIMIT = 80
# Bare replies that answer an open approval, as the native gateway accepts them.
APPROVE_WORDS = {"yes", "y", "approve", "allow", "ok"}
DENY_WORDS = {"no", "n", "deny", "reject"}
TURN_REACTIONS = {"error": words.FAILED, "interrupted": words.STOPPED}

# Cadences, in seconds.
STREAM_EDIT_EVERY = 1
TYPING_EVERY = 8
POLL_EVERY = 1
SCAN_EVERY = 30
SCAN_AFTER_CHANGE = 5
HEARTBEAT_EVERY = 2
STALL_WARN_AFTER = 5
TICK_EVERY = 0.25
# A fresh archive waits this long for its audit log entry before the poll treats it as idle.
AUDIT_LAG = 30


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
    return text in {"!stop", "/stop", "!unlink"} or text.startswith(("!approve ", "!answer "))


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


def _button(label, style, custom_id, disabled=False):
    label = label[: words.BUTTON_LABEL_LIMIT]
    return {"type": 2, "style": style, "label": label, "custom_id": custom_id, "disabled": disabled}


def _rows(buttons):
    """Discord action rows: five buttons each, at most five rows."""
    buttons = buttons[:MAX_BUTTONS]
    return [{"type": 1, "components": buttons[start : start + 5]} for start in range(0, len(buttons), 5)]


def _preview(text):
    flat = " ".join(str(text or "").split())
    return flat if len(flat) <= PREVIEW_LIMIT else flat[: PREVIEW_LIMIT - 1] + "…"


def _is_stale_archive(channel):
    """Archived long enough ago that a close by a person would already be in the audit log."""
    metadata = channel.get("thread_metadata") or {}
    if not metadata.get("archived"):
        return False
    stamp = metadata.get("archive_timestamp")
    if not stamp:
        return True
    return time.time() - datetime.fromisoformat(stamp).timestamp() >= AUDIT_LAG


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
        threads=None,
        guard=None,
        heartbeat=None,
        auto_link=True,
        wording=None,
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
        # Session id -> the Discord thread the native gateway answers it in, if any.
        self.threads = threads or (lambda session: None)
        self.guard = guard or (lambda: True)
        self.heartbeat = heartbeat or (lambda: None)
        self.auto_link = auto_link
        self.words = wording or words.Wording()
        # Only sessions started after sync was first enabled are auto-linked,
        # so enabling it never floods the forum with the whole history.
        self.auto_since = float(store.meta("auto_link_since", time.time()))
        # Posts from before this process started were answered by the normal gateway.
        self.adopt_since = time.time()
        self.ignored_threads = set()
        self.tag_ids = {}
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
        # The Discord message each running turn answers, (thread, message id), and the
        # messages queued behind it; their reactions show where each one stands.
        self.owner = {}
        self.waiting = {}
        # Tool progress lines per running turn, shown above the streamed reply.
        self.activity = {}
        # Turns a Discord message steered into; their finish pings like a Discord-started turn.
        self.steered = set()
        # Stored session id -> how its last turn ended (DONE or NEEDS_YOU), and the status tag on its post.
        self.finished = {}
        self.shown_status = {}

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
        if inflight.get("streaming"):
            self.working[runtime] = link
        replay = await self.rpc.call("session.events.since", {"session_id": runtime, "last_seen": 0})
        # Reconcile durable rows, then start at the snapshot's boundary. A new
        # transport can have already queued some of the same replay frames.
        self.last_seq[runtime] = replay.get("latest_seq", 0)
        for request in replay.get("open_requests", []):
            await self.show_request(request)
        await self.reconcile(link)
        has_turns = any(address.startswith("row:") for address in self.store.get_projection(link["session"]))
        if has_turns:
            # Whatever ran before this connection has ended; a missed finish must not stay "working".
            self.finished.setdefault(link["session"], DONE)

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

    async def adopt(self, thread, starter=None, *, pushed=False):
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
        await self.receive(self._by_session(session), starter, pushed=pushed)
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
        outcome = "ignored" if submitted is None else submitted["status"]
        await self._acknowledge(link, channel, mid, outcome)
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

    def _thread_of(self, session):
        link = self._by_session(session)
        return link["thread"] if link else self.threads(session)

    def _link_sessions(self, body):
        """Session references become thread links when the session has a Discord thread."""

        def replace(match):
            thread = self._thread_of(match.group(1))
            return f"<#{thread}>" if thread else match.group(0)

        return SESSION_REF.sub(replace, body)

    async def upsert(self, link, address, body, components=None):
        body = self._link_sessions(body)
        # Buttons are part of what is shown: a change to them alone is an edit too.
        shown = body if components is None else body + "\x00" + json.dumps(components)
        projection = self.store.get_projection(link["session"]).get(address)
        if projection and projection["body"] == shown:
            return
        if projection and not projection["owned"]:
            # A bot cannot edit a human's message. Corrections are a separate
            # visible projection, never a rewrite of the original phone input.
            projection = None
        previous_ids = projection["ids"] if projection else []
        async with self._writable(link):
            sent_ids = await self._write_parts(link, address, chunks(body), previous_ids, components)
        self.store.project(link["session"], address, sent_ids, shown)

    async def _write_parts(self, link, address, parts, previous_ids, components):
        sent_ids = []
        for index, part in enumerate(parts):
            # Long backfills keep the gateway reservation alive.
            self.heartbeat()
            # Buttons sit under the last part.
            buttons = components if index == len(parts) - 1 else None
            sent = await self._write_part(link, address, index, part, previous_ids, buttons)
            sent_ids.append(sent["id"])
        for stale in previous_ids[len(parts) :]:
            await self.discord.delete(link["thread"], stale)
        return sent_ids

    async def _write_part(self, link, address, index, part, previous_ids, components):
        """Edit the message already holding this part, or send a new one when there is none."""
        thread = link["thread"]
        edited = None
        if index < len(previous_ids):
            edited = await self.discord.edit(thread, previous_ids[index], part, components=components)
        if edited is not None:
            return edited
        # Per post: a session reposted after an unarchive sends its rows again.
        key = f"{thread}:{link['session']}:{address}:{index}"
        if not address.startswith("row:"):
            # Discord returns the earlier message for a reused nonce, even a deleted one.
            # Transcript rows keep a fixed key so a replay never posts twice; transient
            # messages (each turn's live message, prompts, outcomes) get a fresh one.
            key += f":{time.time_ns()}"
        return await self.discord.send(thread, part, key=key, components=components)

    async def remove(self, link, address):
        projection = self.store.get_projection(link["session"]).get(address)
        owned_ids = projection["ids"] if projection and projection["owned"] else []
        if owned_ids:
            async with self._writable(link):
                for mid in owned_ids:
                    await self.discord.delete(link["thread"], mid)
        self.store.forget(link["session"], address)

    async def reconcile(self, link, *, adopt_stream=False):
        runtime = self.runtime[link["session"]]
        history = await self.rpc.call("session.history", {"session_id": runtime})
        if adopt_stream:
            self._adopt_stream(link, history["messages"])
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

    def _adopt_stream(self, link, messages):
        """The streamed reply becomes the finished row in place: edited, not deleted and reposted."""
        projections = self.store.get_projection(link["session"])
        stream = projections.get("stream")
        if stream is None:
            return
        unshown = [
            message
            for message in filter(_is_published, messages)
            if message["role"] == "assistant" and f"row:{message['row_id']}" not in projections
        ]
        if not unshown:
            return
        address = f"row:{unshown[-1]['row_id']}"
        self.store.project(link["session"], address, stream["ids"], stream["body"])
        self.store.forget(link["session"], "stream")

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
            upload_key = f"{link['session']}:{key}"
            async with self._writable(link):
                sent = await self.discord.upload(link["thread"], path.name, data, key=upload_key)
            self.store.project(link["session"], key, [sent["id"]], raw)

    # Reading from Discord

    async def receive(self, link, message, *, pushed=False):
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
        if not pushed:
            # The gateway reacts the moment a pushed message arrives; the poll catches up here.
            await self.discord.react(thread, mid, words.SEEN)
        self.store.received(thread, mid, "sending")
        try:
            status, row_id, outcome = await self._accept(link, message)
        except RpcError as error:
            await self._reject(link, mid, f"message was not accepted: {error}")
            return
        except ValueError as error:
            await self._reject(link, mid, str(error))
            return
        self.store.received(thread, mid, status, row_id)
        await self._acknowledge(link, thread, mid, outcome)
        # Transport failures deliberately keep the durable `sending` intent.
        # There is no prompt idempotency key in the host's current RPC contract:
        # an unacknowledged send MUST NOT be blindly replayed after reconnect.

    async def _accept(self, link, message):
        """Run a command, answer an open prompt or submit a prompt.

        Returns the inbox status, the user row id and what happened to the message.
        """
        text = message.get("content", "")
        if _is_command(text):
            await self.command(link, text)
            return "accepted", None, "done"
        if not message.get("attachments") and await self._answer_by_reply(link, text.strip()):
            return "accepted", None, "done"
        submitted = await self._submit(link, text, message)
        if submitted is None:
            return "ignored", None, "ignored"
        row_id = submitted["row_id"]
        if row_id is not None:
            # The human's own message already shows this row; the bot never reposts it.
            body = f"you: {submitted['prompt']}"
            self.store.project(link["session"], f"row:{row_id}", [str(message["id"])], body, owned=False)
        self.notify(link["session"])
        return "accepted", row_id, submitted["status"]

    async def _acknowledge(self, link, channel, mid, outcome):
        """Say where a message stands, the way the native bot does: reactions plus a busy ack."""
        if outcome == "done":
            await self._swap(channel, mid, words.SEEN, words.DONE)
            return
        if outcome == "ignored":
            await self.discord.unreact(channel, mid, words.SEEN)
            return
        runtime = self.runtime[link["session"]]
        busy = words.STATUS_REACTIONS.get(outcome)
        if busy is None:
            # A new turn: this message keeps 👀 until the turn finishes. An older
            # owner whose completion was missed (a reconnect) is done by now.
            await self._finish_turn(runtime, "complete", promote=False)
            self.owner[runtime] = (channel, mid)
            return
        await self._swap(channel, mid, words.SEEN, busy)
        if outcome in {"steered", "redirected"}:
            self.steered.add(runtime)
            # The run now answers this message, so its live message moves below it:
            # the next flush posts it fresh instead of editing the one above.
            await self.remove(link, "stream")
        if outcome == "queued":
            self.waiting.setdefault(runtime, deque()).append((channel, mid))
        ack = self.words.ack(outcome)
        if ack:
            await self.discord.reply(channel, mid, ack, key=f"ack:{mid}")

    async def _swap(self, channel, mid, old, new):
        # Add before removing, so the message is never without a status.
        await self.discord.react(channel, mid, new)
        await self.discord.unreact(channel, mid, old)

    async def _finish_turn(self, runtime, status, *, promote=True):
        """Mark the message a turn answered, then hand 👀 to the next queued message."""
        owner = self.owner.pop(runtime, None)
        if owner is not None:
            await self._swap(*owner, words.SEEN, TURN_REACTIONS.get(status, words.DONE))
        queued = self.waiting.get(runtime)
        if not promote or not queued:
            return
        following = queued.popleft()
        await self._swap(*following, words.QUEUED, words.SEEN)
        self.owner[runtime] = following

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
        status = result.get("status") or "streaming"
        return {"prompt": prompt, "row_id": result.get("user_row_id"), "status": status}

    async def _reject(self, link, mid, reason):
        self.store.received(link["thread"], mid, "rejected")
        await self._swap(link["thread"], mid, words.SEEN, words.FAILED)
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
        handlers = {
            "forum": self._forum_message,
            "mention": self.mention,
            "tags": self._thread_updated,
            "press": self.press,
        }
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
            await self.adopt({"id": thread, "name": event.get("name")}, starter=message, pushed=True)
            return
        if link is None:
            # The gateway already marked it seen; this post is not synced, so take that back.
            await self.discord.unreact(thread, message["id"], words.SEEN)
            return
        await self.receive(link, message, pushed=True)
        self._advance_cursor(link, message)

    def _advance_cursor(self, link, message):
        current = self._by_session(link["session"])
        is_newer = current is not None and int(message["id"]) > int(current["cursor"])
        if is_newer:
            self.store.update(link["session"], cursor=str(message["id"]))

    # Commands and answers from Discord

    async def command(self, link, text):
        runtime = self.runtime[link["session"]]
        if text in {"!stop", "/stop"}:
            await self.rpc.call("session.interrupt", {"session_id": runtime})
            return
        if text == "!unlink":
            self.store.update(link["session"], enabled=0)
            await self.upsert(link, "unlinked", "sync stopped. the desktop conversation is unchanged.")
            return
        if text.startswith("!approve "):
            parts = text.split(maxsplit=2)
            if len(parts) != 3:
                raise ValueError(USAGE)
            await self._approve(runtime, parts[1], parts[2], None)
            return
        await self._answer(runtime, text)

    async def _approve(self, runtime, rid, choice, user):
        request = self._open_request(
            rid, "approval", runtime, "That approval is no longer open in this session"
        )
        if choice not in request["params"].get("choices", []):
            raise ValueError("Choose one of the offered approval options")
        answered = await self.rpc.call("request.answer", {"id": rid, "result": {"choice": choice}})
        label = self.words.approval_outcome(choice)
        if answered.get("status") == "expired":
            label = self.words.text("platform.discord.approval.expired")
        elif user:
            label = self.words.text("platform.discord.approval.by_user", label=label, user=user)
        await self._settle_request(rid, label)

    async def _answer(self, runtime, text):
        parts = text.split(maxsplit=3)
        if len(parts) != 4 or not parts[2].isdecimal():
            raise ValueError(USAGE)
        _, rid, number, answer = parts
        request = self._open_request(
            rid, "clarify", runtime, "That question is no longer open in this session"
        )
        index = int(number) - 1
        if not 0 <= index < len(request["params"]["questions"]):
            raise ValueError("Question number is out of range")
        await self._lock_answer(rid, request, index, answer)

    async def _lock_answer(self, rid, request, index, answer):
        question = request["params"]["questions"][index]
        choices = question.get("choices") or []
        picked_by_number = answer.isdecimal() and 0 < int(answer) <= len(choices)
        if picked_by_number:
            answer = choices[int(answer) - 1]
        locked = await self.rpc.call(
            "clarify.lock", {"request_id": rid, "question_id": question["qid"], "answer": answer}
        )
        request.setdefault("answers", {})[question["qid"]] = answer
        if locked.get("status") == "expired":
            # Answered elsewhere (Desktop) or timed out; the gateway sends no cancel for the former.
            request["answers"].pop(question["qid"])
            await self._settle_request(rid, "closed")
            return False
        if locked.get("remaining") == 0:
            await self.rpc.call("request.answer", {"id": rid, "result": {"answers": request["answers"]}})
            await self._settle_request(rid, None)
            return True
        await self._show_request_body(request)
        return True

    async def _answer_by_reply(self, link, text):
        """A plain reply answers the open prompt, as with the native bot. False when none is open."""
        runtime = self.runtime[link["session"]]
        open_requests = [
            frame for frame in self.requests.values() if frame["params"].get("session_id") == runtime
        ]
        if not open_requests or not text:
            return False
        request = open_requests[-1]
        rid = str(request["id"])
        if request["method"] == "clarify":
            # A closed question must not swallow the reply; it goes to the agent instead.
            return await self._lock_answer(rid, request, self._next_question(request), text)
        lowered = text.lower()
        choices = request["params"].get("choices", [])
        if lowered in APPROVE_WORDS and "once" in choices:
            await self._approve(runtime, rid, "once", None)
            return True
        if lowered in DENY_WORDS and "deny" in choices:
            await self._approve(runtime, rid, "deny", None)
            return True
        # Anything else is conversation; it goes to the agent like any other message.
        return False

    @staticmethod
    def _next_question(request):
        answered = request.get("answers", {})
        questions = request["params"]["questions"]
        return next((index for index, q in enumerate(questions) if q["qid"] not in answered), 0)

    def _open_request(self, rid, method, runtime, missing):
        request = self.requests.get(rid)
        is_open = (
            request is not None and request["method"] == method and request["params"]["session_id"] == runtime
        )
        if not is_open:
            raise ValueError(missing)
        return request

    async def press(self, event):
        """A button press the gateway acknowledged: an approval choice or a clarify option."""
        if str(event.get("user")) not in self.allowed_users:
            return
        parts = str(event.get("custom_id", "")).removeprefix(BUTTON_PREFIX).split(":")
        rid = parts[1] if len(parts) > 1 else ""
        request = self.requests.get(rid)
        if request is None:
            return
        runtime = request["params"]["session_id"]
        name = event.get("name") or None
        if parts[0] == "approve" and len(parts) == 3:
            await self._approve(runtime, rid, parts[2], name)
            return
        if parts[0] == "answer" and len(parts) == 4 and parts[2].isdecimal() and parts[3].isdecimal():
            index = int(parts[2])
            choices = request["params"]["questions"][index].get("choices") or []
            choice = int(parts[3])
            if choice < len(choices):
                await self._lock_answer(rid, request, index, choices[choice])

    # Prompts the agent is waiting on

    async def show_request(self, frame):
        rid = str(frame.get("id", ""))
        params = frame.get("params") or {}
        link = self._by_runtime(params.get("session_id"))
        if link is None or not rid:
            return
        self.requests[rid] = frame
        await self._show_request_body(frame)
        address = f"ping:{rid}"
        # One ping per request, even when a reconnect shows the card again.
        if address not in self.store.get_projection(link["session"]):
            await self._ping(link, address, REQUEST_PINGS.get(frame["method"], "needs the desktop app"))

    async def _show_request_body(self, frame, outcome=None):
        rid = str(frame["id"])
        params = frame["params"]
        link = self._by_runtime(params.get("session_id"))
        if link is None:
            return
        method = frame["method"]
        if method == "approval":
            body, buttons = self._approval_card(rid, params, outcome)
        elif method == "clarify":
            body, buttons = self._clarify_card(rid, params, frame.get("answers", {}), outcome)
        else:
            body, buttons = f"{method} {DESKTOP_ONLY}", []
        await self.upsert(link, f"request:{rid}", body, _rows(buttons))

    def _approval_card(self, rid, params, outcome):
        lines = [f"⚠️ **{self.words.text('gateway.exec_approval.header')}**"]
        if params.get("command"):
            lines.append(f"```\n{params['command']}\n```")
        if params.get("description"):
            lines.append(params["description"])
        if outcome:
            lines.append(f"**{outcome}**")
        buttons = [
            _button(
                self.words.approval_label(choice),
                self.words.approval_style(choice),
                f"{BUTTON_PREFIX}approve:{rid}:{choice}",
                disabled=bool(outcome),
            )
            for choice in params.get("choices", [])
        ]
        return "\n".join(lines), buttons

    def _clarify_card(self, rid, params, answers, outcome):
        lines = [f"❓ **{self.words.text('platform.discord.prompt.clarify_title')}**"]
        buttons = []
        for index, question in enumerate(params.get("questions", [])):
            answer = answers.get(question["qid"])
            lines.append(question["question"] if answer is None else f"~~{question['question']}~~ → {answer}")
            for number, choice in enumerate(question.get("choices") or []):
                custom_id = f"{BUTTON_PREFIX}answer:{rid}:{index}:{number}"
                label = f"{number + 1}. {choice}"
                buttons.append(_button(label, words.PRIMARY, custom_id, disabled=bool(outcome or answer)))
        if outcome:
            lines.append(f"**{outcome}**")
        else:
            lines.append(f"-# {self.words.text('platform.discord.prompt.clarify_hint_text')}")
        return "\n".join(lines), buttons

    async def _settle_request(self, rid, outcome):
        """Keep the answered card, buttons greyed out, so the thread shows what was decided."""
        frame = self.requests.pop(rid, None)
        if frame is None:
            return
        link = self._by_runtime(frame["params"].get("session_id"))
        if link is not None:
            await self.remove(link, f"ping:{rid}")
        if outcome is None:
            answered = len(frame.get("answers", {}))
            outcome = "answered" if answered else "closed"
        await self._show_request_body(frame, outcome)

    async def _expire_requests(self, runtime):
        for rid, frame in list(self.requests.items()):
            if frame["params"].get("session_id") == runtime:
                await self._settle_request(rid, "closed")

    # Backend events

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
        self.finished.pop(link["session"], None)
        await self.remove(link, "outcome")
        # The last turn's ping is stale once another turn starts.
        await self.remove(link, TURN_PING)
        # Typing shows at once, not on the next 8 second beat.
        await self.discord.typing(link["thread"])

    async def _on_message_delta(self, link, runtime, payload):
        self.streams[runtime] = self.streams.get(runtime, "") + payload.get("text", "")

    async def _on_message_interim(self, link, runtime, payload):
        await self._settle_live(link, runtime)
        self.streams[runtime] = ""

    async def _on_message_complete(self, link, runtime, payload):
        await self._settle_live(link, runtime)
        self.streams.pop(runtime, None)
        self.working.pop(runtime, None)
        await self._expire_requests(runtime)
        status = payload.get("status") or "complete"
        from_discord = runtime in self.owner or runtime in self.steered
        self.steered.discard(runtime)
        await self._finish_turn(runtime, status)
        if status in {"error", "interrupted"}:
            await self.upsert(link, "outcome", payload.get("error") or status)
        self.finished[link["session"]] = NEEDS_YOU if status == "error" else DONE
        if status == "error":
            await self._ping(link, TURN_PING, "hit an error")
        elif status == "complete" and from_discord:
            # Desktop-started turns only change the tag: whoever started them is at the desktop.
            await self._ping(link, TURN_PING, "done")

    async def _on_tool_start(self, link, runtime, payload):
        name = payload.get("name") or "tool"
        preview = _preview(payload.get("preview") or payload.get("context") or "")
        line = f"-# {self.words.tool_emoji(name)} {name}" + (f": {preview}" if preview else "")
        self.activity.setdefault(runtime, []).append(line)
        self.working[runtime] = link

    async def _on_tool_complete(self, link, runtime, payload):
        pass

    def _live_body(self, runtime):
        """The message showing a running turn: tool lines since the last reply, then the reply so far."""
        lines = self.activity.get(runtime, [])
        hidden = len(lines) - ACTIVITY_LINES
        shown = ([f"-# +{hidden} earlier"] if hidden > 0 else []) + lines[-ACTIVITY_LINES:]
        text = self.streams.get(runtime, "")
        return "\n".join(shown + ([text] if text else [])).strip()

    async def _settle_live(self, link, runtime):
        """The live message becomes the finished reply in place; without one, it goes away."""
        self.activity.pop(runtime, None)
        await self.reconcile(link, adopt_stream=True)
        await self.remove(link, "stream")

    async def _on_title(self, link, runtime, payload):
        title = payload.get("title")
        if not title or title == link["title"]:
            return
        async with self._writable(link):
            await self.discord.rename(link["thread"], title)
        self.store.update(link["session"], title=title)

    async def _on_request_cancel(self, link, runtime, payload):
        rid = str(payload.get("id") or payload.get("request_id") or "")
        self.requests.pop(rid, None)
        await self.remove(link, f"request:{rid}")
        await self.remove(link, f"ping:{rid}")

    async def _on_error(self, link, runtime, payload):
        await self.upsert(link, "outcome", payload.get("message") or "session error")
        self.finished[link["session"]] = NEEDS_YOU
        await self._ping(link, TURN_PING, "hit an error")

    async def _ping(self, link, address, text):
        """A new message that notifies the allowed users; edits never notify, so it is always fresh."""
        await self.remove(link, address)
        users = sorted(self.allowed_users)
        body = " ".join(f"<@{user}>" for user in users) + f" {text}"
        key = f"{link['session']}:{address}:{time.time_ns()}"
        async with self._writable(link):
            sent = await self.discord.send(link["thread"], body, key=key, ping=users)
        self.store.project(link["session"], address, [sent["id"]], body)

    # Forum tags: pins (Discord allows one pinned post per forum, so a tag stands in) and status.

    async def _tag(self, name):
        if name not in self.tag_ids:
            self.tag_ids[name] = await self.discord.ensure_tag(self.forum, name)
        return self.tag_ids[name]

    async def _pin_tag(self):
        return await self._tag(PIN_TAG)

    def _status(self, link):
        """Where a post stands: waiting on the user, running, or how its last turn ended."""
        runtime = self.runtime.get(link["session"])
        if runtime is None:
            return None
        asking = any(frame["params"].get("session_id") == runtime for frame in self.requests.values())
        if asking:
            return NEEDS_YOU
        if runtime in self.working:
            return WORKING
        return self.finished.get(link["session"])

    async def push_tags(self):
        """Desktop to Discord: tag pinned sessions' posts, and every post with its status."""
        pinned = None if self.pins is None else self.pins.pinned()
        pin_tag = None if pinned is None else await self._pin_tag()
        for link in self.store.links():
            session = link["session"]
            if self.store.closed(session):
                # Retagging waits for the post to reopen, rather than fetching it every tick.
                continue
            was_pinned = self.store.pin(session)
            pin = was_pinned if pinned is None else session in pinned
            status = self._status(link)
            status_changed = status is not None and self.shown_status.get(session) != status
            if pin == was_pinned and not status_changed:
                continue
            managed = set() if pin_tag is None else {pin_tag}
            wanted = [pin_tag] if pin and pin_tag is not None else []
            if status_changed:
                managed |= {await self._tag(name) for name in STATUS_TAGS}
                wanted.append(await self._tag(status))
            if not await self.discord.retag(link["thread"], managed, wanted):
                continue
            self.store.set_pin(session, pin)
            if status_changed:
                self.shown_status[session] = status

    async def _thread_updated(self, event):
        await self._pin_from_tags(event)
        link = self._by_thread(event["thread"])
        if link is not None:
            await self._follow_post(link, await self.discord.channel(link["thread"]))

    # An archived session's post is deleted, wherever the archive came from; unarchiving it
    # gives the session a new post with its history. Closing a post in Discord archives its session.

    async def push_archived(self):
        """Desktop to Discord: archived sessions lose their posts; unarchived ones get new posts."""
        archived = None if self.pins is None else self.pins.archived()
        if archived is None:
            return
        doomed = [link for link in self.store.links() if link["session"] in archived]
        # A few per pass: a bulk archive must not hold the loop while Discord rate-limits deletes.
        for link in doomed[:DELETES_PER_PASS]:
            await self._delete_post(link)
        back = next((session for session in self.store.retired() if session not in archived), None)
        if back is not None:
            await self._repost(back)

    async def _delete_post(self, link):
        session = link["session"]
        try:
            await self.discord.delete_post(link["thread"])
        except DiscordError as error:
            # Left closed instead; the next pass tries again.
            log.warning("Discord sync could not delete the post of session %s: %s", session, error)
            return
        self.store.retire(session)
        runtime = self.runtime.get(session)
        for live in (self.working, self.streams, self.activity):
            live.pop(runtime, None)
        log.info("Discord sync deleted the post of archived session %s", session)

    async def _repost(self, session):
        """One per pass, since it copies the whole history into the new post."""
        try:
            await self._link(session)
        except (RpcError, ValueError) as error:
            log.warning("Discord sync could not repost unarchived session %s: %s", session, error)
        self.store.restore(session)
        log.info("Discord sync reposted unarchived session %s", session)

    async def _follow_post(self, link, channel):
        """Mirror the post's open state onto the session. True while the user has it closed."""
        session = link["session"]
        archived = bool((channel.get("thread_metadata") or {}).get("archived"))
        closed = self.store.closed(session)
        if archived == closed:
            return closed
        if not archived:
            await self._set_archived(session, False)
            return self.store.closed(session)
        # Discord archives idle posts on its own; only a close by an allowed user hides the session.
        if await self.discord.closed_by(link["thread"]) not in self.allowed_users:
            return False
        await self._set_archived(session, True)
        return self.store.closed(session)

    async def _set_archived(self, session, archived):
        runtime = self.runtime.get(session, session)
        try:
            await self.rpc.call("session.archive", {"session_id": runtime, "archived": archived})
        except RpcError as error:
            # Left as it was; the next update or poll of this post tries again.
            verb = "archive" if archived else "unarchive"
            log.warning("Discord sync could not %s session %s: %s", verb, session, error)
            return
        self.store.set_closed(session, archived)
        log.info("Discord sync %s session %s", "archived" if archived else "unarchived", session)

    @contextlib.asynccontextmanager
    async def _writable(self, link):
        """Discord refuses edits to closed posts: open one for the write, then close it again."""
        closed = self.store.closed(link["session"])
        if closed:
            await self.discord.reopen(link["thread"])
        try:
            yield
        finally:
            if closed:
                await self.discord.close(link["thread"])

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
            await self.push_tags()
            await self.push_archived()
            await self.scan()
            await self._flush_streams()
            await self._keep_typing()
            await self._poll_next_thread()

    async def _flush_streams(self):
        if not self.stream_cadence.due():
            return
        for runtime in list({*self.streams, *self.activity}):
            link = self._by_runtime(runtime)
            body = self._live_body(runtime)
            # A closed post gets the finished reply, not a reopen and close every second.
            if link and body and not self.store.closed(link["session"]):
                await self.upsert(link, "stream", body)

    async def _keep_typing(self):
        if not self.typing_cadence.due():
            return
        for link in list(self.working.values()):
            if not self.store.closed(link["session"]):
                await self.discord.typing(link["thread"])

    async def _poll_next_thread(self):
        """Safety net for anything the push path missed: one thread per pass keeps a cycle cheap."""
        links = self.store.links()
        if not links or not self.poll_cadence.due():
            return
        link = links[self.poll_index % len(links)]
        self.poll_index += 1
        channel = await self.discord.channel(link["thread"])
        closed = await self._follow_post(link, channel)
        if not closed and _is_stale_archive(channel):
            # Archived for inactivity: keep the post open so the session can still be read and continued.
            await self.discord.reopen(link["thread"])
        for message in await self.discord.messages_after(link["thread"], link["cursor"]):
            await self.receive(link, message)
            self._advance_cursor(link, message)
            if self._by_session(link["session"]) is None:
                return
        await self.reconcile(link)

    async def beat(self):
        # Separate from the tick so a slow Discord pass never lets the gateway
        # reservation lapse and the normal bot answer a synced post twice.
        last = time.monotonic()
        while True:
            self.heartbeat()
            await asyncio.sleep(HEARTBEAT_EVERY)
            now = time.monotonic()
            if now - last > HEARTBEAT_EVERY + STALL_WARN_AFTER:
                # Something blocked the loop; past 20s the gateway stops handing work over.
                log.warning("Discord sync loop stalled %.1fs between heartbeats", now - last - HEARTBEAT_EVERY)
            last = now

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
        self.owner.clear()
        self.waiting.clear()
        self.activity.clear()
        self.steered.clear()
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

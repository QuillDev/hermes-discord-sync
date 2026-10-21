"""Stateful simulators for what the plugin does not own: Discord, the Hermes backend and its pins."""

import json
import time
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from urllib.parse import unquote

import aiohttp
import pytest_asyncio
from aiohttp import web

from dashboard.sync.bridge import Bridge
from dashboard.sync.discord import DISCORD_EPOCH_MS, Discord
from dashboard.sync.inbox import Inbox
from dashboard.sync.rpc import Rpc
from dashboard.sync.store import Store


class ExternalServices:
    def __init__(self):
        self.rows = []
        self.threads = {}
        self.sockets = []
        self.counter = 1000
        self.turns = []
        self.answers = {}
        self.seq = 0
        self.submits = 0
        self.drop_after_submit = False
        self.nonces = {}
        self.open_requests = []
        self.sessions = []
        self.forum_tags = []
        self.typing = []
        # The bot's reactions: (channel, message id) -> emoji, in the order they were added.
        self.reactions = {}
        self.busy_status = None
        # Turn isolation: the receipt carries no user row id, and the stored row gains host context.
        self.isolated = False
        self.questions = {}
        self.locked = {}
        # Requests the host already settled, e.g. answered from Desktop.
        self.settled = set()
        # Desktop's archived flag per stored session, as session.archive set it.
        self.archived = {}
        # Discord answering 403 to deleting a post, as when the bot lacks Manage Threads.
        self.refuse_deletes = False
        # Discord's audit log of thread updates, oldest first.
        self.audit = []

    def add_post(self, name, content, user="42", parent="forum"):
        # Discord forum posts: the starter message id equals the thread id.
        tid = str(((int(time.time() * 1000) - DISCORD_EPOCH_MS) << 22) + int(self.mid()))
        self.threads[tid] = {"name": name, "messages": [], "archived": False, "parent_id": parent}
        self.threads[tid]["messages"].append(
            {"id": tid, "channel_id": tid, "content": content, "author": {"id": user, "bot": False}}
        )
        return tid

    def close_post(self, thread, user="42", *, ago=0, logged=True):
        """Archive a post: a person closing it (logged), or Discord's own idle archive (not logged)."""
        self._set_archived(thread, True, user if logged else None, at=time.time() - ago)

    def reopen_post(self, thread, user="42"):
        self._set_archived(thread, False, user)

    def _set_archived(self, thread, archived, user, at=None):
        post = self.threads[thread]
        if post["archived"] == archived:
            return
        post["archived"] = archived
        post["archive_timestamp"] = at if at is not None else time.time()
        if user is not None:
            change = {"key": "archived", "old_value": not archived, "new_value": archived}
            self.audit.append({"id": self.mid(), "user_id": user, "target_id": thread, "changes": [change]})

    def mid(self):
        self.counter += 1
        return str(self.counter)

    def add_message(self, thread, content, user="42", **extra):
        message = {
            "id": self.mid(),
            "channel_id": thread,
            "content": content,
            "author": {"id": user, "bot": user == "bot"},
            **extra,
        }
        self.threads[thread]["messages"].append(message)
        return message

    async def event(self, kind, payload=None):
        self.seq += 1
        frame = {
            "jsonrpc": "2.0",
            "method": "event",
            "params": {
                "type": kind,
                "session_id": "live-one",
                "seq": self.seq,
                "payload": payload or {},
            },
        }
        for socket in self.sockets:
            if not socket.closed:
                await socket.send_json(frame)
        return frame

    async def ws(self, request):
        socket = web.WebSocketResponse()
        await socket.prepare(request)
        self.sockets.append(socket)
        async for message in socket:
            frame = json.loads(message.data)
            if "method" not in frame:
                continue
            method, params = frame["method"], frame.get("params", {})
            if method == "client.capabilities":
                result = {"server_requests": ["approval", "clarify"]}
            elif method == "session.list":
                result = {
                    "sessions": [{"id": "session-one", "source": "desktop", "title": "task"}, *self.sessions]
                }
            elif method == "session.create":
                existing = next(
                    (row for row in self.sessions if row["key"] == params.get("idempotency_key")), None
                )
                if existing is None:
                    existing = {
                        "id": f"stored-{len(self.sessions) + 2}",
                        "source": params["source"],
                        "title": params.get("title", ""),
                        "started_at": time.time(),
                        "key": params.get("idempotency_key"),
                    }
                    self.sessions.append(existing)
                result = {"session_id": "live-" + existing["id"], "stored_session_id": existing["id"]}
            elif method in {"session.resume", "session.activate"}:
                live = "live-one" if params["session_id"] == "session-one" else "live-" + params["session_id"]
                result = {
                    "session_id": live,
                    "stored_session_id": "session-one",
                    "messages": [],
                    "info": {},
                    "message_count": len(self.rows),
                }
            elif method == "session.events.since":
                result = {
                    "events": [],
                    "latest_seq": self.seq,
                    "open_requests": self.open_requests,
                    "truncated": False,
                    "epoch": "test",
                }
            elif method == "session.history":
                result = {"messages": self.rows, "count": len(self.rows)}
            elif method == "prompt.submit":
                assert params["session_id"] == "live-one" or params["session_id"].startswith("live-stored")
                self.submits += 1
                if self.busy_status is not None:
                    # A turn is running: the backend steers or queues instead of starting one.
                    self.turns.append(params["text"])
                    result = {"status": self.busy_status}
                    await socket.send_json({"jsonrpc": "2.0", "id": frame["id"], "result": result})
                    continue
                row_id = len(self.rows) + 1
                stored = params["text"] + ("\n\nVoice: lowercase." if self.isolated else "")
                self.rows.append({"row_id": row_id, "role": "user", "text": stored})
                self.turns.append(params["text"])
                result = {"status": "streaming"}
                if not self.isolated:
                    result["user_row_id"] = row_id
                if self.drop_after_submit:
                    await socket.close()
                    continue
                await self.event("message.start")
            elif method == "session.archive":
                live = params["session_id"]
                stored = "session-one" if live == "live-one" else live.removeprefix("live-")
                self.archived[stored] = params["archived"]
                result = {"archived": params["archived"], "session_key": stored}
            elif method == "session.interrupt":
                result = {"interrupted": True}
                await self.event("message.complete", {"status": "interrupted", "text": ""})
            elif method == "request.answer":
                expired = params["id"] in self.answers
                self.answers.setdefault(params["id"], params["result"])
                result = {"status": "expired" if expired else "ok"}
            elif method == "clarify.lock" and params["request_id"] in self.settled:
                result = {"status": "expired"}
            elif method == "clarify.lock":
                # A batch clarify resolves once every question has an answer.
                qids = self.questions.get(params["request_id"], [params["question_id"]])
                locked = self.locked.setdefault(params["request_id"], set())
                locked.add(params["question_id"])
                result = {"status": "ok", "remaining": len([q for q in qids if q not in locked])}
            else:
                await socket.send_json({"id": frame["id"], "error": {"code": -32601, "message": method}})
                continue
            await socket.send_json({"jsonrpc": "2.0", "id": frame["id"], "result": result})
        return socket

    def notified(self, thread):
        """Bot messages that notify someone, oldest first."""
        return [message for message in self.threads[thread]["messages"] if message.get("notifies")]

    def tag_names(self, thread):
        names = {tag["id"]: tag["name"] for tag in self.forum_tags}
        return sorted(names[tag] for tag in self.threads[thread].get("applied_tags", []))

    def reacted(self, channel, mid):
        return self.reactions.get((str(channel), str(mid)), [])

    async def discord(self, request):
        path = ["forum" if part == "555" else part for part in request.match_info["tail"].split("/")]
        method = request.method
        channel = path[1] if len(path) > 1 else ""
        if path == ["channels", "forum"]:
            if method == "PATCH":
                tags = (await request.json())["available_tags"]
                self.forum_tags = [{**tag, "id": tag.get("id") or self.mid()} for tag in tags]
            return web.json_response(
                {"id": "forum", "type": 15, "guild_id": "guild", "available_tags": self.forum_tags}
            )
        if path[0] == "guilds" and path[2] == "audit-logs":
            assert request.query["action_type"] == "111"
            return web.json_response({"audit_log_entries": list(reversed(self.audit))})
        if path[0] == "guilds":
            return web.json_response(
                {
                    "threads": [
                        {"id": tid, "name": t["name"], "parent_id": t.get("parent_id", "forum")}
                        for tid, t in self.threads.items()
                    ]
                }
            )
        if len(path) == 3 and path[2] == "threads" and method == "POST":
            payload = await request.json()
            tid = self.mid()
            self.threads[tid] = {"name": payload["name"], "messages": [], "archived": False}
            starter = self.add_message(tid, payload["message"]["content"], "bot")
            return web.json_response({"id": tid, "message": starter})
        if len(path) == 3 and path[2] == "typing":
            self.typing.append(channel)
            return web.Response(status=204)
        if len(path) == 7 and path[4] == "reactions":
            emoji = unquote(request.match_info["tail"].split("/")[5])
            mine = self.reactions.setdefault((channel, path[3]), [])
            if method == "PUT" and emoji not in mine:
                mine.append(emoji)
            if method == "DELETE" and emoji in mine:
                mine.remove(emoji)
            return web.Response(status=204)
        if len(path) == 2 and method == "DELETE":
            if self.refuse_deletes:
                return web.json_response({"code": 50013}, status=403)
            if self.threads.pop(channel, None) is None:
                return web.Response(status=404)
            return web.json_response({"id": channel})
        thread = self.threads.setdefault(
            channel, {"name": channel, "messages": [], "archived": False, "parent_id": "text"}
        )
        if len(path) == 2:
            if method == "PATCH":
                body = await request.json()
                if thread["archived"] and body.get("archived") is not False:
                    # Discord: an archived thread only accepts being unarchived.
                    return web.json_response({"code": 50083}, status=400)
                if "archived" in body:
                    self._set_archived(channel, body.pop("archived"), "bot")
                thread.update(body)
            stamp = thread.get("archive_timestamp", 0)
            return web.json_response(
                {
                    "id": channel,
                    "name": thread["name"],
                    "applied_tags": thread.get("applied_tags", []),
                    "thread_metadata": {
                        "archived": thread["archived"],
                        "archive_timestamp": datetime.fromtimestamp(stamp, UTC).isoformat(),
                    },
                }
            )
        if len(path) == 3 and method == "GET":
            before = int(request.query.get("before", "9999999999999999999"))
            messages = [message for message in thread["messages"] if int(message["id"]) < before]
            return web.json_response(sorted(messages, key=lambda m: int(m["id"]), reverse=True)[:100])
        if len(path) == 3 and method == "POST":
            body = await request.json()
            # Nothing mass-pings; only explicitly listed users are notified.
            mentions = body["allowed_mentions"]
            assert mentions["parse"] == []
            nonce = body["nonce"]
            if nonce in self.nonces:
                return web.json_response(self.nonces[nonce])
            # Discord reopens an archived thread when a message is sent in it.
            self._set_archived(channel, False, None)
            sent = self.add_message(channel, body["content"], "bot", components=body.get("components", []))
            sent["notifies"] = mentions.get("users", [])
            sent["reply_to"] = (body.get("message_reference") or {}).get("message_id")
            self.nonces[nonce] = sent
            return web.json_response(sent)
        message = next((message for message in thread["messages"] if message["id"] == path[3]), None)
        if message is None:
            return web.Response(status=404)
        if method == "GET":
            return web.json_response(message)
        if thread["archived"]:
            return web.json_response({"code": 50083}, status=400)
        if method == "PATCH":
            body = await request.json()
            message["content"] = body["content"]
            if "components" in body:
                message["components"] = body["components"]
            return web.json_response(message)
        if method == "DELETE":
            thread["messages"].remove(message)
            return web.Response(status=204)
        raise AssertionError(f"Unhandled external request {method} {path}")


class FakeClock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


class FakePins:
    """Desktop's pinned and archived sessions, as Hermes stores them."""

    def __init__(self, external):
        self.sessions = set()
        self.external = external

    def pinned(self):
        return set(self.sessions)

    def archived(self):
        return {session for session, archived in self.external.archived.items() if archived}

    def set(self, session, pinned):
        if pinned:
            self.sessions.add(session)
        else:
            self.sessions.discard(session)


@asynccontextmanager
async def serving(app):
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        await runner.cleanup()


@pytest_asyncio.fixture
async def world(tmp_path):
    external = ExternalServices()
    app = web.Application()
    app.router.add_get("/api/ws", external.ws)
    app.router.add_route("*", "/discord/{tail:.*}", external.discord)
    async with serving(app) as base, aiohttp.ClientSession() as http:
        rpc = Rpc(http, base + "/api/ws")
        await rpc.connect()
        store = Store(tmp_path / "sync" / "state.db")
        inbox = Inbox(tmp_path / "sync" / "inbox")
        pins = FakePins(external)
        clock = FakeClock()
        # Sessions the native gateway answers in a Discord thread: session id -> thread id.
        native_threads = {}
        discord = Discord(http, "test-token", base + "/discord")
        bridge = Bridge(
            rpc,
            discord,
            store,
            "forum",
            ["42"],
            inbox=inbox,
            pins=pins,
            threads=native_threads.get,
            clock=clock,
        )
        await bridge.discord.validate("forum")
        await bridge.link("session-one")
        try:
            yield SimpleNamespace(
                bridge=bridge,
                external=external,
                http=http,
                base=base,
                inbox=inbox,
                pins=pins,
                native_threads=native_threads,
                clock=clock,
                link=store.links()[0],
            )
        finally:
            await bridge.close()
            for socket in external.sockets:
                await socket.close()
            store.close()

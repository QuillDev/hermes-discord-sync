"""Stateful simulators for what the plugin does not own: Discord, the Hermes backend and its pins."""

import json
import time
from contextlib import asynccontextmanager
from types import SimpleNamespace

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

    def add_post(self, name, content, user="42", parent="forum"):
        # Discord forum posts: the starter message id equals the thread id.
        tid = str(((int(time.time() * 1000) - DISCORD_EPOCH_MS) << 22) + int(self.mid()))
        self.threads[tid] = {"name": name, "messages": [], "archived": False, "parent_id": parent}
        self.threads[tid]["messages"].append(
            {"id": tid, "channel_id": tid, "content": content, "author": {"id": user, "bot": False}}
        )
        return tid

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
                row_id = len(self.rows) + 1
                self.rows.append({"row_id": row_id, "role": "user", "text": params["text"]})
                self.turns.append(params["text"])
                result = {"status": "streaming", "user_row_id": row_id}
                if self.drop_after_submit:
                    await socket.close()
                    continue
                await self.event("message.start")
            elif method == "session.interrupt":
                result = {"interrupted": True}
                await self.event("message.complete", {"status": "interrupted", "text": ""})
            elif method == "request.answer":
                self.answers[params["id"]] = params["result"]
                result = {"status": "ok"}
            elif method == "clarify.lock":
                result = {"status": "ok", "remaining": 0}
            else:
                await socket.send_json({"id": frame["id"], "error": {"code": -32601, "message": method}})
                continue
            await socket.send_json({"jsonrpc": "2.0", "id": frame["id"], "result": result})
        return socket

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
        thread = self.threads.setdefault(
            channel, {"name": channel, "messages": [], "archived": False, "parent_id": "text"}
        )
        if len(path) == 2:
            if method == "PATCH":
                thread.update(await request.json())
            return web.json_response(
                {
                    "id": channel,
                    "name": thread["name"],
                    "applied_tags": thread.get("applied_tags", []),
                    "thread_metadata": {"archived": thread["archived"]},
                }
            )
        if len(path) == 3 and method == "GET":
            before = int(request.query.get("before", "9999999999999999999"))
            messages = [message for message in thread["messages"] if int(message["id"]) < before]
            return web.json_response(sorted(messages, key=lambda m: int(m["id"]), reverse=True)[:100])
        if len(path) == 3 and method == "POST":
            body = await request.json()
            assert body["allowed_mentions"] == {"parse": []}
            nonce = body["nonce"]
            if nonce in self.nonces:
                return web.json_response(self.nonces[nonce])
            sent = self.add_message(channel, body["content"], "bot")
            sent["reply_to"] = (body.get("message_reference") or {}).get("message_id")
            self.nonces[nonce] = sent
            return web.json_response(sent)
        message = next((message for message in thread["messages"] if message["id"] == path[3]), None)
        if message is None:
            return web.Response(status=404)
        if method == "GET":
            return web.json_response(message)
        if method == "PATCH":
            message["content"] = (await request.json())["content"]
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
    """Desktop's pinned sessions, as Hermes stores them."""

    def __init__(self):
        self.sessions = set()

    def pinned(self):
        return set(self.sessions)

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
        pins = FakePins()
        clock = FakeClock()
        discord = Discord(http, "test-token", base + "/discord")
        bridge = Bridge(rpc, discord, store, "forum", ["42"], inbox=inbox, pins=pins, clock=clock)
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
                clock=clock,
                link=store.links()[0],
            )
        finally:
            await bridge.close()
            for socket in external.sockets:
                await socket.close()
            store.close()

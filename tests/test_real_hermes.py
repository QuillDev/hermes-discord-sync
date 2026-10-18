"""Opt-in smoke test against the installed, unmodified Hermes runtime.

Only the model provider and Discord HTTP boundary are simulated. Sessions,
locks, history persistence, transport fanout, RPC and the plugin are real.
"""
import asyncio
import json
import os
import re
import shutil
import socket
import subprocess
from pathlib import Path

import aiohttp
import pytest
from aiohttp import web
from conftest import ExternalServices, serving

from dashboard.sync.bridge import Bridge
from dashboard.sync.discord import Discord
from dashboard.sync.rpc import Rpc
from dashboard.sync.store import Store


@pytest.mark.skipif(os.environ.get("REAL_HERMES") != "1", reason="requires the installed Hermes runtime")
async def test_installed_hermes_preserves_one_live_session_across_desktop_and_discord(tmp_path):
    async def llm(request):
        if request.path.endswith("/models"):
            return web.json_response({"object": "list", "data": [{"id": "fixture-model", "object": "model"}]})
        body = await request.json()
        text = "verified reply from the shared Hermes session"
        if body.get("stream"):
            response = web.StreamResponse(headers={"Content-Type": "text/event-stream"})
            await response.prepare(request)
            for delta in ("verified reply ", "from the shared Hermes session"):
                chunk = {"id": "chatcmpl-fixture", "object": "chat.completion.chunk", "created": 1,
                         "model": "fixture-model", "choices": [{"index": 0, "delta": {"content": delta},
                                                                  "finish_reason": None}]}
                await response.write(("data: " + json.dumps(chunk) + "\n\n").encode())
            await response.write(("data: " + json.dumps({"id": "chatcmpl-fixture", "object": "chat.completion.chunk",
                "created": 1, "model": "fixture-model", "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]})
                + "\n\ndata: [DONE]\n\n").encode())
            await response.write_eof()
            return response
        return web.json_response({"id": "chatcmpl-fixture", "object": "chat.completion", "created": 1,
                                  "model": "fixture-model", "choices": [{"index": 0, "message": {
                                      "role": "assistant", "content": text}, "finish_reason": "stop"}],
                                  "usage": {"prompt_tokens": 10, "completion_tokens": 10, "total_tokens": 20}})

    external = ExternalServices()
    app = web.Application()
    app.router.add_route("*", "/v1/{tail:.*}", llm)
    app.router.add_route("*", "/discord/{tail:.*}", external.discord)
    async with serving(app) as service, aiohttp.ClientSession() as http:
        home = tmp_path / "home"
        home.mkdir()
        (home / "config.yaml").write_text(f"""
model:
  provider: custom
  default: fixture-model
  base_url: {service}/v1
  api_key: fixture-only
agent:
  max_turns: 3
terminal:
  cwd: {tmp_path}
plugins:
  enabled: []
dashboard:
  auth:
    provider: none
""")
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        env = {key: value for key, value in os.environ.items() if key in {"PATH", "HOME", "USER", "LANG", "SHELL"}}
        env.update({"HERMES_GATEWAY_LOCK_DIR": str(tmp_path / "locks"), "XDG_STATE_HOME": str(tmp_path / "state"), "HERMES_HOME": str(home), "OPENAI_API_KEY": "fixture-only", "CUSTOM_API_KEY": "fixture-only",
                    "OPENAI_BASE_URL": service + "/v1", "HERMES_NO_AUTO_UPDATE": "1"})
        logpath = tmp_path / "hermes.log"
        with logpath.open("w") as logfile:
            process = subprocess.Popen([shutil.which("hermes"), "serve", "--host", "127.0.0.1", "--port", str(port),
                                        "--skip-build", "--isolated"], env=env, stdout=logfile, stderr=subprocess.STDOUT)
        rpc = None
        desktop = None
        store = None
        try:
            base = f"http://127.0.0.1:{port}"
            html = ""
            for _ in range(120):
                if process.poll() is not None:
                    pytest.fail(logpath.read_text()[-12000:])
                try:
                    async with http.get(base + "/", allow_redirects=False) as response:
                        html = await response.text()
                        if response.status == 200:
                            break
                except aiohttp.ClientError:
                    pass
                await asyncio.sleep(0.5)
            token = re.search(r'__HERMES_SESSION_TOKEN__\s*=\s*["\']([^"\']+)', html)
            assert token, logpath.read_text()[-12000:]
            url = base + "/api/ws?token=" + token.group(1)
            desktop = Rpc(http, url)
            await desktop.connect()
            created = await desktop.call("session.create", {"source": "desktop", "cwd": str(tmp_path),
                                                             "title": "sync verification", "close_on_disconnect": False})
            stored = created["stored_session_id"]
            desktop_id = created["session_id"]
            result = await desktop.call("prompt.submit", {"session_id": desktop_id, "text": "desktop input"})
            assert result.get("status") == "streaming", result
            for _ in range(180):
                frame = await asyncio.wait_for(desktop.events.get(), 90)
                if frame.get("params", {}).get("type") == "message.complete":
                    assert frame["params"]["payload"].get("status") == "complete", frame
                    break
            # Link an existing conversation mid-task, as the phone switch does.
            rpc = Rpc(http, url)
            await rpc.connect()
            store = Store(tmp_path / "sync" / "state.db")
            bridge = Bridge(rpc, Discord(http, "fixture-only", service + "/discord"), store, "forum", ["42"])
            await bridge.discord.validate("forum")
            linked = await bridge.link(stored)
            assert bridge.runtime[stored] == desktop_id
            assert "you: desktop input" in [m["content"] for m in external.threads[linked["thread"]]["messages"]]
            phone = external.add_message(linked["thread"], "phone input")
            await bridge.receive(linked, phone)
            for _ in range(180):
                frame = await asyncio.wait_for(desktop.events.get(), 90)
                if frame.get("params", {}).get("type") == "message.complete":
                    assert frame["params"]["payload"].get("status") == "complete", frame
                    break
            await bridge.reconcile(linked)
            history = await desktop.call("session.history", {"session_id": desktop_id})
            users = [row["text"] for row in history["messages"] if row["role"] == "user"]
            assert users == ["desktop input", "phone input"], history
            assistants = [row["text"] for row in history["messages"] if row["role"] == "assistant"]
            assert assistants == ["verified reply from the shared Hermes session"] * 2
            # Reattaching does not create another runtime session or repeat a turn.
            await bridge.attach(linked)
            assert bridge.runtime[stored] == desktop_id
            again = await desktop.call("session.history", {"session_id": desktop_id})
            assert again == history
            # A brand-new forum post becomes a real desktop session on the real backend.
            post = external.add_post("started on phone", "phone-started task")
            bridge.last_scan = 0
            await bridge.tick()
            started = next(link for link in store.links() if link["thread"] == post)
            runtime = bridge.runtime[started["session"]]
            rows = []
            for _ in range(120):
                rows = (await rpc.call("session.history", {"session_id": runtime}))["messages"]
                if any(row["role"] == "assistant" for row in rows):
                    break
                await asyncio.sleep(0.5)
            assert [row["text"] for row in rows if row["role"] == "user"] == ["phone-started task"], rows
            listed = await desktop.call("session.list", {"limit": 50})
            mine = next(row for row in listed["sessions"] if row["id"] == started["session"])
            assert mine["source"] == "desktop" and mine["title"] == "started on phone", mine
            await bridge.reconcile(started)
            assert "verified reply from the shared Hermes session" in [
                m["content"] for m in external.threads[post]["messages"]]
            print(f"real Hermes: phone post created desktop session {started['session']} and got its reply")
            print(f"real Hermes: same runtime session {desktop_id}; desktop + phone inputs persisted once; 2 shared replies")
        finally:
            if rpc is not None:
                await rpc.close()
            if desktop is not None:
                await desktop.close()
            if store is not None:
                store.close()
            process.terminate()
            try:
                await asyncio.to_thread(process.wait, 15)
            except subprocess.TimeoutExpired:
                process.kill()
                await asyncio.to_thread(process.wait)


@pytest.mark.skipif(os.environ.get("REAL_HERMES") != "1", reason="requires the installed Hermes runtime")
async def test_installed_plugin_starts_with_the_backend_and_turns_a_post_into_a_desktop_session(tmp_path):
    """No Desktop window, no toggle: the backend alone runs the sync."""
    async def llm(request):
        if request.path.endswith("/models"):
            return web.json_response({"object": "list", "data": [{"id": "fixture-model", "object": "model"}]})
        body = await request.json()
        if not body.get("stream"):
            return web.json_response({"id": "x", "object": "chat.completion", "created": 1, "model": "fixture-model",
                                      "choices": [{"index": 0, "message": {"role": "assistant", "content": "a title"},
                                                   "finish_reason": "stop"}],
                                      "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}})
        response = web.StreamResponse(headers={"Content-Type": "text/event-stream"})
        await response.prepare(request)
        for delta, finish in (({"content": "auto reply"}, None), ({}, "stop")):
            chunk = {"id": "x", "object": "chat.completion.chunk", "created": 1, "model": "fixture-model",
                     "choices": [{"index": 0, "delta": delta, "finish_reason": finish}]}
            await response.write(("data: " + json.dumps(chunk) + "\n\n").encode())
        await response.write(b"data: [DONE]\n\n")
        await response.write_eof()
        return response

    external = ExternalServices()
    app = web.Application()
    app.router.add_route("*", "/v1/{tail:.*}", llm)
    app.router.add_route("*", "/discord/{tail:.*}", external.discord)
    async with serving(app) as service:
        home = tmp_path / "home"
        (home / "plugins").mkdir(parents=True)
        source = Path(__file__).parents[1]
        shutil.copytree(source, home / "plugins" / "discord-sync", ignore=shutil.ignore_patterns(".git", "tests", "__pycache__"))
        (home / ".env").write_text("DISCORD_BOT_TOKEN=fixture-only\nDISCORD_ALLOWED_USERS=42\n")
        (home / "config.yaml").write_text(f"""
model:
  provider: custom
  default: fixture-model
  base_url: {service}/v1
  api_key: fixture-only
terminal:
  cwd: {tmp_path}
plugins:
  enabled: [discord-sync]
  entries:
    discord-sync:
      settings:
        forum_id: 555
        discord_api: {service}/discord
""")
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        env = {key: value for key, value in os.environ.items() if key in {"PATH", "HOME", "USER", "LANG", "SHELL"}}
        env.update({"HERMES_GATEWAY_LOCK_DIR": str(tmp_path / "locks"), "XDG_STATE_HOME": str(tmp_path / "state"), "HERMES_HOME": str(home), "HERMES_NO_AUTO_UPDATE": "1"})
        logpath = tmp_path / "hermes.log"
        with logpath.open("w") as logfile:
            process = subprocess.Popen([shutil.which("hermes"), "serve", "--host", "127.0.0.1", "--port", str(port),
                                        "--skip-build", "--isolated"], env=env, stdout=logfile, stderr=subprocess.STDOUT)
        try:
            beat = home / "discord-sync" / "heartbeat"
            for _ in range(120):
                if process.poll() is not None:
                    pytest.fail(logpath.read_text()[-8000:])
                if beat.exists():
                    break
                await asyncio.sleep(0.5)
            assert beat.exists(), (logpath.read_text()[-6000:] + (home / "logs" / "errors.log").read_text()[-6000:])
            post = external.add_post("phone first", "started from discord", parent="555")
            reply = None
            for _ in range(120):
                reply = next((m for m in external.threads[post]["messages"] if m["content"] == "auto reply"), None)
                if reply:
                    break
                await asyncio.sleep(0.5)
            assert reply, external.threads[post]
            import sqlite3
            with sqlite3.connect(home / "state.db") as db:
                rows = db.execute("SELECT id, source, title FROM sessions").fetchall()
            assert len(rows) == 1 and rows[0][1] == "desktop", rows
            # Tagging the post in Discord pins the desktop session through the real SessionDB.
            for _ in range(60):
                tag = next((t["id"] for t in getattr(external, "forum_tags", []) if t["name"] == "pinned"), None)
                if tag:
                    break
                await asyncio.sleep(0.5)
            (home / "discord-sync" / "tag-events").write_text(json.dumps({"thread": post, "tags": [tag]}) + "\n")
            for _ in range(40):
                with sqlite3.connect(home / "state.db") as db:
                    pinned = db.execute("SELECT pinned FROM sessions WHERE id=?", (rows[0][0],)).fetchone()[0]
                if pinned:
                    break
                await asyncio.sleep(0.25)
            assert pinned == 1
            print(f"real Hermes: backend auto-started sync; post became desktop session {rows[0][0]} with reply; tag pinned it")
        finally:
            process.terminate()
            try:
                await asyncio.to_thread(process.wait, 15)
            except subprocess.TimeoutExpired:
                process.kill()

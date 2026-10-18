"""Backend half: runs the bridge inside the Hermes backend. No second public server."""

from __future__ import annotations

import asyncio
import importlib.util
import logging
import sqlite3
import sys
import threading
import time
from contextlib import closing
from pathlib import Path
from urllib.parse import urlencode

import aiohttp
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, ConfigDict

# The dashboard loader imports this file as a standalone module. Give our own
# vendored package a local namespace; never patch/import agent-server privates.
_package = __name__ + "_sync"
_spec = importlib.util.spec_from_file_location(
    _package,
    Path(__file__).parent / "sync" / "__init__.py",
    submodule_search_locations=[str(Path(__file__).parent / "sync")],
)
_module = importlib.util.module_from_spec(_spec)
sys.modules[_package] = _module
_spec.loader.exec_module(_module)
Bridge = importlib.import_module(_package + ".bridge").Bridge
API = importlib.import_module(_package + ".discord").API
Discord = importlib.import_module(_package + ".discord").Discord
Inbox = importlib.import_module(_package + ".inbox").Inbox
Rpc = importlib.import_module(_package + ".rpc").Rpc
Store = importlib.import_module(_package + ".store").Store

log = logging.getLogger("hermes.discord-sync")
PLUGIN = "discord-sync"
SETTINGS_RETRY_SECONDS = 30


def settings():
    from hermes_cli.config import load_config
    from hermes_constants import get_hermes_home, named_profile_home

    config = load_config()
    plugins = config.get("plugins", {})
    if PLUGIN not in plugins.get("enabled", []):
        raise HTTPException(403, "Enable the discord-sync agent plugin first")
    home = get_hermes_home().resolve()
    # This installation deliberately owns default only. Never clone it into or
    # publish another profile merely because the Desktop focus changed.
    if named_profile_home(home) is not None:
        raise HTTPException(403, "Discord sync is configured for the default profile only")
    entry = plugins.get("entries", {}).get(PLUGIN, {}).get("settings", {})
    forum = str(entry.get("forum_id", ""))
    if not forum.isdecimal():
        raise HTTPException(409, "Set plugins.entries.discord-sync.settings.forum_id")
    return home, entry, forum


def _credentials(home, entry):
    from agent.secret_scope import build_profile_secret_scope, get_secret, set_secret_scope

    # This thread outlives any request, so bind this profile's secrets
    # explicitly; an unscoped read fails closed on a multi-profile backend.
    set_secret_scope(build_profile_secret_scope(home), profile_home=str(home))
    token = get_secret("DISCORD_BOT_TOKEN")
    configured = entry.get("allowed_users") or str(get_secret("DISCORD_ALLOWED_USERS") or "").split(",")
    users = [str(user).strip() for user in configured if str(user).strip()]
    all_numeric = all(user.isdecimal() for user in users)
    if not token or not users or not all_numeric:
        raise ValueError("Configure the Discord bot and an explicit numeric user allowlist")
    return token, users


def _identity(request):
    session = getattr(request.state, "session", None)
    if session is not None:
        return session.user_id, session.provider
    if getattr(request.app.state, "auth_required", False):
        raise HTTPException(401, "Sign into Hermes first")
    # The normal loopback token gate must still be satisfied. No alternate
    # unauthenticated pairing route is introduced by this plugin.
    from hermes_cli.web_server import _has_valid_session_token

    if not _has_valid_session_token(request):
        raise HTTPException(401, "Unauthorized")
    return "local", "loopback"


def _connection_url(app, port):
    # This client is spawned by the server itself, so it uses the server-internal
    # credential meant for exactly that. It never leaves this process.
    from hermes_cli import web_server
    from hermes_cli.dashboard_auth.ws_tickets import internal_ws_credential

    def url():
        if getattr(app.state, "auth_required", False):
            query = {"internal": internal_ws_credential()}
        else:
            query = {"token": web_server._SESSION_TOKEN}
        return f"http://127.0.0.1:{port}/api/ws?{urlencode(query)}"

    return url


def _notify(session):
    from hermes_cli.plugin_events import broadcast_plugin_event

    broadcast_plugin_event(PLUGIN, "transcript", {"session_id": session, "profile": "default"})


class HermesPins:
    """Desktop pins, read from and written to the profile's own session database."""

    def __init__(self, home):
        self.path = home / "state.db"

    def pinned(self):
        with closing(sqlite3.connect(f"file:{self.path}?mode=ro", uri=True)) as db:
            return {row[0] for row in db.execute("SELECT id FROM sessions WHERE pinned=1")}

    def set(self, session, pinned):
        # The same SessionDB setter the sessions PATCH route uses; the backend's
        # change watcher then tells Desktop to refetch, and Desktop adopts the pin.
        from hermes_state_registry import acquire, release_or_close

        db = acquire(self.path)
        try:
            db.set_session_pinned(session, pinned)
        finally:
            release_or_close(db)


class _Service:
    """Runs the bridge on its own loop thread for the life of the backend."""

    def __init__(self):
        self.loop = None
        self.bridge = None
        self.thread = None
        self.error = "starting"
        self.lock = threading.Lock()

    def ensure(self):
        with self.lock:
            running = self.thread is not None and self.thread.is_alive()
            if running:
                return
            self.thread = threading.Thread(target=self._run, name="hermes-discord-sync", daemon=True)
            self.thread.start()

    def _run(self):
        from hermes_cli.web_server import app

        # Plugin routes mount before the socket binds; wait for the real port.
        while getattr(app.state, "bound_port", None) is None:
            time.sleep(0.5)
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        try:
            self.loop.run_until_complete(self._main(app))
        except Exception as error:
            # Surface it in /status and the log instead of dying silently.
            self.error = f"Discord sync stopped: {type(error).__name__}: {error}"
            log.exception("Discord sync stopped")

    async def _configured(self):
        while True:
            try:
                home, entry, forum = settings()
                token, users = _credentials(home, entry)
            except (HTTPException, ValueError) as error:
                self.error = getattr(error, "detail", None) or str(error)
                log.warning("Discord sync waiting: %s", self.error)
                await asyncio.sleep(SETTINGS_RETRY_SECONDS)
                continue
            self.error = None
            return home, entry, forum, token, users

    async def _main(self, app):
        home, entry, forum, token, users = await self._configured()
        sync_dir = home / "discord-sync"
        heartbeat = sync_dir / "heartbeat"
        async with aiohttp.ClientSession() as http:
            self.bridge = Bridge(
                Rpc(http, _connection_url(app, app.state.bound_port)),
                Discord(http, token, str(entry.get("discord_api", API))),
                Store(sync_dir / "state.db"),
                forum,
                users,
                inbox=Inbox(sync_dir / "inbox"),
                pins=HermesPins(home),
                notify=_notify,
                heartbeat=heartbeat.touch,
                auto_link=bool(entry.get("auto_link", True)),
            )
            log.info("Discord sync started for forum %s", forum)
            await self.bridge.serve()

    async def call(self, coroutine_factory):
        if self.loop is None or self.bridge is None:
            raise HTTPException(503, self.error or "Discord sync is starting")
        future = asyncio.run_coroutine_threadsafe(coroutine_factory(self.bridge), self.loop)
        return await asyncio.wrap_future(future)


_service = _Service()
router = APIRouter()


@router.post("/start")
async def start(request: Request):
    _identity(request)
    _, entry, _ = settings()
    _service.ensure()
    return {"started": True, "auto_link": bool(entry.get("auto_link", True))}


@router.get("/status")
async def status(request: Request):
    _identity(request)
    if _service.bridge is None:
        return {"connected": False, "links": [], "error": _service.error}

    async def read(bridge):
        return {"connected": bridge.connected, "links": bridge.store.links(), "error": bridge.last_error}

    return await _service.call(read)


class LinkBody(BaseModel):
    model_config = ConfigDict(extra="forbid")
    session_id: str


@router.post("/link")
async def link(body: LinkBody, request: Request):
    _identity(request)
    _service.ensure()

    async def wait_then_link(bridge):
        for _ in range(100):
            if bridge.connected:
                break
            await asyncio.sleep(0.1)
        if not bridge.connected:
            raise HTTPException(503, "Discord sync is not connected yet")
        try:
            linked = await bridge.link(body.session_id)
        except ValueError as error:
            raise HTTPException(400, str(error)) from error
        return {**linked, "url": f"https://discord.com/channels/{bridge.discord.guild}/{linked['thread']}"}

    return await _service.call(wait_then_link)


# Start with the backend itself: no Desktop window or toggle is required.
_service.ensure()

"""Use the same public WebSocket RPC as Desktop; never open a second agent writer."""

from __future__ import annotations

import asyncio
import contextlib
import json
import uuid

import aiohttp

# Desktop's keepalive (apps/shared json-rpc-channel): an app-level `gateway.ping`
# every 15 seconds, and the link counts as dead only after 45 seconds of silence.
# The backend answers it from its read loop, so an agent turn holding the GIL for
# a few seconds never looks like a dead socket. A WebSocket-level heartbeat drops
# the link after a 7.5 second stall, and every reconnect re-attaches every post.
PING_EVERY = 15
SILENCE_LIMIT = 45


class RpcError(RuntimeError):
    def __init__(self, error):
        self.code = error.get("code")
        super().__init__(error.get("message", "RPC failed"))


class Rpc:
    def __init__(self, http, url, *, ping_every=PING_EVERY, silence_limit=SILENCE_LIMIT):
        self.http = http
        self.url = url
        self.ping_every = ping_every
        self.silence_limit = silence_limit
        self.pending = {}
        self.events = asyncio.Queue(maxsize=10000)
        self.ws = None
        self.reader = None
        self.keepalive = None
        self.heard_at = 0.0

    async def connect(self):
        # A fresh queue per link: closing the previous link queued its own
        # disconnect, and reading that here would drop this link at once.
        self.events = asyncio.Queue(maxsize=10000)
        self.ws = await self.http.ws_connect(
            self.url() if callable(self.url) else self.url, max_msg_size=64 * 1024 * 1024
        )
        self.heard_at = asyncio.get_running_loop().time()
        self.reader = asyncio.create_task(self._read())
        self.keepalive = asyncio.create_task(self._keep_alive(self.ws))
        await self.call("client.capabilities", {"server_requests": True})

    async def _keep_alive(self, ws):
        loop = asyncio.get_running_loop()
        while not ws.closed:
            await asyncio.sleep(self.ping_every)
            if loop.time() - self.heard_at > self.silence_limit:
                # Closing ends the reader, which reports the disconnect.
                await ws.close()
                return
            ping = {"jsonrpc": "2.0", "id": f"ping-{uuid.uuid4().hex}", "method": "gateway.ping"}
            with contextlib.suppress(ConnectionError, RuntimeError):
                await ws.send_json(ping)

    async def _read(self):
        try:
            async for message in self.ws:
                if message.type != aiohttp.WSMsgType.TEXT:
                    continue
                self.heard_at = asyncio.get_running_loop().time()
                for line in message.data.splitlines():
                    frame = json.loads(line)
                    if "method" in frame:
                        await self.events.put(frame)
                        continue
                    future = self.pending.get(frame.get("id"))
                    if future is None or future.done():
                        continue
                    if "error" in frame:
                        future.set_exception(RpcError(frame["error"]))
                    else:
                        future.set_result(frame.get("result", {}))
        finally:
            for future in list(self.pending.values()):
                if not future.done():
                    future.set_exception(ConnectionError("Desktop connection closed"))
            await self.events.put({"method": "bridge.disconnected"})

    async def call(self, method, params=None):
        if self.ws is None or self.ws.closed:
            raise ConnectionError("Desktop backend is offline")
        rid = uuid.uuid4().hex
        future = asyncio.get_running_loop().create_future()
        self.pending[rid] = future
        try:
            await self.ws.send_json({"jsonrpc": "2.0", "id": rid, "method": method, "params": params or {}})
            return await asyncio.wait_for(future, timeout=90)
        finally:
            self.pending.pop(rid, None)

    async def close(self):
        if self.ws is not None:
            await self.ws.close()
        for task in (self.keepalive, self.reader):
            if task is None:
                continue
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

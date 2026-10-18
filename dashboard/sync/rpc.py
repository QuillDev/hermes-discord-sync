"""Use the same public WebSocket RPC as Desktop; never open a second agent writer."""
from __future__ import annotations

import asyncio
import contextlib
import json
import uuid

import aiohttp


class RpcError(RuntimeError):
    def __init__(self, error):
        self.code = error.get("code")
        super().__init__(error.get("message", "RPC failed"))


class Rpc:
    def __init__(self, http, url):
        self.http = http
        self.url = url
        self.pending = {}
        self.events = asyncio.Queue(maxsize=10000)
        self.ws = None
        self.reader = None

    async def connect(self):
        self.ws = await self.http.ws_connect(self.url() if callable(self.url) else self.url,
                                              max_msg_size=64 * 1024 * 1024, heartbeat=15)
        self.reader = asyncio.create_task(self._read())
        await self.call("client.capabilities", {"server_requests": True})

    async def _read(self):
        try:
            async for message in self.ws:
                if message.type != aiohttp.WSMsgType.TEXT:
                    continue
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

    async def answer(self, rid, result):
        if self.ws is None or self.ws.closed:
            raise ConnectionError("Desktop backend is offline")
        await self.ws.send_json({"jsonrpc": "2.0", "id": rid, "result": result})

    async def close(self):
        if self.ws is not None:
            await self.ws.close()
        if self.reader is not None:
            self.reader.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.reader

import asyncio
import json

import aiohttp
from aiohttp import web
from conftest import serving

from dashboard.sync.rpc import Rpc


def backend(*, answers_pings):
    """A Desktop backend that never answers WebSocket-level pings, as when a turn holds the GIL."""

    async def ws(request):
        socket = web.WebSocketResponse(autoping=False)
        await socket.prepare(request)
        async for message in socket:
            if message.type != aiohttp.WSMsgType.TEXT:
                continue
            frame = json.loads(message.data)
            if frame["method"] == "gateway.ping" and not answers_pings:
                continue
            await socket.send_json({"jsonrpc": "2.0", "id": frame["id"], "result": {"ok": True}})
        return socket

    app = web.Application()
    app.router.add_get("/api/ws", ws)
    return app


async def test_a_backend_answering_app_pings_stays_connected_through_a_stall():
    async with serving(backend(answers_pings=True)) as base, aiohttp.ClientSession() as http:
        rpc = Rpc(http, base + "/api/ws", ping_every=0.05, silence_limit=0.3)
        await rpc.connect()
        try:
            await asyncio.sleep(1)
            assert not rpc.ws.closed
            assert await rpc.call("session.list") == {"ok": True}
        finally:
            await rpc.close()


async def test_a_silent_backend_is_reported_disconnected():
    async with serving(backend(answers_pings=False)) as base, aiohttp.ClientSession() as http:
        rpc = Rpc(http, base + "/api/ws", ping_every=0.05, silence_limit=0.3)
        await rpc.connect()
        try:
            frame = await asyncio.wait_for(rpc.events.get(), 2)
            assert frame == {"method": "bridge.disconnected"}
        finally:
            await rpc.close()

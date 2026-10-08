"""过载先于业务调用拒绝，流消费和取消期间保持正确容量。"""

import asyncio
from dataclasses import replace

import httpx
from fastapi import FastAPI
from fastapi.responses import StreamingResponse

from model_gateway.admission import AdmissionMiddleware
from model_gateway.config import Settings


async def test_overload_does_not_enter_business_and_cancel_returns_slot():
    entered, finish = asyncio.Event(), asyncio.Event()
    calls = 0
    app = FastAPI()
    app.add_middleware(
        AdmissionMiddleware, settings=replace(Settings("", "", "", {}), max_inflight_requests=1)
    )

    @app.get("/v1/work")
    async def work():
        nonlocal calls
        calls += 1

        async def stream():
            yield b"first"
            entered.set()
            await finish.wait()
            yield b"last"

        return StreamingResponse(stream())

    @app.get("/live")
    async def live():
        return {"alive": True}

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://test"
    ) as client:
        first = asyncio.create_task(client.get("/v1/work"))
        await asyncio.wait_for(entered.wait(), 1)
        rejected = await client.get("/v1/work")
        assert rejected.status_code == 503
        assert rejected.json()["error"]["code"] == "gateway_busy"
        assert rejected.headers["retry-after"] == "1" and calls == 1
        assert (await client.get("/live")).status_code == 200
        first.cancel()
        await asyncio.gather(first, return_exceptions=True)
        finish.set()
        assert (await client.get("/v1/work")).content == b"firstlast"
        assert calls == 2

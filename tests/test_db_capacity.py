"""同步SQL被取消后仍占容量，不能错误放行无限后台操作。"""

import asyncio
import threading
from dataclasses import replace

import pytest
from fastapi import HTTPException

from model_gateway.app import Runtime
from model_gateway.config import Settings


async def test_cancelled_db_call_keeps_slot_until_real_execution_finishes(tmp_path):
    settings = replace(
        Settings(f"sqlite:///{tmp_path / 'capacity.db'}", "redis://localhost:1", "t", {}),
        db_workers=1,
        db_queue_size=1,
        db_queue_seconds=0.03,
    )
    runtime = Runtime(settings)
    entered, release = threading.Event(), threading.Event()

    def blocking():
        entered.set()
        release.wait(timeout=2)
        return "finished"

    first = asyncio.create_task(runtime.db_call(blocking))
    second = None
    try:
        while not entered.is_set():
            await asyncio.sleep(0.001)
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        second = asyncio.create_task(runtime.db_call(lambda: "queued"))
        await asyncio.sleep(0.01)
        with pytest.raises(HTTPException) as exc:
            await runtime.db_call(lambda: "must not run")
        assert exc.value.status_code == 503 and exc.value.detail == "database_busy"
        assert len(runtime.db_pending) == 2
        release.set()
        assert await second == "queued"
        await asyncio.sleep(0)
        assert not runtime.db_pending
    finally:
        release.set()
        if second:
            await asyncio.gather(second, return_exceptions=True)
        await runtime.close()

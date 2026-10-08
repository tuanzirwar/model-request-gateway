"""从已合并贡献迁移的资源所有权、清理故障与真实HTTP流回归。"""

import asyncio
from contextlib import AsyncExitStack
from types import SimpleNamespace

import httpx
import pytest
from redis.exceptions import RedisError
from sqlalchemy.exc import SQLAlchemyError

from model_gateway.app import Execution
from model_gateway.config import Model, Settings
from model_gateway.observability import Metrics
from model_gateway.sse import frames, parse


def execution_with_probes(*, close_error=False, record_error=False, release_error=False):
    events = []

    class Quota:
        async def release(self, app_id, model, request_id):
            events.append("release")
            if release_error:
                raise RedisError("controlled release failure")

    class Response:
        async def aclose(self):
            events.append("close")
            if close_error:
                raise RuntimeError("controlled close failure")

    async def db_call(function, *args, **kwargs):
        events.append(("record", kwargs["status"]))
        if record_error:
            raise SQLAlchemyError("controlled commit failure")

    runtime = SimpleNamespace(
        settings=Settings("", "", "", {}),
        quota=Quota(),
        metrics=Metrics(),
        database=SimpleNamespace(update_record=lambda: None),
        db_call=db_call,
        cleanups=set(),
    )
    model = Model("coding", "fixture", "http://127.0.0.1", "")
    execution = Execution(runtime, {"id": "a"}, model)
    execution.response = Response()
    execution.leased = True
    runtime.metrics.active.inc()
    return execution, events


@pytest.mark.parametrize("status", ["succeeded", "failed", "cancelled"])
async def test_cleanup_closes_and_releases_before_recording_exactly_once(status):
    execution, events = execution_with_probes()
    await asyncio.gather(execution.cleanup(status), execution.cleanup(status))
    assert events == ["close", "release", ("record", status)]
    assert "gateway_active_requests 0.0" in execution.runtime.metrics.render().decode()


@pytest.mark.parametrize("close_error,release_error", [(True, False), (False, True)])
async def test_one_cleanup_failure_does_not_skip_later_cleanup(close_error, release_error):
    execution, events = execution_with_probes(close_error=close_error, release_error=release_error)
    await execution.cleanup("failed", "upstream_incomplete")
    assert events == ["close", "release", ("record", "failed")]


@pytest.mark.parametrize("status", ["failed", "cancelled"])
async def test_failed_recording_cannot_prevent_quota_release(status):
    execution, events = execution_with_probes(record_error=True)
    await execution.cleanup(status)
    assert events == ["close", "release", ("record", status)]


async def test_repeated_caller_cancellation_cannot_interrupt_owned_cleanup():
    execution, events = execution_with_probes()
    entered, finish_close = asyncio.Event(), asyncio.Event()

    class Response:
        async def aclose(self):
            entered.set()
            await finish_close.wait()
            events.append("close")

    execution.response = Response()
    caller = asyncio.create_task(execution.cleanup("cancelled"))
    try:
        await asyncio.wait_for(entered.wait(), 1)
        owned = list(execution.runtime.cleanups)
        assert len(owned) == 1
        caller.cancel()
        caller.cancel()
        with pytest.raises(asyncio.CancelledError):
            await caller
        assert not owned[0].done()
        finish_close.set()
        await asyncio.wait_for(asyncio.gather(*owned), 1)
        assert events == ["close", "release", ("record", "cancelled")]
    finally:
        finish_close.set()
        await asyncio.gather(caller, *execution.runtime.cleanups, return_exceptions=True)


async def test_real_http_streams_emit_before_remainder_and_have_independent_consumption():
    """上游未放行剩余内容时两个HTTP流都已产出首帧，不用延时猜测先后顺序。"""
    release = asyncio.Event()
    handlers = set()
    writers = set()
    sent_remainders = []
    connections = []

    async def handle(reader, writer):
        task = asyncio.current_task()
        handlers.add(task)
        writers.add(writer)
        number = len(connections) + 1
        connections.append(number)
        try:
            await reader.readuntil(b"\r\n\r\n")
            writer.write(
                b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n"
                b"Connection: close\r\n\r\n"
                + f'data: {{"choices":[],"request":{number}}}\n\n'.encode()
            )
            await writer.drain()
            await release.wait()
            sent_remainders.append(number)
            writer.write(b"data: [DONE]\n\n")
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()
            handlers.discard(task)
            writers.discard(writer)

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    try:
        async with server, httpx.AsyncClient(trust_env=False, timeout=2) as client:
            async with AsyncExitStack() as stack:
                responses = [
                    await stack.enter_async_context(
                        client.stream("GET", f"http://127.0.0.1:{port}")
                    )
                    for _ in range(2)
                ]
                iterators = [frames(response, 1024) for response in responses]
                for iterator in iterators:
                    stack.push_async_callback(iterator.aclose)
                first = await asyncio.wait_for(
                    asyncio.gather(*(anext(iterator) for iterator in iterators)), 1
                )
                assert [parse(value)["request"] for value in first] == [1, 2]
                assert sent_remainders == []
                release.set()
                endings = await asyncio.wait_for(
                    asyncio.gather(*(anext(iterator) for iterator in iterators)), 1
                )
                assert endings == ["[DONE]", "[DONE]"]
            assert all(response.is_closed for response in responses)
    finally:
        release.set()
        server.close()
        await server.wait_closed()
        for writer in list(writers):
            writer.close()
        if handlers:
            await asyncio.wait_for(asyncio.gather(*handlers), 2)

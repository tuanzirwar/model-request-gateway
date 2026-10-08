import asyncio
import time
from dataclasses import replace

import httpx
import pytest
from sqlalchemy.orm import Session

from model_gateway.app import Execution, usage_fields
from model_gateway.config import Settings
from model_gateway.db import Application, Base, Database, RequestRecord, key_hash
from model_gateway.sse import StreamError, frames, parse


class Chunks(httpx.AsyncByteStream):
    def __init__(self, chunks):
        self.chunks = chunks

    async def __aiter__(self):
        for chunk in self.chunks:
            yield chunk


@pytest.mark.parametrize("ending", [b"\n\n", b"\r\n\r\n"])
async def test_sse_fragmented_unicode_and_done(ending):
    raw = 'data: {"choices":[],"text":"你好"}'.encode() + ending
    response = httpx.Response(
        200, stream=Chunks([raw[:17], raw[17:32], raw[32:], b"data: [DONE]" + ending])
    )
    iterator = frames(response, 1000)
    assert parse(await anext(iterator))["text"] == "你好"
    assert parse(await anext(iterator)) is None
    await iterator.aclose()


async def test_first_small_frame_is_not_buffered_until_eof():
    gate = asyncio.Event()

    class Stream(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b'data: {"choices":[]}\n\n'
            await gate.wait()

    iterator = frames(httpx.Response(200, stream=Stream()), 100)
    assert await asyncio.wait_for(anext(iterator), 0.2) == '{"choices":[]}'
    await iterator.aclose()


async def test_oversized_frame_rejected_before_terminator():
    iterator = frames(httpx.Response(200, stream=Chunks([b"data: " + b"x" * 100])), 50)
    with pytest.raises(StreamError, match="frame_too_large"):
        await anext(iterator)


async def test_missing_done_is_failure():
    iterator = frames(httpx.Response(200, stream=Chunks([b'data: {"choices":[]}\n\n'])), 100)
    await anext(iterator)
    with pytest.raises(StreamError, match="upstream_incomplete"):
        await anext(iterator)


@pytest.mark.parametrize("data", ['{"error":{"message":"secret"}}', "[]", "broken"])
def test_protocol_errors_do_not_echo_upstream(data):
    with pytest.raises(StreamError) as error:
        parse(data)
    assert "secret" not in str(error.value)


def test_usage_only_numeric_allowlist():
    assert usage_fields(
        {"prompt_tokens": 2, "completion_tokens": -1, "total_tokens": True, "secret": "payload"}
    ) == {"prompt_tokens": 2}
    assert usage_fields({"total_tokens": 2**63}) == {}


def test_database_ownership_pagination_and_terminal_fencing(tmp_path):
    db = Database(f"sqlite:///{tmp_path / 'test.db'}")
    Base.metadata.create_all(db.engine)
    with Session(db.engine) as session, session.begin():
        session.add_all(
            [
                Application(
                    id=name, key_hash=key_hash(name), model_allowlist=["coding"], concurrency=1
                )
                for name in ("a", "b")
            ]
        )
    assert db.authenticate("a")["id"] == "a"
    assert db.authenticate("wrong") is None
    db.create_record("1", "a", "coding", 100)
    db.create_record("2", "a", "coding", 100)
    assert db.get_record("b", "1") is None
    first = db.list_records("a", limit=1)
    assert first["items"][0]["id"] == "2"
    assert db.list_records("a", first["next_cursor"], 1)["items"][0]["id"] == "1"
    assert db.list_records("b", "1", 1) is None
    db.update_record("1", status="succeeded", finished_at=time.time())
    assert db.update_record("1", status="failed") == 0
    with Session(db.engine) as session, session.begin():
        row = session.get(RequestRecord, "2")
        row.deadline_at = time.time() - 10
    assert db.reconcile() == 1
    assert db.get_record("a", "2")["status"] == "abandoned"
    db.engine.dispose()


async def test_lease_loss_cancels_pending_upstream_task():
    class Runtime:
        settings = replace(Settings("", "", "", {}), total_seconds=1)

    execution = Execution(Runtime(), {"id": "a"}, None)
    cancelled = asyncio.Event()

    async def pending():
        try:
            await asyncio.sleep(10)
        finally:
            cancelled.set()

    async def lose():
        await asyncio.sleep(0.01)
        execution.lost.set()

    task = asyncio.create_task(lose())
    with pytest.raises(StreamError, match="lease_lost"):
        await execution.wait(pending(), 1)
    await task
    assert cancelled.is_set()


def test_stream_output_budget_counts_utf8_and_protocol_bytes():
    class Runtime:
        settings = replace(Settings("", "", "", {}), max_stream_bytes=20)

    execution = Execution(Runtime(), {"id": "a"}, None)
    assert execution.account_stream_frame("中") == "data: 中\n\n".encode()
    assert execution.bytes_out == 11
    with pytest.raises(StreamError, match="response_too_large"):
        execution.account_stream_frame("中")
    assert execution.bytes_out == 11

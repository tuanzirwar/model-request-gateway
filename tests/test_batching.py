"""验证队列确认、背压、取消和批量SQL的终态保护。"""

import asyncio
import time

import pytest
from fastapi import HTTPException
from sqlalchemy import update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from model_gateway.batching import BatchQueue
from model_gateway.db import Application, Base, Database, key_hash


async def test_queue_ack_waits_for_consumer_and_cancel_keeps_capacity():
    entered, release = asyncio.Event(), asyncio.Event()
    messages_seen = []

    async def consume(messages):
        messages_seen.extend(messages)
        entered.set()
        await release.wait()
        return messages

    queue = BatchQueue(consume, size=16, capacity=1, timeout=0.01)
    caller = asyncio.create_task(queue.submit("first"))
    await entered.wait()
    assert not caller.done()
    caller.cancel()
    with pytest.raises(asyncio.CancelledError):
        await caller
    with pytest.raises(HTTPException, match="queue_busy"):
        await queue.submit("must not run")
    release.set()
    await queue.close()
    assert messages_seen == ["first"]
    assert queue.slots._value == 1
    with pytest.raises(HTTPException, match="queue_closed"):
        await queue.submit("closed")


async def test_queue_batches_errors_and_drains_on_close():
    seen = []

    async def consume(messages):
        seen.append(messages)
        raise ValueError("rolled back")

    queue = BatchQueue(consume, size=4, delay=0.01)
    callers = [asyncio.create_task(queue.submit(index)) for index in range(4)]
    results = await asyncio.gather(*callers, return_exceptions=True)
    await queue.close()
    assert seen == [[0, 1, 2, 3]]
    assert all(isinstance(item, ValueError) for item in results)
    assert queue.slots._value == 256


@pytest.fixture
def database(tmp_path):
    db = Database(f"sqlite:///{tmp_path / 'batch.db'}")
    Base.metadata.create_all(db.engine)
    with Session(db.engine) as session, session.begin():
        session.add(Application(id="a", key_hash=key_hash("key"), model_allowlist=["coding"]))
    yield db
    db.engine.dispose()


def test_bulk_auth_has_no_stale_cache_and_preserves_input_order(database):
    messages = [((key,), {}) for key in ("key", "bad", "key")]
    result = database.batch_authenticate(messages)
    assert [value["id"] if value else None for value in result] == ["a", None, "a"]
    with database.engine.begin() as connection:
        connection.execute(update(Application).values(enabled=False))
    assert database.batch_authenticate(messages) == [None, None, None]


def test_bulk_writes_preserve_json_terminal_and_duplicate_id_order(database):
    database.batch_create_record([((name, "a", "coding", 100), {}) for name in ("1", "2")])
    database.batch_update_record(
        [
            (("1",), {"status": "running"}),
            (
                ("1",),
                {"status": "succeeded", "usage": {"total_tokens": 7}, "finished_at": time.time()},
            ),
            (
                ("2",),
                {"status": "failed", "usage": {"total_tokens": 3}, "finished_at": time.time()},
            ),
            (("1",), {"status": "failed"}),
        ]
    )
    assert database.get_record("a", "1")["status"] == "succeeded"
    assert database.get_record("a", "1")["usage"] == {"total_tokens": 7}
    assert database.get_record("a", "2")["status"] == "failed"


def test_bulk_insert_failure_rolls_back_whole_batch(database):
    with pytest.raises(IntegrityError):
        database.batch_create_record([(("same", "a", "coding", 100), {})] * 2)
    assert database.get_record("a", "same") is None


def test_bulk_update_failure_rolls_back_previous_group(database):
    database.create_record("1", "a", "coding", 100)
    with pytest.raises(IntegrityError):
        database.batch_update_record(
            [
                (("1",), {"status": "running"}),
                (("1",), {"status": None, "usage": {}}),
            ]
        )
    assert database.get_record("a", "1")["status"] == "accepted"

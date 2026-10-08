"""共享资源、恢复隔离和记录时间边界的回归验证。"""

import asyncio
import os
import time
import uuid

import pytest
import yaml
from redis.asyncio import Redis
from redis.exceptions import RedisError
from sqlalchemy.orm import Session

from model_gateway.app import Runtime
from model_gateway.config import Model, Settings, load_settings
from model_gateway.db import Application, Base, Database, key_hash
from model_gateway.quota import Quota


def test_capacity_group_configuration_and_conflicting_limits(tmp_path, monkeypatch):
    raw = {
        "database_url": "sqlite:///test.db",
        "redis_url": "redis://localhost:6379/0",
        "models": {
            name: {
                "model": name,
                "endpoint": "https://example.com/v1/chat",
                "capacity_group": "shared",
                "concurrency": 2,
            }
            for name in ("fast", "quality")
        },
    }
    path = tmp_path / "gateway.yaml"
    monkeypatch.setenv("GATEWAY_CONFIG", str(path))
    path.write_text(yaml.safe_dump(raw), "utf-8")
    assert load_settings().models["fast"].capacity_key == "shared"
    raw["models"]["quality"]["concurrency"] = 3
    path.write_text(yaml.safe_dump(raw), "utf-8")
    with pytest.raises(ValueError, match="容量组"):
        load_settings()


def test_persistent_policy_registration(tmp_path):
    db = Database(f"sqlite:///{tmp_path / 'guard.db'}")
    Base.metadata.create_all(db.engine)
    try:
        assert db.register_coordination("namespace", "a" * 64)
        assert not db.register_coordination("namespace", "a" * 64)
        with pytest.raises(ValueError, match="policy_mismatch"):
            db.register_coordination("namespace", "b" * 64)
    finally:
        db.engine.dispose()


@pytest.mark.parametrize("transport", ["direct", "batch"])
def test_original_execution_times_survive_database_queue_delay(tmp_path, transport):
    db = Database(f"sqlite:///{tmp_path / 'time.db'}")
    Base.metadata.create_all(db.engine)
    with Session(db.engine) as session, session.begin():
        session.add(Application(id="a", key_hash=key_hash("a"), model_allowlist=["m"]))
    started = time.time() - 8
    fields = dict(status="running", started_at=started, deadline_at=started + 20)
    try:
        if transport == "direct":
            db.create_record("r", "a", "m", 20, **fields)
        elif transport == "batch":
            db.batch_create_record([(("r", "a", "m", 20), fields)])
        row = db.get_record("a", "r")
        assert row["started_at"] == started
        assert row["deadline_at"] == started + 20
    finally:
        db.engine.dispose()


@pytest.mark.parametrize("batch", [False, True])
def test_late_observed_result_resolves_unknown_without_overwriting_final(tmp_path, batch):
    db = Database(f"sqlite:///{tmp_path / 'late.db'}")
    Base.metadata.create_all(db.engine)
    with Session(db.engine) as session, session.begin():
        session.add(Application(id="a", key_hash=key_hash("a"), model_allowlist=["m"]))
    try:
        db.create_record("r", "a", "m", 1, started_at=time.time() - 20)
        assert db.reconcile() == 1
        assert db.update_record("r", status="running") == 0
        fields = dict(
            status="succeeded", error="", finished_at=time.time(), usage={"total_tokens": 3}
        )
        if batch:
            db.batch_update_record([(("r",), fields)], ensure_existing=True)
        else:
            assert db.update_record("r", **fields) == 1
        row = db.get_record("a", "r")
        assert row["status"] == "succeeded" and row["error"] == ""
        assert db.update_record("r", status="failed") == 0
    finally:
        db.engine.dispose()


@pytest.mark.skipif(not os.environ.get("GATEWAY_TEST_REDIS"), reason="需要真实Redis")
async def test_shared_aliases_guard_loss_recovery_and_stale_owner():
    client = Redis.from_url(os.environ["GATEWAY_TEST_REDIS"])
    quota = Quota(client, "guard-" + uuid.uuid4().hex, 1)
    models = [
        Model(x, x, "https://example.com", "", capacity_group="shared") for x in ("fast", "quality")
    ]
    try:
        await quota.configure(["shared"], "a" * 64, 0)
        assert await quota.ready()
        assert await quota.acquire("a", models[0].capacity_key, "old", 1, 1)
        assert not await quota.acquire("a", models[1].capacity_key, "extra", 1, 1)
        await client.delete(quota.guard_key("shared"), *quota.keys("a", "shared"))
        assert not await quota.renew("a", "shared", "old")
        with pytest.raises(RedisError, match="coordination_lost"):
            await quota.acquire("a", "shared", "new", 1, 1)
        await quota.configure(["shared"], "a" * 64, 0.1)
        assert not await quota.ready()
        with pytest.raises(RedisError, match="recovering"):
            await quota.acquire("a", "shared", "new", 1, 1)
        assert not await quota.renew("a", "shared", "old")
        await asyncio.sleep(0.15)
        assert await quota.ready()
        assert await quota.acquire("a", "shared", "new", 1, 1)
        await quota.release("a", "shared", "old")
        assert not await quota.acquire("a", "shared", "another", 1, 1)
        with pytest.raises(RedisError, match="policy_mismatch"):
            await quota.configure(["shared"], "b" * 64, 0)
    finally:
        await quota.close()
        await client.delete(quota.guard_key("shared"), *quota.keys("a", "shared"))
        await client.aclose()


@pytest.mark.skipif(
    not (os.environ.get("GATEWAY_TEST_REDIS") and os.environ.get("GATEWAY_TEST_MYSQL")),
    reason="需要真实MySQL和Redis",
)
async def test_initial_sql_owner_completes_multi_process_initialization():
    settings = Settings(
        os.environ["GATEWAY_TEST_MYSQL"],
        os.environ["GATEWAY_TEST_REDIS"],
        "initial-race-" + uuid.uuid4().hex,
        {"m": Model("m", "fixture", "https://example.com", "")},
    )
    first, second = Runtime(settings), Runtime(settings)
    registered, allow = asyncio.Event(), asyncio.Event()
    original = first.quota.configure

    async def delayed(*args):
        registered.set()
        await allow.wait()
        await original(*args)

    first.quota.configure = delayed
    task = asyncio.create_task(first.ensure_coordination())
    try:
        await asyncio.wait_for(registered.wait(), 3)
        await second.ensure_coordination()
        assert not await second.quota.ready()
        allow.set()
        await asyncio.wait_for(task, 3)
        assert await first.quota.ready() and await second.quota.ready()
        assert first.quota.epochs == second.quota.epochs
        conflicting = Runtime(
            Settings(
                settings.database_url,
                settings.redis_url,
                settings.namespace,
                {"m": Model("m", "fixture", "https://example.com", "", concurrency=8)},
            )
        )
        try:
            with pytest.raises(RedisError, match="policy_mismatch"):
                await conflicting.ensure_coordination()
        finally:
            await conflicting.close()
    finally:
        allow.set()
        await asyncio.gather(task, return_exceptions=True)
        await first.redis.delete(first.quota.guard_key("m"))
        await asyncio.gather(first.close(), second.close())

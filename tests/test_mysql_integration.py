"""在专用MySQL库确认租约秒级精度与查询隔离。"""

import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import pytest
from sqlalchemy import text, update
from sqlalchemy.orm import Session

from model_gateway.db import Application, Database, RequestRecord, key_hash

pytestmark = pytest.mark.skipif(not os.environ.get("GATEWAY_TEST_MYSQL"), reason="需要专用MySQL库")


def test_mysql_deadline_precision_and_metadata_only():
    db = Database(os.environ["GATEWAY_TEST_MYSQL"])
    app_id = "precision-" + uuid.uuid4().hex[:16]
    request_id = str(uuid.uuid4())
    try:
        with Session(db.engine) as session, session.begin():
            session.add(
                Application(
                    id=app_id, key_hash=key_hash(app_id), model_allowlist=["coding"], concurrency=1
                )
            )
        db.create_record(request_id, app_id, "coding", 6)
        row = db.get_record(app_id, request_id)
        assert abs(row["deadline_at"] - row["started_at"] - 6) < 0.001
        assert abs(row["started_at"] - time.time()) < 2
        assert "messages" not in row and "answer" not in row
        assert db.get_record("foreign", request_id) is None
        db.update_record(
            request_id, status="succeeded", finished_at=time.time(), usage={"total_tokens": 7}
        )
        statistics = db.statistics(app_id, 1)
        assert statistics["total_requests"] == 1
        assert statistics["groups"][0]["observed_total_tokens"] == 7
    finally:
        db.engine.dispose()


def test_mysql_reconcile_skips_locked_rows_and_preserves_terminal():
    db = Database(os.environ["GATEWAY_TEST_MYSQL"])
    app_id = "locked-" + uuid.uuid4().hex[:16]
    ids = [str(uuid.uuid4()) for _ in range(3)]
    try:
        with Session(db.engine) as session, session.begin():
            session.add(
                Application(id=app_id, key_hash=key_hash(app_id), model_allowlist=["coding"])
            )
            session.flush()
            for index, request_id in enumerate(ids):
                session.add(
                    RequestRecord(
                        id=request_id,
                        app_id=app_id,
                        model="coding",
                        status="succeeded" if index == 2 else "accepted",
                        started_at=time.time() - 20,
                        deadline_at=time.time() - 10,
                    )
                )
        with db.engine.connect() as connection, connection.begin():
            connection.execute(
                text("SELECT id FROM requests WHERE id=:id FOR UPDATE"), {"id": ids[0]}
            )
            # 专用库也保留其他实验的过期记录；每批256行，不假定本次记录
            # 必然处于第一批。遍历已有积压，锁定行始终应被跳过。
            for _ in range(100):
                db.reconcile()
                if db.get_record(app_id, ids[1])["status"] == "abandoned":
                    break
            assert db.get_record(app_id, ids[0])["status"] == "accepted"
            assert db.get_record(app_id, ids[1])["status"] == "abandoned"
            assert db.get_record(app_id, ids[2])["status"] == "succeeded"
        db.reconcile()
        assert db.get_record(app_id, ids[0])["status"] == "abandoned"
    finally:
        db.engine.dispose()


def test_mysql_batch_json_commit_and_terminal_fencing():
    db = Database(os.environ["GATEWAY_TEST_MYSQL"])
    app_id = "batch-" + uuid.uuid4().hex[:16]
    ids = [str(uuid.uuid4()) for _ in range(8)]
    try:
        with Session(db.engine) as session, session.begin():
            session.add(
                Application(id=app_id, key_hash=key_hash(app_id), model_allowlist=["coding"])
            )
        db.batch_create_record([((request_id, app_id, "coding", 6), {}) for request_id in ids])
        db.batch_update_record(
            [
                (
                    (request_id,),
                    dict(
                        status="succeeded",
                        usage={"total_tokens": index},
                        finished_at=time.time(),
                        first_ms=index,
                        bytes_out=198,
                        error="",
                    ),
                )
                for index, request_id in enumerate(ids)
            ]
        )
        db.batch_update_record([((request_id,), {"status": "failed"}) for request_id in ids])
        for index, request_id in enumerate(ids):
            row = db.get_record(app_id, request_id)
            assert row["status"] == "succeeded"
            assert row["usage"] == {"total_tokens": index}
            assert row["first_ms"] == index
    finally:
        db.engine.dispose()


def test_auth_autocommit_pool_keeps_write_transactions_and_revocation():
    db = Database(os.environ["GATEWAY_TEST_MYSQL"], pool_size=4)
    app_id = "auth-pool-" + uuid.uuid4().hex[:16]
    try:
        with Session(db.engine) as session, session.begin():
            session.add(
                Application(id=app_id, key_hash=key_hash(app_id), model_allowlist=["coding"])
            )
        with db.auth_engine.connect() as connection:
            assert connection.scalar(text("SELECT @@autocommit")) == 1
        with db.engine.connect() as connection:
            assert connection.scalar(text("SELECT @@autocommit")) == 0
        assert db.authenticate(app_id)["id"] == app_id
        with db.engine.begin() as connection:
            connection.execute(
                update(Application).where(Application.id == app_id).values(enabled=False)
            )
        assert db.authenticate(app_id) is None
        assert db.batch_authenticate([((app_id,), {})]) == [None]
        assert db.auth_engine.pool.size() == 2
    finally:
        db.auth_engine.dispose()
        db.engine.dispose()


def test_retention_is_bounded_skips_locked_and_preserves_active_records():
    db = Database(os.environ["GATEWAY_TEST_MYSQL"], pool_size=4)
    app_id = "purge-" + uuid.uuid4().hex[:16]
    ids = [str(uuid.uuid4()) for _ in range(18)]
    now = time.time()
    try:
        with Session(db.engine) as session, session.begin():
            session.add(Application(id=app_id, key_hash=key_hash(app_id), model_allowlist=["m"]))
        db.batch_create_record([((value, app_id, "m", 10), {}) for value in ids])
        db.batch_update_record(
            [
                ((value,), {"status": "succeeded", "finished_at": now - 40 * 86400})
                for value in ids[:16]
            ]
            + [((ids[16],), {"status": "succeeded", "finished_at": now})]
        )
        with db.engine.connect() as conn, conn.begin():
            conn.execute(text("SELECT id FROM requests WHERE id=:id FOR UPDATE"), {"id": ids[0]})
            with ThreadPoolExecutor(max_workers=4) as workers:
                futures = [workers.submit(db.purge, 30, 3, app_id=app_id) for _ in range(8)]
                counts = [future.result(timeout=10) for future in futures]
            assert all(0 <= count <= 3 for count in counts)
            # SKIP LOCKED 的空批不代表全局没有积压，其他清理事务可能仍持有候选行。
            # 所有并发调用完成后继续有界清理，验证无重复计数且最终只剩外部锁定行。
            assert 0 < sum(counts) <= 15
            for _ in range(6):
                count = db.purge(30, 3, app_id=app_id)
                assert 0 <= count <= 3
                counts.append(count)
                if not count:
                    break
            assert sum(counts) == 15
            assert db.get_record(app_id, ids[0]) is not None
        assert db.purge(30, 3, app_id=app_id) == 1
        assert db.purge(30, 3, app_id=app_id) == 0
        assert db.get_record(app_id, ids[16])["status"] == "succeeded"
        assert db.get_record(app_id, ids[17])["status"] == "accepted"
        with pytest.raises(ValueError):
            db.purge(30, 1025, app_id=app_id)
    finally:
        db.auth_engine.dispose()
        db.engine.dispose()


def test_coordination_namespace_matches_eighty_character_configuration_limit():
    db = Database(os.environ["GATEWAY_TEST_MYSQL"])
    namespace = "guard-" + uuid.uuid4().hex + "x" * 42
    assert len(namespace) == 80
    try:
        assert db.register_coordination(namespace, "a" * 64)
        assert not db.register_coordination(namespace, "a" * 64)
    finally:
        db.auth_engine.dispose()
        db.engine.dispose()


def test_statistics_generated_tokens_follow_json_updates_without_manual_copy():
    db = Database(os.environ["GATEWAY_TEST_MYSQL"])
    app_id = "statistics-" + uuid.uuid4().hex[:16]
    ids = [str(uuid.uuid4()) for _ in range(3)]
    try:
        with Session(db.engine) as session, session.begin():
            session.add(Application(id=app_id, key_hash=key_hash(app_id), model_allowlist=["m"]))
        db.batch_create_record([((value, app_id, "m", 10), {}) for value in ids])
        db.update_record(ids[0], status="running", usage={"total_tokens": 3000000000})
        db.batch_update_record([((ids[1],), {"status": "running", "usage": {"total_tokens": 7}})])
        assert (
            sum(row["observed_total_tokens"] for row in db.statistics(app_id, 1)["groups"])
            == 3000000007
        )
        db.update_record(ids[0], status="running", usage={})
        assert sum(row["observed_total_tokens"] for row in db.statistics(app_id, 1)["groups"]) == 7
        assert "usage_total_tokens" not in db.get_record(app_id, ids[0])
    finally:
        db.auth_engine.dispose()
        db.engine.dispose()

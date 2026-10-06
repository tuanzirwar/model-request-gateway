"""在专用MySQL库确认租约秒级精度与查询隔离。"""

import os
import time
import uuid

import pytest
from sqlalchemy.orm import Session

from model_gateway.db import Application, Database, key_hash

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

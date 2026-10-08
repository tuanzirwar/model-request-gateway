"""本机专用库：保留期索引的执行计划、写入成本及分批清理实验。"""

import argparse
import hashlib
import json
import time
import uuid
from pathlib import Path

import yaml
from sqlalchemy import insert, text
from sqlalchemy.engine import make_url

from model_gateway.db import Application, Database, RequestRecord, key_hash

ROOT = Path(__file__).resolve().parent.parent


def query_measure(connection, query, params, repeats=10):
    costs, baseline = [], None
    for _ in range(repeats):
        started = time.perf_counter()
        ids = list(connection.scalars(text(query), params))
        costs.append((time.perf_counter() - started) * 1000)
        if baseline is not None:
            assert ids == baseline
        baseline = ids
    return {
        "mean_ms": sum(costs) / len(costs),
        "p95_ms": sorted(costs)[int((len(costs) - 1) * 0.95)],
        "result_count": len(baseline),
        "ordered_ids_hash": hashlib.sha256(json.dumps(baseline).encode()).hexdigest(),
        "explain": list(connection.execute(text("EXPLAIN " + query), params).mappings()),
        "explain_analyze": connection.scalar(text("EXPLAIN ANALYZE " + query), params),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default="reports/retention-index.json")
    args = parser.parse_args()
    target = (ROOT / args.output).resolve()
    if not target.is_relative_to(ROOT / "reports"):
        parser.error("报告必须在reports内")
    config = yaml.safe_load((ROOT / "gateway.yaml").read_text("utf-8"))
    url = make_url(config["database_url"])
    if url.host not in {"localhost", "127.0.0.1"}:
        parser.error("仅允许本机专用测试库")
    db = Database(url.set(database="model_gateway_test").render_as_string(hide_password=False))
    app = "retention-" + uuid.uuid4().hex[:16]
    report = {"passed": False, "scope": "synthetic local MySQL retention workload"}
    candidate = False
    now = time.time()
    try:
        with db.engine.begin() as conn:
            conn.execute(
                insert(Application),
                [
                    {
                        "id": app,
                        "key_hash": key_hash(app),
                        "enabled": True,
                        "concurrency": 1,
                        "model_allowlist": ["fixture"],
                    }
                ],
            )
        records = [
            dict(
                id=str(uuid.uuid4()),
                app_id=app,
                model="fixture",
                status="succeeded",
                error="",
                started_at=now - 40 * 86400 + i / 1000,
                deadline_at=now - 40 * 86400 + i / 1000 + 10,
                finished_at=now - 40 * 86400 + i / 1000 + 1,
                bytes_out=0,
                usage={},
            )
            for i in range(2000)
        ]
        with db.engine.begin() as conn:
            conn.execute(insert(RequestRecord), records)
        query = (
            "SELECT id FROM requests {hint} WHERE finished_at<:cutoff "
            "ORDER BY finished_at,id LIMIT 256"
        )
        params = {"cutoff": now - 30 * 86400}
        with db.engine.connect() as conn:
            indexes = {
                row["Key_name"] for row in conn.execute(text("SHOW INDEX FROM requests")).mappings()
            }
            hint = "IGNORE INDEX(ix_finished_id)" if "ix_finished_id" in indexes else ""
            report["rows_before"] = conn.scalar(text("SELECT COUNT(*) FROM requests"))
            report["before"] = query_measure(conn, query.format(hint=hint), params)
            conn.commit()
            started = time.perf_counter()
            conn.execute(
                text("CREATE INDEX ix_retention_candidate ON requests(finished_at,id) INVISIBLE")
            )
            candidate = True
            report["index_build_seconds"] = time.perf_counter() - started
            conn.execute(text("SET SESSION optimizer_switch='use_invisible_indexes=on'"))
            report["after"] = query_measure(conn, query.format(hint=""), params)
            assert report["before"]["ordered_ids_hash"] == report["after"]["ordered_ids_hash"]
            conn.commit()
            # 只删除本实验应用，避免清理任何已有实验或其他用户的记录。
        deleted, times = 0, []
        while True:
            started = time.perf_counter()
            count = db.purge(30, 256, app_id=app)
            times.append((time.perf_counter() - started) * 1000)
            deleted += count
            assert count <= 256
            if not count:
                break
        assert deleted == 2000
        report["bounded_delete"] = {
            "rows": deleted,
            "max_batch_rows": 256,
            "transactions": len(times),
            "max_ms": max(times),
            "mean_ms": sum(times) / len(times),
        }
        report["passed"] = True
    finally:
        if candidate:
            with db.engine.begin() as conn:
                conn.execute(text("DROP INDEX ix_retention_candidate ON requests"))
        target.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=dict), "utf-8")
        db.engine.dispose()
    print(
        json.dumps(
            {
                "passed": report["passed"],
                "before_ms": report["before"]["mean_ms"],
                "after_ms": report["after"]["mean_ms"],
                "deleted": deleted,
            }
        )
    )


if __name__ == "__main__":
    main()

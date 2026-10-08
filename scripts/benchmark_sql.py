"""在本机专用测试库构造十万行，比较真实筛选SQL及候选索引。"""

import argparse
import json
import time
import uuid
from pathlib import Path

import yaml
from sqlalchemy import func, insert, select, text
from sqlalchemy.engine import make_url

from model_gateway.db import Application, Database, RequestRecord, key_hash

ROOT = Path(__file__).resolve().parent.parent


def measure(connection, sql, params, repeats):
    values = []
    ids = None
    for _ in range(repeats):
        started = time.perf_counter()
        rows = connection.execute(text(sql), params).mappings().all()
        values.append((time.perf_counter() - started) * 1000)
        current = [row["id"] for row in rows]
        if ids is not None:
            assert current == ids
        ids = current
    return {
        "mean_ms": round(sum(values) / len(values), 3),
        "p95_ms": round(sorted(values)[int((len(values) - 1) * 0.95)], 3),
        "result_ids": ids,
        "explain": connection.execute(text("EXPLAIN " + sql), params).mappings().all(),
        "explain_analyze": connection.execute(text("EXPLAIN ANALYZE " + sql), params).scalar(),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=100000)
    parser.add_argument("--repeats", type=int, default=30)
    parser.add_argument("--output", default="reports/sql-pressure.json")
    args = parser.parse_args()
    output = (ROOT / args.output).resolve()
    if not output.is_relative_to(ROOT / "reports"):
        parser.error("报告必须位于reports目录")
    if not 10000 <= args.rows <= 1000000 or not 2 <= args.repeats <= 100:
        parser.error("行数需为一万至一百万，重复次数需为2至100")
    config = yaml.safe_load((ROOT / "gateway.yaml").read_text("utf-8"))
    url = make_url(config["database_url"])
    if url.host not in {"127.0.0.1", "localhost"}:
        parser.error("只允许本机测试库")
    database = Database(
        url.set(database="model_gateway_test").render_as_string(hide_password=False)
    )
    app_id = "sql-pressure-" + uuid.uuid4().hex[:16]
    report = {
        "passed": False,
        "synthetic_rows": args.rows,
        "app_id": app_id,
        "repeats": args.repeats,
        "before": {},
        "after": {},
        "limitations": [
            "synthetic skewed distribution",
            "warm same-host MySQL",
            "SQL latency is not chat generation throughput",
        ],
    }
    created = []
    try:
        with database.engine.begin() as connection:
            report["database_rows_before"] = connection.scalar(
                select(func.count()).select_from(RequestRecord)
            )
            connection.execute(
                insert(Application),
                [
                    {
                        "id": app_id,
                        "key_hash": key_hash(uuid.uuid4().hex),
                        "model_allowlist": ["fixture"],
                        "enabled": True,
                        "concurrency": 1,
                    }
                ],
            )
        now = time.time()
        started = time.perf_counter()
        for offset in range(0, args.rows, 1000):
            records = []
            for index in range(offset, min(args.rows, offset + 1000)):
                # 最新20%的记录均正常，稀疏失败/模型记录位于较早的区间。
                rare = index < args.rows * 0.8 and index % 100 == 0
                records.append(
                    {
                        "id": str(uuid.uuid4()),
                        "app_id": app_id,
                        "model": "rare" if rare else "fixture",
                        "status": "failed" if rare else "succeeded",
                        "error": "",
                        "started_at": now - (args.rows - index),
                        "deadline_at": now + 120,
                        "finished_at": now,
                        "first_ms": 1,
                        "bytes_out": 198,
                        "usage": {},
                    }
                )
            with database.engine.begin() as connection:
                connection.execute(insert(RequestRecord), records)
        report["seed_seconds"] = round(time.perf_counter() - started, 3)
        params = {"app": app_id, "cursor": now - args.rows * 0.1}
        queries = {
            "status": "SELECT * FROM requests WHERE app_id=:app AND status='failed' "
            "ORDER BY started_at DESC,id DESC LIMIT 26",
            "model": "SELECT * FROM requests WHERE app_id=:app AND model='rare' "
            "ORDER BY started_at DESC,id DESC LIMIT 26",
            "status_cursor": "SELECT * FROM requests WHERE app_id=:app AND status='failed' "
            "AND started_at<:cursor ORDER BY started_at DESC,id DESC LIMIT 26",
            "unfiltered": "SELECT * FROM requests WHERE app_id=:app "
            "ORDER BY started_at DESC,id DESC LIMIT 26",
        }
        with database.engine.connect() as connection:
            connection.execute(text("ANALYZE TABLE requests")).all()
            report["database_rows_after"] = connection.scalar(
                select(func.count()).select_from(RequestRecord)
            )
            existing = {
                row["Key_name"]
                for row in connection.execute(text("SHOW INDEX FROM requests")).mappings()
            }
            production = existing & {"ix_app_status_started_id", "ix_app_model_started_id"}
            report["baseline_ignored_indexes"] = sorted(production)
            report["queries"] = queries
            for name, sql in queries.items():
                baseline = (
                    sql.replace(
                        "FROM requests",
                        "FROM requests IGNORE INDEX (" + ",".join(sorted(production)) + ")",
                    )
                    if production
                    else sql
                )
                report["before"][name] = measure(connection, baseline, params, args.repeats)
                print(
                    json.dumps({"before": name, "mean_ms": report["before"][name]["mean_ms"]}),
                    flush=True,
                )
            for name, fields in (
                ("pressure_app_status", "app_id,status,started_at,id"),
                ("pressure_app_model", "app_id,model,started_at,id"),
            ):
                connection.execute(text(f"CREATE INDEX {name} ON requests ({fields}) INVISIBLE"))
                created.append(name)
            connection.execute(text("SET SESSION optimizer_switch='use_invisible_indexes=on'"))
            connection.execute(text("ANALYZE TABLE requests")).all()
            for name, sql in queries.items():
                report["after"][name] = measure(connection, sql, params, args.repeats)
                assert report["before"][name]["result_ids"] == report["after"][name]["result_ids"]
                print(
                    json.dumps({"after": name, "mean_ms": report["after"][name]["mean_ms"]}),
                    flush=True,
                )
        report["passed"] = True
    finally:
        # 只移除本次实验创建的候选索引，保留数据和失败证据。
        with database.engine.begin() as connection:
            for name in created:
                connection.execute(text(f"DROP INDEX {name} ON requests"))
        database.engine.dispose()
        output.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=dict), "utf-8")


if __name__ == "__main__":
    main()

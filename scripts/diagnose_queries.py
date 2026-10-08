"""在专用本机测试库核对统计查询改写的结果、耗时与执行计划。"""

import argparse
import json
import time
from pathlib import Path

import yaml
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

from model_gateway.db import Database

ROOT = Path(__file__).resolve().parent.parent


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--benchmark", default="reports/mq-final-100000.json")
    parser.add_argument("--output", default="reports/mysql-query-diagnosis.json")
    args = parser.parse_args()
    source, output = (ROOT / args.benchmark).resolve(), (ROOT / args.output).resolve()
    if not all(path.is_relative_to(ROOT / "reports") for path in (source, output)):
        parser.error("输入输出必须位于reports目录")
    benchmark = json.loads(source.read_text("utf-8"))
    url = make_url(yaml.safe_load((ROOT / "gateway.yaml").read_text("utf-8"))["database_url"])
    if url.host not in {"localhost", "127.0.0.1"}:
        parser.error("只允许本机专用测试库")
    database = Database(
        url.set(database="model_gateway_test").render_as_string(hide_password=False)
    )
    # 诊断旧慢查询允许等待60秒；正式服务仍保持5秒读写预算，不弱化运行保护。
    database.engine.dispose()
    database.engine = create_engine(
        url.set(database="model_gateway_test"),
        connect_args={"connect_timeout": 3, "read_timeout": 60, "write_timeout": 5},
    )
    statements = {
        "old_status_error": (
            "SELECT status,error,COUNT(*) AS n FROM requests "
            "WHERE app_id=:app GROUP BY status,error"
        ),
        "covering_status": (
            "SELECT status,COUNT(*) AS n FROM requests WHERE app_id=:app GROUP BY status"
        ),
        "rare_errors": (
            "SELECT status,error,COUNT(*) AS n FROM requests WHERE app_id=:app "
            "AND status IN ('failed','rejected','cancelled','abandoned') GROUP BY status,error"
        ),
        "retention_candidate": (
            "SELECT id FROM requests WHERE finished_at<:cutoff ORDER BY finished_at,id LIMIT 256"
        ),
    }
    params = {"app": benchmark["app_id"], "cutoff": time.time() - 30 * 86400}
    report = {
        "passed": False,
        "repeats": 5,
        "diagnostic_read_timeout_seconds": 60,
        "queries": {},
        "limitations": [
            "warm local database",
            "client latency includes transfer",
            "retention SELECT only; no deletion performed",
        ],
    }
    try:
        with database.engine.connect() as connection:
            connection.execute(text("SET SESSION information_schema_stats_expiry=0"))
            report["table"] = dict(
                connection.execute(
                    text(
                        "SELECT TABLE_ROWS,DATA_LENGTH,INDEX_LENGTH FROM information_schema.TABLES "
                        "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='requests'"
                    )
                )
                .mappings()
                .one()
            )
            report["exact_rows"] = connection.scalar(text("SELECT COUNT(*) FROM requests"))
            report["runtime"] = dict(
                connection.execute(
                    text(
                        "SELECT @@innodb_buffer_pool_size AS buffer_bytes,"
                        "@@innodb_flush_log_at_trx_commit AS flush_log,"
                        "@@sync_binlog AS sync_binlog,@@long_query_time AS slow_threshold_seconds"
                    )
                )
                .mappings()
                .one()
            )
            outcomes = {}
            for name, sql in statements.items():
                durations = []
                for _ in range(5):
                    start = time.perf_counter()
                    rows = [dict(row) for row in connection.execute(text(sql), params).mappings()]
                    durations.append((time.perf_counter() - start) * 1000)
                outcomes[name] = rows
                report["queries"][name] = {
                    "sql": sql,
                    "mean_ms": round(sum(durations) / len(durations), 3),
                    "samples_ms": [round(value, 3) for value in durations],
                    "explain": [
                        dict(row)
                        for row in connection.execute(text("EXPLAIN " + sql), params).mappings()
                    ],
                    "explain_analyze": connection.scalar(text("EXPLAIN ANALYZE " + sql), params),
                    "returned_rows": len(rows),
                }
            old = {}
            for row in outcomes["old_status_error"]:
                old[row["status"]] = old.get(row["status"], 0) + row["n"]
            new = {row["status"]: row["n"] for row in outcomes["covering_status"]}
            old_errors = [
                row
                for row in outcomes["old_status_error"]
                if row["status"] in {"failed", "rejected", "cancelled", "abandoned"}
            ]
            report["equal_counts"] = old == new and old_errors == outcomes["rare_errors"]
            report["app_rows"] = sum(new.values())
            report["passed"] = report["equal_counts"]
    finally:
        database.auth_engine.dispose()
        database.engine.dispose()
        output.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str), "utf-8")
    print(
        json.dumps(
            {
                "passed": report["passed"],
                "exact_rows": report["exact_rows"],
                "mean_ms": {name: row["mean_ms"] for name, row in report["queries"].items()},
            }
        )
    )


if __name__ == "__main__":
    main()

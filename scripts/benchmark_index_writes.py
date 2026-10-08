"""独立临时表上的索引写入成本实验；不删除或修改现有业务记录。"""

import argparse
import json
import time
import uuid
from pathlib import Path

import yaml
from sqlalchemy import text
from sqlalchemy.engine import make_url

from model_gateway.db import Database

ROOT = Path(__file__).resolve().parent.parent


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--index-name", choices=["ix_finished_id", "ix_app_statistics"], default="ix_finished_id"
    )
    parser.add_argument("--output", default="reports/v05-index-write-cost.json")
    args = parser.parse_args()
    target = (ROOT / args.output).resolve()
    if not target.is_relative_to(ROOT / "reports"):
        parser.error("报告必须在reports内")
    columns = {
        "ix_finished_id": "finished_at,id",
        "ix_app_statistics": (
            "app_id,started_at,model,status,first_ms,finished_at,bytes_out,usage_total_tokens"
        ),
    }[args.index_name]
    url = make_url(yaml.safe_load((ROOT / "gateway.yaml").read_text("utf-8"))["database_url"])
    assert url.host in {"localhost", "127.0.0.1"}
    db = Database(url.set(database="model_gateway_test").render_as_string(hide_password=False))
    table = "index_probe_" + uuid.uuid4().hex
    # 表名仅来自固定前缀和本机生成的十六进制 UUID，不接收外部输入。
    report = {
        "passed": False,
        "scope": "isolated synthetic table, ABBA, 20000 rows per round",
        "rounds": [],
        "index_name": args.index_name,
        "limitations": ["local warm cache", "not production write amplification"],
    }
    created = False
    try:
        with db.engine.begin() as conn:
            conn.execute(text(f"CREATE TABLE {table} LIKE requests"))
            created = True
        for index_enabled in (False, True, True, False):
            with db.engine.begin() as conn:
                conn.execute(text(f"TRUNCATE TABLE {table}"))
                present = {
                    row["Key_name"]
                    for row in conn.execute(text(f"SHOW INDEX FROM {table}")).mappings()
                }
                if index_enabled and args.index_name not in present:
                    conn.execute(text(f"CREATE INDEX {args.index_name} ON {table}({columns})"))
                if not index_enabled and args.index_name in present:
                    conn.execute(text(f"DROP INDEX {args.index_name} ON {table}"))
            rows = [
                {
                    "id": str(uuid.uuid4()),
                    "started": time.time(),
                    "deadline": time.time() + 120,
                    "finished": time.time(),
                }
                for _ in range(20000)
            ]
            started = time.perf_counter()
            for offset in range(0, len(rows), 256):
                with db.engine.begin() as conn:
                    conn.execute(
                        text(
                            f"INSERT INTO {table} "
                            "(id,app_id,model,status,error,started_at,deadline_at,"
                            "finished_at,bytes_out,`usage`) "
                            "VALUES(:id,'synthetic','fixture','succeeded','',:started,:deadline,:finished,198,'{}')"
                        ),
                        rows[offset : offset + 256],
                    )
            seconds = time.perf_counter() - started
            with db.engine.connect() as conn:
                count = conn.scalar(text(f"SELECT COUNT(*) FROM {table}"))
            assert count == 20000
            report["rounds"].append(
                {
                    "index_enabled": index_enabled,
                    "rows": count,
                    "seconds": seconds,
                    "rows_per_second": count / seconds,
                }
            )
        report["passed"] = True
    finally:
        if created:
            with db.engine.begin() as conn:
                conn.execute(text(f"DROP TABLE {table}"))
        db.auth_engine.dispose()
        db.engine.dispose()
        target.write_text(json.dumps(report, indent=2), "utf-8")
    print(json.dumps(report))


if __name__ == "__main__":
    main()

"""在新建独立MySQL数据库上验证完整迁移，不以ORM建表替代迁移。"""

import json
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path

import pymysql
from sqlalchemy import create_engine, text

ROOT = Path(__file__).resolve().parent.parent


def main():
    # 开发环境演示口令，数据库名固定前缀且仅包含程序生成的十六进制字符。
    name = "model_gateway_migration_" + uuid.uuid4().hex[:12]
    connection = pymysql.connect(
        host="127.0.0.1", port=13316, user="root", password="local-root-demo-only"
    )
    with connection.cursor() as cursor:
        cursor.execute(f"CREATE DATABASE `{name}` CHARACTER SET utf8mb4")
        cursor.execute(f"GRANT ALL ON `{name}`.* TO 'gateway'@'127.0.0.1'")
    connection.close()
    url = f"mysql+pymysql://gateway:local-gateway-only@127.0.0.1:13316/{name}"
    env = dict(os.environ, GATEWAY_DATABASE_URL=url)
    result = {"passed": False, "database": name, "checks": []}
    engine = create_engine(url)
    try:
        for _ in range(2):
            subprocess.run(
                [sys.executable, "-m", "alembic", "upgrade", "head"],
                cwd=ROOT,
                env=env,
                check=True,
            )
        with engine.begin() as conn:
            version = conn.execute(text("SELECT version_num FROM alembic_version")).scalar_one()
            assert version == "0002", version
            result["checks"].append("empty_database_upgrade_and_repeat")
            columns = conn.execute(text("SHOW COLUMNS FROM requests")).mappings().all()
            types = {row["Field"]: row["Type"] for row in columns}
            for field in ("started_at", "deadline_at", "finished_at"):
                assert types[field].lower() == "double", types
            result["checks"].append("three_timestamps_double")
            now = time.time()
            conn.execute(
                text(
                    "INSERT INTO applications VALUES "
                    "('migration-test', :hash, 1, '[\"coding\"]', 2)"
                ),
                {"hash": uuid.uuid4().hex * 2},
            )
            conn.execute(
                text(
                    "INSERT INTO requests "
                    "(id,app_id,model,status,error,started_at,deadline_at,bytes_out,`usage`) "
                    "VALUES (:id,'migration-test','coding','accepted','',:now,:deadline,0,'{}')"
                ),
                {"id": str(uuid.uuid4()), "now": now, "deadline": now + 6},
            )
            delta = conn.execute(text("SELECT deadline_at-started_at FROM requests")).scalar_one()
            assert abs(delta - 6) < 0.001, delta
            result["checks"].append("six_second_deadline_precision")
            indexes = conn.execute(text("SHOW INDEX FROM requests")).mappings().all()
            assert {"ix_app_started_id", "ix_status_deadline"}.issubset(
                {row["Key_name"] for row in indexes}
            )
            result["checks"].append("cursor_and_reconcile_indexes")
        result["passed"] = True
    finally:
        engine.dispose()
        (ROOT / "reports/migrations.json").write_text(
            json.dumps(result, ensure_ascii=False, indent=2), "utf-8"
        )
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()

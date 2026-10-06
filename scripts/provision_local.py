"""仅为本机测试创建独立数据库和用户，不修改其他应用表。"""

from pathlib import Path

import pymysql
import yaml

ROOT = Path(__file__).resolve().parent.parent


def main():
    connection = pymysql.connect(
        host="127.0.0.1", port=13316, user="root", password="local-root-demo-only"
    )
    with connection.cursor() as cursor:
        cursor.execute("CREATE DATABASE IF NOT EXISTS model_gateway CHARACTER SET utf8mb4")
        cursor.execute("CREATE DATABASE IF NOT EXISTS model_gateway_test CHARACTER SET utf8mb4")
        cursor.execute(
            "CREATE USER IF NOT EXISTS 'gateway'@'127.0.0.1' IDENTIFIED BY 'local-gateway-only'"
        )
        cursor.execute("GRANT ALL ON model_gateway.* TO 'gateway'@'127.0.0.1'")
        cursor.execute("GRANT ALL ON model_gateway_test.* TO 'gateway'@'127.0.0.1'")
    connection.close()
    config = yaml.safe_load((ROOT / "gateway.example.yaml").read_text("utf-8"))
    config["database_url"] = (
        "mysql+pymysql://gateway:local-gateway-only@127.0.0.1:13316/model_gateway?charset=utf8mb4"
    )
    config["redis_url"] = "redis://127.0.0.1:16379/0"
    target = ROOT / "gateway.yaml"
    if not target.exists():
        target.write_text(yaml.safe_dump(config, allow_unicode=True), "utf-8")
    print("独立测试库及本机配置就绪；演示口令仅限本机")


if __name__ == "__main__":
    main()

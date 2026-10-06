"""生成本机演示密钥和TUI配置，不改动用户原有config.yaml。"""

import argparse
import secrets
from pathlib import Path

import yaml
from sqlalchemy.orm import Session

from model_gateway.config import load_settings
from model_gateway.db import Application, Database, key_hash


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8788)
    args = parser.parse_args()
    root = Path(__file__).resolve().parent.parent
    directory = root / ".local/tui-demo"
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / "config.yaml"
    key = (
        yaml.safe_load(target.read_text("utf-8"))["providers"][0]["api_key"]
        if target.exists()
        else secrets.token_urlsafe(32)
    )
    db = Database(load_settings().database_url)
    with Session(db.engine) as session, session.begin():
        row = session.get(Application, "tui-demo")
        if row is None:
            row = Application(id="tui-demo")
            session.add(row)
        row.key_hash, row.enabled, row.model_allowlist, row.concurrency = (
            key_hash(key),
            True,
            ["coding"],
            2,
        )
    config = {
        "providers": [
            {
                "name": "Model Gateway",
                "protocol": "openai",
                "model": "coding",
                "base_url": f"http://127.0.0.1:{args.port}/v1/chat/completions",
                "api_key": key,
                "wire_api": "chat_completions",
                "thinking": False,
            }
        ]
    }
    target.write_text(yaml.safe_dump(config, allow_unicode=True), "utf-8")
    db.engine.dispose()
    print("演示应用与隔离TUI配置已就绪，密钥不回显")


if __name__ == "__main__":
    main()

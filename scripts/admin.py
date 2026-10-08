"""本地管理入口：初始化应用、撤销权限、对账及清理记录。"""

import argparse
import json
import os
import re

from sqlalchemy import select
from sqlalchemy.orm import Session

from model_gateway.config import load_settings
from model_gateway.db import Application, Database, key_hash


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "command", choices=["grant", "rotate", "list", "disable", "reconcile", "purge"]
    )
    parser.add_argument("--app", default="tui")
    parser.add_argument("--models", nargs="+", default=["coding"])
    parser.add_argument("--concurrency", type=int, default=2)
    parser.add_argument("--batch-size", type=int, default=256)
    parser.add_argument("--batches", type=int, default=1)
    args = parser.parse_args()
    if not re.fullmatch(r"[A-Za-z0-9_.:-]{1,64}", args.app):
        parser.error("应用ID必须为长度1至64的安全标识")
    settings = load_settings()
    database = Database(settings.database_url)
    if args.command == "grant":
        key = os.environ.get("GATEWAY_APP_KEY", "")
        if len(key) < 32 or args.concurrency < 1 or not set(args.models) <= settings.models.keys():
            parser.error("GATEWAY_APP_KEY至少32字符，并发需为正，模型需在配置白名单内")
        with Session(database.engine) as session, session.begin():
            row = session.get(Application, args.app)
            if row is None:
                row = Application(id=args.app)
                session.add(row)
            row.key_hash = key_hash(key)
            row.enabled = True
            row.model_allowlist = args.models
            row.concurrency = args.concurrency
        print("应用授权已保存，密钥不回显")
    elif args.command == "rotate":
        key = os.environ.get("GATEWAY_APP_KEY", "")
        if len(key) < 32:
            parser.error("新密钥至少32字符")
        with Session(database.engine) as session, session.begin():
            row = session.get(Application, args.app)
            if row is None:
                parser.error("应用不存在")
            row.key_hash = key_hash(key)
        print("密钥已轮换，旧密钥不再允许新接入；已有流不被强制撤销")
    elif args.command == "list":
        with Session(database.engine) as session:
            rows = session.scalars(select(Application).order_by(Application.id)).all()
            print(
                json.dumps(
                    [
                        {
                            "id": row.id,
                            "enabled": row.enabled,
                            "models": row.model_allowlist,
                            "concurrency": row.concurrency,
                        }
                        for row in rows
                    ],
                    ensure_ascii=False,
                    indent=2,
                )
            )
    elif args.command == "disable":
        with Session(database.engine) as session, session.begin():
            row = session.get(Application, args.app)
            if row:
                row.enabled = False
        print("应用已禁用；已接入请求不在此命令中强制取消")
    elif args.command == "reconcile":
        print(database.reconcile())
    else:
        if not 1 <= args.batches <= 100 or not 1 <= args.batch_size <= 1024:
            parser.error("清理批次数必须为1至100，每批1至1024条")
        count = 0
        for _ in range(args.batches):
            deleted = database.purge(settings.retention_days, args.batch_size)
            count += deleted
            if deleted < args.batch_size:
                break
        print(json.dumps({"deleted": count, "bounded_batches": args.batches}))
    database.engine.dispose()


if __name__ == "__main__":
    main()

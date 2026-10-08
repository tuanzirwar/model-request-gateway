"""真实统计接口的结果一致性、执行计划与HTTP验收，仅用于专用测试库。"""

import asyncio
import json
import math
import os
import secrets
import subprocess
import sys
import time
import uuid
from pathlib import Path

import httpx
import yaml
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from verify_e2e import port, stop_process

from model_gateway.db import Database, key_hash

ROOT = Path(__file__).resolve().parent.parent


async def main():
    config = yaml.safe_load((ROOT / "gateway.yaml").read_text("utf-8"))
    url = make_url(config["database_url"])
    assert url.host in {"localhost", "127.0.0.1"}
    config["database_url"] = url.set(database="model_gateway_test").render_as_string(
        hide_password=False
    )
    app_id = json.loads((ROOT / "reports/v05-final-100000.json").read_text("utf-8"))["app_id"]
    assert app_id.startswith("benchmark-")
    engine = create_engine(config["database_url"], connect_args={"read_timeout": 60})
    report = {
        "passed": False,
        "scope": "synthetic benchmark application in local test database",
        "diagnostic_read_timeout_seconds": 60,
        "runtime_read_timeout_seconds": 5,
        "queries": {},
    }
    # 仅轮换本实验已完成的随机测试应用密钥，既有用户应用与业务库不受影响。
    key = secrets.token_urlsafe(32)
    cutoff = time.time() - 24 * 3600
    old_tokens = "COALESCE(CAST(JSON_EXTRACT(`usage`,'$.total_tokens') AS SIGNED),0)"
    template = (
        "SELECT model,status,COUNT(*) AS requests,AVG(first_ms) AS average_first_frame_ms,"
        "AVG((finished_at-started_at)*1000) AS average_terminal_ms,"
        "SUM(bytes_out) AS attempted_bytes,SUM({tokens}) AS observed_total_tokens "
        "FROM requests {hint} WHERE app_id=:app AND started_at>=:cutoff "
        "GROUP BY model,status ORDER BY model,status"
    )
    process, log = None, None
    try:
        outcomes = {}
        with engine.connect() as conn:
            for name, tokens, hint in [
                ("old", old_tokens, "IGNORE INDEX(ix_app_statistics)"),
                ("new", "usage_total_tokens", ""),
            ]:
                query = template.format(tokens=tokens, hint=hint)
                durations = []
                for _ in range(3):
                    started = time.perf_counter()
                    rows = [
                        dict(row)
                        for row in conn.execute(
                            text(query), {"app": app_id, "cutoff": cutoff}
                        ).mappings()
                    ]
                    durations.append((time.perf_counter() - started) * 1000)
                outcomes[name] = rows
                report["queries"][name] = {
                    "mean_ms": sum(durations) / len(durations),
                    "samples_ms": durations,
                    "explain": [
                        dict(r)
                        for r in conn.execute(
                            text("EXPLAIN " + query), {"app": app_id, "cutoff": cutoff}
                        ).mappings()
                    ],
                    "explain_analyze": conn.scalar(
                        text("EXPLAIN ANALYZE " + query), {"app": app_id, "cutoff": cutoff}
                    ),
                }
        assert len(outcomes["old"]) == len(outcomes["new"])
        for before, after in zip(outcomes["old"], outcomes["new"], strict=True):
            for field, value in before.items():
                other = after[field]
                if value is None or isinstance(value, str):
                    assert value == other
                else:
                    assert math.isclose(float(value), float(other), rel_tol=1e-9, abs_tol=1e-6)
        report["equal_results"] = True
        with engine.begin() as conn:
            assert (
                conn.execute(
                    text("UPDATE applications SET key_hash=:key WHERE id=:app"),
                    {"key": key_hash(key), "app": app_id},
                ).rowcount
                == 1
            )
        # 再通过实际Runtime的5秒驱动预算读取，诊断连接的60秒不得掩盖接口超时。
        db = Database(config["database_url"])
        started = time.perf_counter()
        direct = db.statistics(app_id, 24)
        report["runtime_direct_ms"] = (time.perf_counter() - started) * 1000
        db.auth_engine.dispose()
        db.engine.dispose()
        gateway = port()
        config["namespace"] = "stats-http-" + uuid.uuid4().hex
        config["models"] = {
            "fixture": {"model": "fixture", "endpoint": "http://127.0.0.1:1/v1/chat/completions"}
        }
        target = ROOT / ".local" / (config["namespace"] + ".yaml")
        target.write_text(yaml.safe_dump(config), "utf-8")
        log = (ROOT / "reports" / f"statistics-server-{gateway}.txt").open("w", encoding="utf-8")
        process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "uvicorn",
                "model_gateway.app:create_app",
                "--factory",
                "--host",
                "127.0.0.1",
                "--port",
                str(gateway),
                "--log-level",
                "warning",
            ],
            cwd=ROOT,
            env={**os.environ, "GATEWAY_CONFIG": str(target)},
            stdout=log,
            stderr=log,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        base = f"http://127.0.0.1:{gateway}"
        async with httpx.AsyncClient(timeout=10, trust_env=False) as client:
            for _ in range(100):
                try:
                    if (await client.get(base + "/health")).status_code == 200:
                        break
                except httpx.HTTPError:
                    pass
                await asyncio.sleep(0.1)
            else:
                raise AssertionError("统计验收网关未就绪")
            started = time.perf_counter()
            response = await client.get(
                base + "/stats?hours=24", headers={"Authorization": "Bearer " + key}
            )
            report["http_ms"] = (time.perf_counter() - started) * 1000
            report["http_status"] = response.status_code
            assert response.status_code == 200
            data = response.json()
            assert data["groups"] == direct["groups"]
            report["total_requests"] = data["total_requests"]
            assert data["total_requests"] >= 100000
        report["passed"] = True
    finally:
        if process:
            stop_process(process)
        if log:
            log.close()
        engine.dispose()
        (ROOT / "reports/v05-statistics-index.json").write_text(
            json.dumps(report, indent=2, default=str), "utf-8"
        )
    print(json.dumps({k: v for k, v in report.items() if k != "queries"}))


if __name__ == "__main__":
    asyncio.run(main())

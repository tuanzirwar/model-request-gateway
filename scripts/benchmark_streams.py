"""真实 TCP 长流与途中取消实验，使用本机专用库和受控上游。"""

import asyncio
import hashlib
import json
import os
import secrets
import subprocess
import sys
import time
import uuid
from pathlib import Path

import httpx
import yaml
from benchmark import percentile
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session
from verify_e2e import port, stop_process

from model_gateway.db import Application, Database, key_hash

ROOT = Path(__file__).resolve().parent.parent


async def main():
    config = yaml.safe_load((ROOT / "gateway.yaml").read_text("utf-8"))
    url = make_url(config["database_url"])
    assert url.host in {"localhost", "127.0.0.1"}
    config["database_url"] = url.set(database="model_gateway_test").render_as_string(
        hide_password=False
    )
    upstream, gateway = port(), port()
    app_id = "streams-" + uuid.uuid4().hex
    key = secrets.token_urlsafe(32)
    config.update(namespace=app_id, lease_seconds=3, total_seconds=30, idle_seconds=2)
    config["models"] = {
        "fixture": {
            "model": "fixture",
            "concurrency": 32,
            "endpoint": f"http://127.0.0.1:{upstream}/v1/chat/completions",
        }
    }
    target = ROOT / ".local" / (app_id + ".yaml")
    target.write_text(yaml.safe_dump(config), "utf-8")
    db = Database(config["database_url"])
    with Session(db.engine) as session, session.begin():
        session.add(
            Application(
                id=app_id, key_hash=key_hash(key), concurrency=32, model_allowlist=["fixture"]
            )
        )
    processes, logs = [], []
    report = {
        "passed": False,
        "concurrency": 16,
        "frames_per_stream": 200,
        "payload_bytes": 4096,
        "delay_seconds": 0.025,
        "cancel_after_frames": 40,
        "scope": "local real TCP gateway, controlled SSE, no model inference",
        "source_sha256": {
            p.name: hashlib.sha256(p.read_bytes()).hexdigest()
            for p in (ROOT / "src/model_gateway").glob("*.py")
        },
    }
    headers = {"Authorization": "Bearer " + key}
    base = f"http://127.0.0.1:{gateway}"
    try:
        for module, endpoint, factory in [
            ("scripts.upstream_fixture:app", upstream, False),
            ("model_gateway.app:create_app", gateway, True),
        ]:
            log = (ROOT / "reports" / f"streams-server-{endpoint}.txt").open("w", encoding="utf-8")
            logs.append(log)
            args = [
                sys.executable,
                "-m",
                "uvicorn",
                module,
                "--host",
                "127.0.0.1",
                "--port",
                str(endpoint),
                "--log-level",
                "warning",
            ] + (["--factory"] if factory else [])
            processes.append(
                subprocess.Popen(
                    args,
                    cwd=ROOT,
                    env={**os.environ, "GATEWAY_CONFIG": str(target), "PYTHONIOENCODING": "utf-8"},
                    stdout=log,
                    stderr=log,
                    creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                )
            )
        async with httpx.AsyncClient(timeout=40, trust_env=False) as client:
            for _ in range(100):
                try:
                    if (await client.get(base + "/health")).status_code == 200:
                        break
                except httpx.HTTPError:
                    pass
                await asyncio.sleep(0.1)
            else:
                raise AssertionError("网关未就绪")

            async def consume(index):
                started = time.perf_counter()
                frames, size, first, done = 0, 0, None, False
                async with client.stream(
                    "POST",
                    base + "/v1/chat/completions",
                    headers=headers,
                    json={
                        "model": "fixture",
                        "stream": True,
                        "messages": [{"role": "user", "content": "paced:200:4096:0.025"}],
                    },
                ) as r:
                    assert r.status_code == 200
                    request_id = r.headers["X-Request-ID"]
                    async for line in r.aiter_lines():
                        if not line.startswith("data:"):
                            continue
                        if first is None:
                            first = (time.perf_counter() - started) * 1000
                        size += len(line.encode()) + 2
                        if "[DONE]" in line:
                            done = True
                            break
                        value = json.loads(line.removeprefix("data:").strip())
                        assert "error" not in value
                        frames += 1
                        if index < 4 and frames == 40:
                            break
                return {
                    "request_id": request_id,
                    "first_ms": first,
                    "done": done,
                    "frames": frames,
                    "bytes": size,
                    "seconds": time.perf_counter() - started,
                }

            started = time.perf_counter()
            samples = await asyncio.gather(*(consume(i) for i in range(16)))
            seconds = time.perf_counter() - started
            for _ in range(80):
                rows = [
                    (
                        await client.get(base + "/requests/" + s["request_id"], headers=headers)
                    ).json()
                    for s in samples
                ]
                stats = (await client.get(f"http://127.0.0.1:{upstream}/stats")).json()
                if stats["active"] == 0 and all(
                    r["status"] not in {"accepted", "running"} for r in rows
                ):
                    break
                await asyncio.sleep(0.1)
            report.update(
                completed=sum(s["done"] for s in samples),
                cancelled=sum(not s["done"] for s in samples),
                first_frame_p95_ms=round(percentile([s["first_ms"] for s in samples], 0.95), 2),
                elapsed_seconds=round(seconds, 3),
                body_mbps=sum(s["bytes"] for s in samples) * 8 / seconds / 1e6,
                samples=samples,
                terminal_counts={
                    status: sum(r["status"] == status for r in rows)
                    for status in {r["status"] for r in rows}
                },
                upstream_active_after=stats["active"],
                upstream_max_active=stats["max_active"],
            )
            assert report["completed"] == 12 and report["cancelled"] == 4
            assert report["terminal_counts"] == {"succeeded": 12, "cancelled": 4}
            assert stats["active"] == 0
            report["source_unchanged"] = all(
                hashlib.sha256((ROOT / "src/model_gateway" / name).read_bytes()).hexdigest()
                == value
                for name, value in report["source_sha256"].items()
            )
            assert report["source_unchanged"]
            report["passed"] = True
    finally:
        for process in processes:
            stop_process(process)
        for log in logs:
            log.close()
        db.auth_engine.dispose()
        db.engine.dispose()
        (ROOT / "reports/v05-streams.json").write_text(json.dumps(report, indent=2), "utf-8")
    print(json.dumps({k: v for k, v in report.items() if k not in {"samples", "source_sha256"}}))


if __name__ == "__main__":
    asyncio.run(main())

"""隔离 Redis 进程强杀与恢复：只操作本次启动的本机随机端口实例。"""

import argparse
import asyncio
import json
import os
import shlex
import subprocess
import sys
import time
import uuid
from pathlib import Path

import httpx
import yaml
from redis.asyncio import Redis
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session
from verify_e2e import port, stop_process

from model_gateway.db import Application, Database, key_hash

ROOT = Path(__file__).resolve().parent.parent


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--wsl-distribution", default="AfterCareInfra")
    args = parser.parse_args()
    config = yaml.safe_load((ROOT / "gateway.yaml").read_text("utf-8"))
    url = make_url(config["database_url"])
    assert url.host in {"localhost", "127.0.0.1"}
    config["database_url"] = url.set(database="model_gateway_test").render_as_string(
        hide_password=False
    )
    name = "restart-" + uuid.uuid4().hex
    directory = ROOT / ".local" / name
    directory.mkdir()
    redis_port, upstream_port, gateway_port = port(), port(), port()
    redis_url = f"redis://127.0.0.1:{redis_port}/0"
    config.update(
        namespace=name,
        redis_url=redis_url,
        lease_seconds=1,
        total_seconds=3,
        first_seconds=1,
        idle_seconds=3,
    )
    config["models"] = {
        alias: {
            "model": "fixture",
            "endpoint": f"http://127.0.0.1:{upstream_port}/v1/chat/completions",
            "capacity_group": "shared",
            "concurrency": 1,
        }
        for alias in ("fast", "quality")
    }
    config_file = directory / "gateway.yaml"
    config_file.write_text(yaml.safe_dump(config), "utf-8")
    database = Database(config["database_url"])
    key = uuid.uuid4().hex + uuid.uuid4().hex
    with Session(database.engine) as session, session.begin():
        session.add(
            Application(
                id=name,
                key_hash=key_hash(key),
                model_allowlist=["fast", "quality"],
                concurrency=1,
                enabled=True,
            )
        )
    env = {**os.environ, "GATEWAY_CONFIG": str(config_file), "PYTHONIOENCODING": "utf-8"}
    env.pop("GATEWAY_DATABASE_URL", None)
    env.pop("GATEWAY_REDIS_URL", None)
    processes, logs = [], []
    client_redis = Redis.from_url(redis_url, socket_timeout=1, socket_connect_timeout=1)
    report = {
        "passed": False,
        "checks": [],
        "redis_persistence": "none for loss injection",
        "total_seconds": 3,
        "recovery_guard_seconds": 8,
    }

    def check(name, passed, **details):
        report["checks"].append({"name": name, "passed": bool(passed), **details})
        assert passed, name

    def linux_path(path):
        return "/mnt/" + path.drive[0].lower() + path.as_posix()[2:]

    def start_redis():
        base = linux_path(ROOT / ".local" / "redis")
        working = linux_path(directory)
        command = (
            f"LD_LIBRARY_PATH={shlex.quote(base + '/usr/lib:' + base + '/lib')} "
            f"{shlex.quote(base + '/usr/bin/redis-server')} --bind 127.0.0.1 "
            f"--port {redis_port} --save '' --appendonly no --daemonize no "
            f"--dir {shlex.quote(working)} --maxmemory-policy noeviction"
        )
        log = (directory / f"redis-{len(logs)}.log").open("w", encoding="utf-8")
        logs.append(log)
        process = subprocess.Popen(
            ["wsl", "-d", args.wsl_distribution, "--", "sh", "-c", command],
            stdout=log,
            stderr=log,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        processes.append(process)

    def start_http(module, server_port, factory=False):
        log = (directory / f"http-{server_port}.log").open("w", encoding="utf-8")
        logs.append(log)
        command = [
            sys.executable,
            "-m",
            "uvicorn",
            module,
            "--host",
            "127.0.0.1",
            "--port",
            str(server_port),
            "--log-level",
            "warning",
        ]
        if factory:
            command.append("--factory")
        process = subprocess.Popen(
            command,
            cwd=ROOT,
            env=env,
            stdout=log,
            stderr=log,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        processes.append(process)

    async def wait_redis():
        for _ in range(100):
            try:
                if await client_redis.ping():
                    return
            except Exception:
                pass
            await asyncio.sleep(0.1)
        raise RuntimeError("isolated_redis_not_ready")

    try:
        start_redis()
        await wait_redis()
        start_http("scripts.upstream_fixture:app", upstream_port)
        start_http("model_gateway.app:create_app", gateway_port, True)
        base = f"http://127.0.0.1:{gateway_port}"
        headers = {"Authorization": "Bearer " + key}
        payload = {
            "model": "fast",
            "messages": [{"role": "user", "content": "hold:restart"}],
            "stream": True,
        }
        async with httpx.AsyncClient(timeout=15, trust_env=False) as client:
            for _ in range(150):
                try:
                    if (await client.get(base + "/health")).status_code == 200:
                        break
                except httpx.HTTPError:
                    pass
                await asyncio.sleep(0.1)
            else:
                raise RuntimeError("gateway_not_ready")
            async with client.stream(
                "POST", base + "/v1/chat/completions", headers=headers, json=payload
            ) as stream:
                check("initial_stream", stream.status_code == 200)
                iterator = stream.aiter_lines()
                check("first_frame_received", (await anext(iterator)).startswith("data:"))
                rejected = await client.post(
                    base + "/v1/chat/completions",
                    headers=headers,
                    json={**payload, "model": "quality"},
                )
                check("aliases_share_capacity", rejected.status_code == 429)
                info = await client_redis.info("server")
                old_run_id = info["run_id"]
                pid = int(info["process_id"])
                # PID 来自本次随机端口实例，不枚举或终止共享 Redis 服务。
                subprocess.run(
                    ["wsl", "-d", args.wsl_distribution, "--", "kill", "-9", str(pid)], check=True
                )
                started = time.monotonic()
                start_redis()
                await wait_redis()
                check(
                    "new_redis_process", (await client_redis.info("server"))["run_id"] != old_run_id
                )
                rejected = await client.post(
                    base + "/v1/chat/completions",
                    headers=headers,
                    json={**payload, "stream": False},
                )
                check("loss_blocks_new_admission", rejected.status_code == 503)
                check(
                    "not_ready_during_recovery",
                    (await client.get(base + "/health")).status_code == 503,
                )
                remainder = "\n".join([line async for line in iterator])
                check("old_stream_stops_without_done", "[DONE]" not in remainder)
            for _ in range(160):
                if (await client.get(base + "/health")).status_code == 200:
                    break
                await asyncio.sleep(0.1)
            else:
                raise RuntimeError("recovery_did_not_complete")
            elapsed = time.monotonic() - started
            check("recovery_wait_enforced", elapsed >= 8, seconds=round(elapsed, 3))
            recovered = await client.post(
                base + "/v1/chat/completions",
                headers=headers,
                json={
                    **payload,
                    "stream": False,
                    "messages": [{"role": "user", "content": "normal"}],
                },
            )
            check("request_after_recovery", recovered.status_code == 200)
            stats = (await client.get(f"http://127.0.0.1:{upstream_port}/stats")).json()
            check("upstream_closed", stats["active"] == 0)
        report["passed"] = True
    finally:
        try:
            await client_redis.shutdown(nosave=True)
        except Exception:
            pass
        await client_redis.aclose()
        for process in processes:
            stop_process(process)
        for log in logs:
            log.close()
        database.engine.dispose()
        database.auth_engine.dispose()
        (ROOT / "reports/coordination-restart.json").write_text(
            json.dumps(report, indent=2), "utf-8"
        )
    print(json.dumps(report))


if __name__ == "__main__":
    asyncio.run(main())

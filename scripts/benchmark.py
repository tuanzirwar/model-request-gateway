"""独立负载实验：真实网关和数据库，上游为无推理HTTP替身。"""

import argparse
import asyncio
import json
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path

import httpx
import yaml
from prometheus_client.parser import text_string_to_metric_families
from sqlalchemy.orm import Session
from verify_e2e import port, stop_process

from model_gateway.db import Application, Database, key_hash

ROOT = Path(__file__).resolve().parent.parent
KEY = "benchmark-only-0123456789abcdefghijklmnop"
MONITOR_KEY = "benchmark-monitor-only-0123456789abcdefghijklmnop"


def percentile(values, fraction):
    import math

    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * fraction) - 1)] if ordered else None


def profile_delta(before, after):
    def samples(raw):
        return {
            (sample.name, tuple(sorted(sample.labels.items()))): sample.value
            for family in text_string_to_metric_families(raw)
            for sample in family.samples
        }

    first, last = samples(before), samples(after)
    profile = []
    for (name, labels), count in last.items():
        if name != "gateway_stage_seconds_count":
            continue
        delta_count = count - first.get((name, labels), 0)
        delta_sum = last.get(("gateway_stage_seconds_sum", labels), 0) - first.get(
            ("gateway_stage_seconds_sum", labels), 0
        )
        if delta_count:
            profile.append(
                {
                    **dict(labels),
                    "count": int(delta_count),
                    "mean_ms": round(delta_sum / delta_count * 1000, 3),
                }
            )
    return sorted(profile, key=lambda row: row["mean_ms"], reverse=True)


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--concurrency", type=int, nargs="+", default=[1, 4, 8])
    parser.add_argument("--seconds", type=float, default=5)
    parser.add_argument("--repeats", type=int, default=2)
    parser.add_argument("--db-workers", type=int, default=4)
    args = parser.parse_args()
    if min(args.concurrency) < 1 or args.seconds <= 0 or args.repeats < 1 or args.db_workers < 1:
        parser.error("负载参数必须为正")
    upstream, gateway = port(), port()
    config = yaml.safe_load((ROOT / "gateway.yaml").read_text("utf-8"))
    config["database_url"] = config["database_url"].replace(
        "/model_gateway?", "/model_gateway_test?"
    )
    config["namespace"] = "benchmark-" + uuid.uuid4().hex
    config["db_workers"] = args.db_workers
    config["models"] = {
        "fixture": {
            "model": "fixture",
            "endpoint": f"http://127.0.0.1:{upstream}/v1/chat/completions",
            "concurrency": 16,
        }
    }
    target = ROOT / ".local/benchmark.yaml"
    target.write_text(yaml.safe_dump(config), "utf-8")
    database = Database(config["database_url"])
    with Session(database.engine) as session, session.begin():
        app = session.get(Application, "benchmark")
        if app is None:
            app = Application(id="benchmark")
            session.add(app)
        app.key_hash, app.enabled, app.model_allowlist, app.concurrency = (
            key_hash(KEY),
            True,
            ["fixture"],
            16,
        )
    processes, logs = [], []
    result = {
        "passed": False,
        "mode": "real HTTP gateway/MySQL/Redis; non-streaming fixture upstream, no model inference",
        "workers": 1,
        "concurrency_limit": 16,
        "seconds_per_round": args.seconds,
        "repeats": args.repeats,
        "db_workers": args.db_workers,
        "closed_loop": True,
        "rounds": [],
        "limitations": [
            "same host and short rounds",
            "not maximum sustained or model QPS",
            "records accumulate between rounds",
            "no optimization before/after comparison",
        ],
    }
    try:
        for module, server_port, factory in (
            ("scripts.upstream_fixture:app", upstream, False),
            ("model_gateway.app:create_app", gateway, True),
        ):
            log = (ROOT / "reports" / f"benchmark-server-{server_port}.txt").open(
                "w", encoding="utf-8"
            )
            logs.append(log)
            server_args = [
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
                server_args.append("--factory")
            processes.append(
                subprocess.Popen(
                    server_args,
                    cwd=ROOT,
                    env={
                        **os.environ,
                        "GATEWAY_CONFIG": str(target),
                        "GATEWAY_METRICS_KEY": MONITOR_KEY,
                    },
                    stdout=log,
                    stderr=log,
                    creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                )
            )
        async with httpx.AsyncClient(trust_env=False, timeout=10) as client:
            for url in (f"http://127.0.0.1:{upstream}/stats", f"http://127.0.0.1:{gateway}/health"):
                for _ in range(100):
                    try:
                        if (await client.get(url)).status_code == 200:
                            break
                    except httpx.HTTPError:
                        pass
                    await asyncio.sleep(0.1)
                else:
                    raise AssertionError("服务未就绪")

        async def round_trip(client):
            started = time.monotonic()
            try:
                response = await client.post(
                    f"http://127.0.0.1:{gateway}/v1/chat/completions",
                    headers={"Authorization": "Bearer " + KEY},
                    json={
                        "model": "fixture",
                        "messages": [{"role": "user", "content": "normal"}],
                        "stream": False,
                    },
                )
                return response.status_code, (time.monotonic() - started) * 1000
            except httpx.HTTPError:
                return "network_error", (time.monotonic() - started) * 1000

        async def scrape():
            async with httpx.AsyncClient(trust_env=False, timeout=10) as client:
                response = await client.get(
                    f"http://127.0.0.1:{gateway}/metrics",
                    headers={"Authorization": "Bearer " + MONITOR_KEY},
                )
                response.raise_for_status()
                return response.text

        for concurrency in args.concurrency:
            for repeat in range(1, args.repeats + 1):
                samples = []
                gate = asyncio.Event()
                window = {}
                ready = [asyncio.Event() for _ in range(concurrency)]

                async def worker(index, gate=gate, samples=samples, window=window, ready=ready):
                    async with httpx.AsyncClient(trust_env=False, timeout=10) as client:
                        for _ in range(3):
                            await round_trip(client)
                        ready[index].set()
                        await gate.wait()
                        while time.monotonic() < window["deadline"]:
                            samples.append(await round_trip(client))

                tasks = [asyncio.create_task(worker(index)) for index in range(concurrency)]
                await asyncio.gather(*[event.wait() for event in ready])
                before_metrics = await scrape()
                started = time.monotonic()
                window["deadline"] = started + args.seconds
                gate.set()
                await asyncio.gather(*tasks)
                elapsed = time.monotonic() - started
                after_metrics = await scrape()
                successful = [latency for code, latency in samples if code == 200]
                counts = {}
                for code, _ in samples:
                    counts[str(code)] = counts.get(str(code), 0) + 1
                row = {
                    "concurrency": concurrency,
                    "repeat": repeat,
                    "requests": len(samples),
                    "successful": len(successful),
                    "elapsed_seconds": round(elapsed, 4),
                    "successful_qps": round(len(successful) / elapsed, 2),
                    "p95_ms": round(percentile(successful, 0.95), 2) if successful else None,
                    "status_counts": counts,
                    "stage_profile": profile_delta(before_metrics, after_metrics),
                }
                result["rounds"].append(row)
                (ROOT / "reports/benchmark.json").write_text(
                    json.dumps(result, ensure_ascii=False, indent=2), "utf-8"
                )
                print(json.dumps(row), flush=True)
        result["passed"] = all(row["successful"] == row["requests"] for row in result["rounds"])
        (ROOT / "reports/benchmark-metrics.txt").write_text(after_metrics, "utf-8")
    finally:
        for process in processes:
            stop_process(process)
        for log in logs:
            log.close()
        database.engine.dispose()
        (ROOT / "reports/benchmark.json").write_text(
            json.dumps(result, ensure_ascii=False, indent=2), "utf-8"
        )


if __name__ == "__main__":
    asyncio.run(main())

"""独立负载实验：真实网关和数据库，上游为无推理HTTP替身。"""

import argparse
import asyncio
import hashlib
import json
import os
import subprocess
import sys
import time
import tomllib
import uuid
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

import httpx
import yaml
from load_client import RawClient
from prometheus_client.parser import text_string_to_metric_families
from sqlalchemy import func, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session
from verify_e2e import port, stop_process

from model_gateway.db import Application, Database, RequestRecord, key_hash

ROOT = Path(__file__).resolve().parent.parent
KEY = "benchmark-only-0123456789abcdefghijklmnop"
MONITOR_KEY = "benchmark-monitor-only-0123456789abcdefghijklmnop"


def runtime_packages():
    result = {}
    for name in ("uvicorn", "httpx", "sqlalchemy", "pymysql", "redis", "httptools", "hiredis"):
        try:
            result[name] = version(name)
        except PackageNotFoundError:
            result[name] = None
    return result


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
    parser.add_argument("--requests", type=int, default=0)
    parser.add_argument("--concurrency-limit", type=int, default=16)
    parser.add_argument("--output", default="reports/benchmark.json")
    parser.add_argument("--max-seconds", type=float, default=1800)
    parser.add_argument("--route", choices=["chat", "models", "live", "requests"], default="chat")
    parser.add_argument("--response-bytes", type=int, default=0)
    parser.add_argument("--gateway-workers", type=int, default=1)
    parser.add_argument("--max-inflight", type=int, default=128)
    parser.add_argument("--connections", type=int, default=100)
    parser.add_argument("--db-batch-size", type=int, default=64)
    parser.add_argument("--db-batch-seconds", type=float, default=0)
    parser.add_argument("--redis-batch-size", type=int, default=64)
    parser.add_argument("--http-parser", choices=["auto", "h11", "httptools"], default="auto")
    parser.add_argument("--direct-upstream", action="store_true")
    parser.add_argument("--client", choices=["httpx", "raw"], default="httpx")
    parser.add_argument(
        "--scenario",
        choices=["normal", "invalid-key", "oversized-key", "oversized-body"],
        default="normal",
    )
    args = parser.parse_args()
    if (
        min(args.concurrency) < 1
        or args.seconds <= 0
        or args.repeats < 1
        or not 1 <= args.db_workers <= 64
        or args.requests < 0
        or args.concurrency_limit < 1
        or args.max_seconds <= 0
        or not 0 <= args.response_bytes <= 1040000
        or not 1 <= args.gateway_workers <= 32
        or args.gateway_workers * (args.db_workers + min(args.db_workers, 2)) > 128
        or not 1 <= args.max_inflight <= 4096
        or not 1 <= args.connections <= 4096
        or not 1 <= args.db_batch_size <= 256
        or not 1 <= args.redis_batch_size <= 256
        or not 0 <= args.db_batch_seconds <= 0.05
    ):
        parser.error("负载参数必须为正")
    output = (ROOT / args.output).resolve()
    if not output.is_relative_to(ROOT / "reports"):
        parser.error("报告必须位于项目reports目录")
    output.parent.mkdir(parents=True, exist_ok=True)
    if args.direct_upstream and (args.route != "chat" or args.scenario != "normal"):
        parser.error("上游基线仅支持正常chat请求")
    upstream, gateway = port(), port()
    gateways = [gateway] + [port() for _ in range(args.gateway_workers - 1)]
    config = yaml.safe_load((ROOT / "gateway.yaml").read_text("utf-8"))
    config["database_url"] = config["database_url"].replace(
        "/model_gateway?", "/model_gateway_test?"
    )
    url = make_url(config["database_url"])
    if url.host not in {"127.0.0.1", "localhost"} or url.database != "model_gateway_test":
        parser.error("高压实验仅允许本机model_gateway_test专用库")
    config["namespace"] = "benchmark-" + uuid.uuid4().hex
    config["db_workers"] = args.db_workers
    config["max_inflight_requests"] = args.max_inflight
    config["upstream_connections"] = args.connections
    config["db_batch_size"] = args.db_batch_size
    config["db_batch_seconds"] = args.db_batch_seconds
    config["redis_batch_size"] = args.redis_batch_size
    config["models"] = {
        "fixture": {
            "model": "fixture",
            "endpoint": f"http://127.0.0.1:{upstream}/v1/chat/completions",
            "concurrency": args.concurrency_limit,
        }
    }
    target = ROOT / (".local/" + config["namespace"] + ".yaml")
    target.write_text(yaml.safe_dump(config), "utf-8")
    database = Database(config["database_url"])
    benchmark_id = config["namespace"]
    app_key = KEY + "-" + uuid.uuid4().hex
    with Session(database.engine) as session, session.begin():
        app = Application(id=benchmark_id)
        session.add(app)
        app.key_hash, app.enabled, app.model_allowlist, app.concurrency = (
            key_hash(app_key),
            True,
            ["fixture"],
            args.concurrency_limit,
        )

    def row_count():
        with Session(database.engine) as session:
            return session.scalar(select(func.count()).select_from(RequestRecord))

    def database_runtime():
        with database.engine.connect() as connection:
            return dict(
                connection.execute(
                    text(
                        "SELECT @@innodb_buffer_pool_size AS buffer_bytes, "
                        "@@innodb_flush_log_at_trx_commit AS flush_log, "
                        "@@sync_binlog AS sync_binlog, @@max_connections AS max_connections"
                    )
                )
                .mappings()
                .one()
            )

    processes, logs = [], []
    result = {
        "passed": False,
        "verification_completed": False,
        "source_sha256": {
            name: hashlib.sha256((ROOT / "src/model_gateway" / name).read_bytes()).hexdigest()
            for name in (
                "app.py",
                "db.py",
                "quota.py",
                "batching.py",
                "config.py",
            )
        },
        "mode": "direct fixture HTTP; gateway bypassed"
        if args.direct_upstream
        else "real HTTP gateway/MySQL/Redis; non-streaming fixture upstream, no model inference",
        "app_id": benchmark_id,
        "workers": args.gateway_workers,
        "max_inflight_per_worker": args.max_inflight,
        "connections_per_worker": args.connections,
        "upstream_keepalive_seconds": config.get("upstream_keepalive_seconds", 2),
        "total_db_connections_limit": (args.db_workers + min(args.db_workers, 2))
        * args.gateway_workers,
        "auth_pool_size_per_worker": min(args.db_workers, 2),
        "concurrency_limit": args.concurrency_limit,
        "requests_per_round": args.requests,
        "route": args.route,
        "direct_upstream": args.direct_upstream,
        "scenario": args.scenario,
        "client": args.client,
        "response_bytes_requested": args.response_bytes,
        "database_rows_before": row_count(),
        "database_runtime_before": database_runtime(),
        "seconds_per_round": args.seconds,
        "repeats": args.repeats,
        "db_workers": args.db_workers,
        "runtime_packages": runtime_packages(),
        "http_parser": args.http_parser,
        "source_version": tomllib.loads((ROOT / "pyproject.toml").read_text("utf-8"))["project"][
            "version"
        ],
        "db_batch_size": args.db_batch_size,
        "db_batch_seconds": args.db_batch_seconds,
        "redis_batch_size": args.redis_batch_size,
        "closed_loop": True,
        "rounds": [],
        "limitations": [
            "same host and short rounds",
            "not maximum sustained or model QPS",
            "records accumulate between rounds",
            "different configurations are not isolated causal comparisons",
            "body bandwidth excludes HTTP/TCP/TLS overhead and is measured on loopback",
            "multiple workers use client-side round-robin, not a deployed load balancer",
        ],
    }
    try:
        servers = [("scripts.upstream_fixture:app", upstream, False)] + [
            ("model_gateway.app:create_app", server_port, True) for server_port in gateways
        ]
        for module, server_port, factory in servers:
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
                "--http",
                args.http_parser,
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
                        "GATEWAY_DATABASE_URL": config["database_url"],
                        "GATEWAY_REDIS_URL": config["redis_url"],
                        "GATEWAY_METRICS_KEY": MONITOR_KEY,
                    },
                    stdout=log,
                    stderr=log,
                    creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                )
            )
        async with httpx.AsyncClient(trust_env=False, timeout=10) as client:
            for url in [f"http://127.0.0.1:{upstream}/stats"] + [
                f"http://127.0.0.1:{server_port}/health" for server_port in gateways
            ]:
                for _ in range(100):
                    try:
                        if (await client.get(url)).status_code == 200:
                            break
                    except httpx.HTTPError:
                        pass
                    await asyncio.sleep(0.1)
                else:
                    raise AssertionError("服务未就绪")

        payload = json.dumps(
            {
                "model": "fixture",
                "messages": [
                    {
                        "role": "user",
                        "content": f"size:{args.response_bytes}"
                        if args.response_bytes
                        else "normal",
                    }
                ],
                "stream": False,
            }
        ).encode()
        if args.scenario == "oversized-body":
            payload = b"x" * (config.get("max_body_bytes", 1048576) + 1)

        async def round_trip(client, worker_index=0):
            gateway = gateways[worker_index % len(gateways)]
            if args.direct_upstream:
                gateway = upstream
            started = time.monotonic()
            try:
                headers = {"Authorization": "Bearer " + app_key}
                if args.scenario == "invalid-key":
                    headers["Authorization"] = "Bearer invalid-pressure-key"
                elif args.scenario == "oversized-key":
                    headers["Authorization"] = "Bearer " + "x" * 513
                if args.route == "chat":
                    response = await client.post(
                        f"http://127.0.0.1:{gateway}/v1/chat/completions",
                        headers={**headers, "Content-Type": "application/json"},
                        content=payload,
                    )
                else:
                    path = {"models": "/v1/models", "live": "/live", "requests": "/requests"}[
                        args.route
                    ]
                    response = await client.get(
                        f"http://127.0.0.1:{gateway}{path}", headers=headers
                    )
                return (
                    response.status_code,
                    (time.monotonic() - started) * 1000,
                    len(response.content),
                )
            except httpx.HTTPError:
                return "network_error", (time.monotonic() - started) * 1000, 0

        async def scrape():
            async with httpx.AsyncClient(trust_env=False, timeout=10) as client:
                values = []
                for gateway in gateways:
                    response = await client.get(
                        f"http://127.0.0.1:{gateway}/metrics",
                        headers={"Authorization": "Bearer " + MONITOR_KEY},
                    )
                    response.raise_for_status()
                    values.append(response.text)
                return values

        def combined_profile(before, after):
            combined = {}
            for first, last in zip(before, after, strict=True):
                for item in profile_delta(first, last):
                    key = (item["operation"], item["phase"])
                    count, total = combined.get(key, (0, 0))
                    combined[key] = (count + item["count"], total + item["count"] * item["mean_ms"])
            return sorted(
                [
                    {
                        "operation": operation,
                        "phase": phase,
                        "count": count,
                        "mean_ms": round(total / count, 3),
                    }
                    for (operation, phase), (count, total) in combined.items()
                ],
                key=lambda item: item["mean_ms"],
                reverse=True,
            )

        for concurrency in args.concurrency:
            for repeat in range(1, args.repeats + 1):
                samples = []
                gate = asyncio.Event()
                window = {}
                ready = [asyncio.Event() for _ in range(concurrency)]
                issued = 0

                async def worker(index, gate=gate, samples=samples, window=window, ready=ready):
                    nonlocal issued
                    context = (
                        RawClient(timeout=10)
                        if args.client == "raw"
                        else httpx.AsyncClient(trust_env=False, timeout=10)
                    )
                    async with context as client:
                        for _ in range(3):
                            await round_trip(client, index)
                        ready[index].set()
                        await gate.wait()
                        while time.monotonic() < window["deadline"]:
                            if args.requests and issued >= args.requests:
                                break
                            issued += 1
                            samples.append(await round_trip(client, index))

                async def progress(samples=samples, concurrency=concurrency):
                    while True:
                        await asyncio.sleep(10)
                        print(
                            json.dumps(
                                {
                                    "progress": len(samples),
                                    "target": args.requests,
                                    "concurrency": concurrency,
                                }
                            ),
                            flush=True,
                        )

                tasks = [asyncio.create_task(worker(index)) for index in range(concurrency)]
                await asyncio.gather(*[event.wait() for event in ready])
                before_metrics = await scrape()
                started = time.monotonic()
                window["deadline"] = started + (args.max_seconds if args.requests else args.seconds)
                gate.set()
                observer = asyncio.create_task(progress())
                try:
                    await asyncio.gather(*tasks)
                finally:
                    observer.cancel()
                    await asyncio.gather(observer, return_exceptions=True)
                elapsed = time.monotonic() - started
                after_metrics = await scrape()
                successful = [latency for code, latency, _ in samples if code == 200]
                counts = {}
                for code, _, _ in samples:
                    counts[str(code)] = counts.get(str(code), 0) + 1
                row = {
                    "concurrency": concurrency,
                    "repeat": repeat,
                    "requests": len(samples),
                    "successful": len(successful),
                    "elapsed_seconds": round(elapsed, 4),
                    "successful_qps": round(len(successful) / elapsed, 2),
                    "attempted_qps": round(len(samples) / elapsed, 2),
                    "completed_target": not args.requests or len(samples) == args.requests,
                    "response_body_bytes": sum(size for _, _, size in samples),
                    "response_body_mbps": round(
                        sum(size for _, _, size in samples) * 8 / elapsed / 1e6, 3
                    ),
                    "request_body_bytes": len(payload) * len(samples)
                    if args.route == "chat"
                    else 0,
                    "p95_ms": round(percentile(successful, 0.95), 2) if successful else None,
                    "p99_ms": round(percentile(successful, 0.99), 2) if successful else None,
                    "status_counts": counts,
                    "expected_status": {
                        "normal": 200,
                        "invalid-key": 401,
                        "oversized-key": 401,
                        "oversized-body": 413,
                    }[args.scenario],
                    "stage_profile": combined_profile(before_metrics, after_metrics),
                }
                result["rounds"].append(row)
                output.write_text(json.dumps(result, ensure_ascii=False, indent=2), "utf-8")
                print(json.dumps(row), flush=True)
        result["passed"] = all(
            row["status_counts"].get(str(row["expected_status"]), 0) == row["requests"]
            and row["completed_target"]
            for row in result["rounds"]
        )
        result["database_rows_after"] = row_count()
        result["database_runtime_after"] = database_runtime()
        result["recovery_checks"] = []
        async with httpx.AsyncClient(trust_env=False, timeout=10) as client:
            for gateway in gateways:
                for path in ("/live", "/health", "/v1/models"):
                    response = await client.get(
                        f"http://127.0.0.1:{gateway}{path}",
                        headers={"Authorization": "Bearer " + app_key},
                    )
                    result["recovery_checks"].append({"path": path, "status": response.status_code})
        result["recovered"] = all(item["status"] == 200 for item in result["recovery_checks"])
        with Session(database.engine) as session:
            result["app_terminal_counts"] = [
                dict(row)
                for row in session.execute(
                    select(RequestRecord.status, func.count().label("count"))
                    .where(RequestRecord.app_id == benchmark_id)
                    .group_by(RequestRecord.status)
                ).mappings()
            ]
            # 先用已有应用/状态索引覆盖统计，再只对失败行回表读取错误码。
            # 大应用的成功行不需要为错误码做逐行随机I/O。
            result["app_error_counts"] = [
                dict(row)
                for row in session.execute(
                    select(RequestRecord.status, RequestRecord.error, func.count().label("count"))
                    .where(
                        RequestRecord.app_id == benchmark_id,
                        RequestRecord.status.in_(("failed", "rejected", "cancelled", "abandoned")),
                    )
                    .group_by(RequestRecord.status, RequestRecord.error)
                ).mappings()
            ]
        result["no_nonterminal_records"] = not any(
            row["status"] in {"accepted", "running"} for row in result["app_terminal_counts"]
        )
        result["passed"] = result["passed"] and result["no_nonterminal_records"]
        result["source_unchanged_during_run"] = all(
            hashlib.sha256((ROOT / "src/model_gateway" / name).read_bytes()).hexdigest() == digest
            for name, digest in result["source_sha256"].items()
        )
        result["verification_completed"] = True
        result["passed"] = result["passed"] and result["recovered"]
        output.with_suffix(".metrics.txt").write_text("\n".join(after_metrics), "utf-8")
    finally:
        for process in processes:
            stop_process(process)
        for log in logs:
            log.close()
        database.engine.dispose()
        output.write_text(json.dumps(result, ensure_ascii=False, indent=2), "utf-8")


if __name__ == "__main__":
    asyncio.run(main())

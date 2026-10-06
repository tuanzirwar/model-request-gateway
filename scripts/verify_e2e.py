"""真实双进程HTTP/MySQL/Redis验收；上游替身仅用于可控故障。"""

import asyncio
import json
import os
import socket
import subprocess
import sys
import time
import uuid
from pathlib import Path

import httpx
import yaml
from sqlalchemy.orm import Session

from model_gateway.config import load_settings
from model_gateway.db import Application, Database, key_hash

ROOT = Path(__file__).resolve().parent.parent
REPORT = ROOT / "reports/http-e2e.json"
KEY = "local-e2e-only-0123456789abcdefghijklmnop"
OTHER_KEY = "local-other-only-0123456789abcdefghijklmnop"
MONITOR_KEY = "e2e-monitor-only-0123456789abcdefghijklmnop"


def port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def stop_process(process):
    # Windows虚拟环境启动器可能创建子进程，故障注入需终止本次启动的整棵进程树。
    if process.poll() is None:
        if os.name == "nt":
            subprocess.run(["taskkill", "/PID", str(process.pid), "/T", "/F"], capture_output=True)
        else:
            process.kill()
        process.wait(timeout=5)


async def main():
    REPORT.parent.mkdir(exist_ok=True)
    (ROOT / ".local").mkdir(exist_ok=True)
    upstream, one, two = port(), port(), port()
    config = yaml.safe_load((ROOT / "gateway.yaml").read_text("utf-8"))
    config["database_url"] = config["database_url"].replace(
        "/model_gateway?", "/model_gateway_test?"
    )
    namespace = "gateway-e2e-" + uuid.uuid4().hex
    config.update(
        namespace=namespace, lease_seconds=2, total_seconds=6, first_seconds=1.5, idle_seconds=1.5
    )
    config["models"] = {
        "fixture": {
            "model": "fixture",
            "endpoint": f"http://127.0.0.1:{upstream}/v1/chat/completions",
            "concurrency": 2,
        }
    }
    target = ROOT / ".local/e2e.yaml"
    target.write_text(yaml.safe_dump(config), "utf-8")
    env = {
        **os.environ,
        "GATEWAY_CONFIG": str(target),
        "PYTHONIOENCODING": "utf-8",
        "GATEWAY_METRICS_KEY": MONITOR_KEY,
    }
    os.environ["GATEWAY_CONFIG"] = str(target)
    database = Database(load_settings().database_url)
    subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"], cwd=ROOT, env=env, check=True
    )
    app_id = "e2e-" + uuid.uuid4().hex[:12]
    with Session(database.engine) as session, session.begin():
        # 测试专用固定密钥对应的应用可重复更新，不接触演示库。
        from sqlalchemy import select

        for suffix, key in (("a", KEY), ("b", OTHER_KEY)):
            row = session.scalar(select(Application).where(Application.key_hash == key_hash(key)))
            if row is None:
                row = Application(id=app_id + suffix, key_hash=key_hash(key))
                session.add(row)
            row.enabled, row.model_allowlist, row.concurrency = True, ["fixture"], 2
    processes, logs = [], []
    result = {
        "passed": False,
        "database": "MySQL",
        "quota": "real Redis",
        "transport": "real TCP HTTP; two independent gateway processes",
        "upstream": "controlled SSE fixture, no model quality claim",
        "checks": [],
    }

    def start(module, server_port, factory=False, config_path=None):
        log = (ROOT / "reports" / f"e2e-server-{server_port}.txt").open("w", encoding="utf-8")
        logs.append(log)
        args = [
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
            args.append("--factory")
        process = subprocess.Popen(
            args,
            cwd=ROOT,
            env={**env, **({"GATEWAY_CONFIG": str(config_path)} if config_path else {})},
            stdout=log,
            stderr=log,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        processes.append(process)
        return process

    def check(name, condition, **details):
        row = {"name": name, "passed": bool(condition), **details}
        result["checks"].append(row)
        REPORT.write_text(json.dumps(result, ensure_ascii=False, indent=2), "utf-8")
        print(json.dumps(row, ensure_ascii=False), flush=True)
        assert condition, row

    headers = {"Authorization": "Bearer " + KEY}
    bases = [f"http://127.0.0.1:{one}", f"http://127.0.0.1:{two}"]

    def payload(mode, stream=True):
        return {
            "model": "fixture",
            "messages": [{"role": "user", "content": mode}],
            "stream": stream,
        }

    async def terminal(client, request_id, timeout=15):
        stop = time.monotonic() + timeout
        while time.monotonic() < stop:
            row = (await client.get(bases[1] + "/requests/" + request_id, headers=headers)).json()
            if row["status"] not in ("accepted", "running"):
                return row
            await asyncio.sleep(0.1)
        raise AssertionError("请求未进入终态")

    try:
        start("scripts.upstream_fixture:app", upstream)
        p1 = start("model_gateway.app:create_app", one, True)
        start("model_gateway.app:create_app", two, True)
        async with httpx.AsyncClient(timeout=20, trust_env=False) as client:
            for _ in range(100):
                try:
                    if (await client.get(f"http://127.0.0.1:{upstream}/stats")).status_code == 200:
                        break
                except httpx.HTTPError:
                    pass
                await asyncio.sleep(0.1)
            else:
                raise AssertionError("故障注入上游未就绪")
            for base in bases:
                for _ in range(100):
                    try:
                        if (await client.get(base + "/health")).status_code == 200:
                            break
                    except httpx.HTTPError:
                        pass
                    await asyncio.sleep(0.1)
                else:
                    raise AssertionError("服务未就绪")
            unauthorized = await client.post(
                bases[0] + "/v1/chat/completions", json=payload("normal")
            )
            check("unauthorized_401", unauthorized.status_code == 401)
            forbidden = await client.post(
                bases[0] + "/v1/chat/completions",
                headers=headers,
                json={**payload("normal"), "model": "not-authorized"},
            )
            check("model_allowlist_403", forbidden.status_code == 403)
            oversized = await client.post(
                bases[0] + "/v1/chat/completions", headers=headers, content=b"x" * 1100000
            )
            check("bounded_request_413", oversized.status_code == 413)
            response = await client.post(
                bases[0] + "/v1/chat/completions", headers=headers, json=payload("normal", False)
            )
            row = await terminal(client, response.headers["X-Request-ID"])
            check(
                "nonstream_result_and_usage",
                response.status_code == 200
                and row["status"] == "succeeded"
                and row["usage"]["prompt_tokens"] == 5,
            )
            foreign = await client.get(
                bases[1] + "/requests/" + row["id"],
                headers={"Authorization": "Bearer " + OTHER_KEY},
            )
            check("record_ownership_404", foreign.status_code == 404)
            console = await client.get(bases[0] + "/")
            check(
                "console_without_embedded_credentials",
                console.status_code == 200
                and KEY not in console.text
                and "unsafe-inline" not in console.headers["content-security-policy"],
            )
            check(
                "console_asset_available",
                (await client.get(bases[0] + "/static/console.js")).status_code == 200,
            )
            check(
                "metrics_separate_key",
                (await client.get(bases[0] + "/metrics", headers=headers)).status_code == 401,
            )
            check(
                "stats_requires_app_key", (await client.get(bases[0] + "/stats")).status_code == 401
            )
            stats = (await client.get(bases[0] + "/stats", headers=headers)).json()
            check(
                "mysql_statistics",
                stats["total_requests"] >= 1
                and any(group["status"] == "succeeded" for group in stats["groups"]),
            )
            filtered = (
                await client.get(
                    bases[0] + "/requests?status=succeeded&model=fixture", headers=headers
                )
            ).json()
            check(
                "mysql_filtered_records",
                bool(filtered["items"])
                and all(
                    item["status"] == "succeeded" and item["model"] == "fixture"
                    for item in filtered["items"]
                ),
            )

            holds = []
            for base in bases:
                context = client.stream(
                    "POST",
                    base + "/v1/chat/completions",
                    headers=headers,
                    json=payload("hold:" + base.rsplit(":", 1)[-1]),
                )
                response = await context.__aenter__()
                holds.append((context, response))
            limited = await client.post(
                bases[1] + "/v1/chat/completions", headers=headers, json=payload("normal")
            )
            check(
                "two_process_shared_limit_429",
                all(response.status_code == 200 for _, response in holds)
                and limited.status_code == 429,
            )
            metrics = await client.get(
                bases[0] + "/metrics", headers={"Authorization": "Bearer " + MONITOR_KEY}
            )
            check(
                "metrics_reports_active_and_stages",
                metrics.status_code == 200
                and "gateway_active_requests 1.0" in metrics.text
                and "gateway_stage_seconds" in metrics.text
                and KEY not in metrics.text,
            )
            for context, response in holds:
                await context.__aexit__(None, None, None)
                row = await terminal(client, response.headers["X-Request-ID"])
                check("client_cancel_terminal", row["status"] == "cancelled")
            response = await client.post(
                bases[1] + "/v1/chat/completions", headers=headers, json=payload("normal")
            )
            check(
                "cancelled_quota_reusable",
                response.status_code == 200 and "[DONE]" in response.text,
            )
            from redis.asyncio import Redis

            from model_gateway.quota import Quota

            redis_client = Redis.from_url(config["redis_url"])
            context = client.stream(
                "POST",
                bases[0] + "/v1/chat/completions",
                headers=headers,
                json=payload("hold:lease_lost"),
            )
            response = await context.__aenter__()
            lease_request = response.headers["X-Request-ID"]
            quota = Quota(redis_client, namespace, 2)
            # 本次随机命名空间内删除该请求的模型租约，不影响其他服务。
            await redis_client.zrem(quota.keys(app_id + "a", "fixture")[0], lease_request)
            row = await terminal(client, lease_request)
            await context.__aexit__(None, None, None)
            await redis_client.aclose()
            check(
                "lost_lease_stops_stream",
                row["status"] == "failed" and row["error"] == "lease_lost",
            )
            for mode, expected_status in (
                ("truncated", 200),
                ("oversize", 502),
                ("http_error", 502),
                ("before_first", 504),
                ("idle", 200),
            ):
                response = await client.post(
                    bases[0] + "/v1/chat/completions", headers=headers, json=payload(mode)
                )
                row = await terminal(client, response.headers["X-Request-ID"])
                check(
                    mode + "_terminal_failure",
                    response.status_code == expected_status
                    and row["status"] == "failed"
                    and "private-provider-secret" not in response.text,
                    error=row["error"],
                )
                if expected_status == 200:
                    check(
                        mode + "_no_fake_done",
                        "[DONE]" not in response.text and '"error"' in response.text,
                    )
            # 真实TUI适配器验证文本、工具调用分段及usage协议，不修改用户配置。
            sys.path.insert(0, str(ROOT.parent / "src"))
            from tuicodingagent.models import ChatMessage, ProviderConfig, ProviderEventType
            from tuicodingagent.providers.openai import OpenAIProvider

            provider = OpenAIProvider(
                ProviderConfig(
                    "Gateway", "openai", "fixture", KEY, bases[1] + "/v1/chat/completions"
                )
            )
            try:
                events = [
                    event
                    async for event in provider.stream("system", [ChatMessage("user", "normal")])
                ]
                check(
                    "tui_text_and_usage",
                    "".join(
                        event.text for event in events if event.type == ProviderEventType.TEXT_DELTA
                    )
                    == "第一段第二段"
                    and any(event.type == ProviderEventType.USAGE for event in events),
                )
                events = [
                    event
                    async for event in provider.stream("system", [ChatMessage("user", "tool")])
                ]
                check(
                    "tui_tool_fragments",
                    any(event.type == ProviderEventType.TOOL_CALL for event in events),
                )
            finally:
                await provider.close()
            # 杀死一个专用测试网关进程；另一进程保留用于租约恢复和记录对账。
            context = client.stream(
                "POST",
                bases[0] + "/v1/chat/completions",
                headers=headers,
                json=payload("hold:crash"),
            )
            response = await context.__aenter__()
            lost_id = response.headers["X-Request-ID"]
            stop_process(p1)
            await context.__aexit__(None, None, None)
            await asyncio.sleep(2.2)
            next_response = await client.post(
                bases[1] + "/v1/chat/completions", headers=headers, json=payload("normal")
            )
            check("process_kill_lease_expires", next_response.status_code == 200)
            row = await terminal(client, lost_id)
            check("process_kill_record_reconciled", row["status"] == "abandoned")
            listing = (await client.get(bases[1] + "/requests?limit=2", headers=headers)).json()
            page_two = (
                await client.get(
                    bases[1] + "/requests",
                    headers=headers,
                    params={"limit": 2, "before": listing["next_cursor"]},
                )
            ).json()
            check(
                "cursor_pagination_no_duplicate",
                not {r["id"] for r in listing["items"]} & {r["id"] for r in page_two["items"]},
            )
            stats = (await client.get(f"http://127.0.0.1:{upstream}/stats")).json()
            for _ in range(20):
                if stats["active"] == 0:
                    break
                await asyncio.sleep(0.1)
                stats = (await client.get(f"http://127.0.0.1:{upstream}/stats")).json()
            result["upstream_stats"] = stats
            check("upstream_connections_released", stats["active"] == 0)
            unavailable = dict(config)
            unavailable["redis_url"] = f"redis://127.0.0.1:{port()}/0"
            bad_config = ROOT / ".local/redis-unavailable.yaml"
            bad_config.write_text(yaml.safe_dump(unavailable), "utf-8")
            bad_port = port()
            start("model_gateway.app:create_app", bad_port, True, bad_config)
            bad_base = f"http://127.0.0.1:{bad_port}"
            for _ in range(100):
                try:
                    if (await client.get(bad_base + "/openapi.json")).status_code == 200:
                        break
                except httpx.HTTPError:
                    pass
                await asyncio.sleep(0.1)
            rejected = await client.post(
                bad_base + "/v1/chat/completions", headers=headers, json=payload("normal")
            )
            new_stats = (await client.get(f"http://127.0.0.1:{upstream}/stats")).json()
            check(
                "redis_failure_closed_503",
                rejected.status_code == 503 and new_stats["started"] == stats["started"],
            )
            unavailable_db = dict(config)
            unavailable_db["database_url"] = (
                f"mysql+pymysql://gateway:invalid@127.0.0.1:{port()}/model_gateway_test"
            )
            db_config = ROOT / ".local/mysql-unavailable.yaml"
            db_config.write_text(yaml.safe_dump(unavailable_db), "utf-8")
            db_port = port()
            start("model_gateway.app:create_app", db_port, True, db_config)
            db_base = f"http://127.0.0.1:{db_port}"
            for _ in range(100):
                try:
                    if (await client.get(db_base + "/live")).status_code == 200:
                        break
                except httpx.HTTPError:
                    pass
                await asyncio.sleep(0.1)
            check(
                "live_independent_of_database",
                (await client.get(db_base + "/live")).status_code == 200,
            )
            check(
                "readiness_fails_when_database_down",
                (await client.get(db_base + "/health")).status_code == 503,
            )
            denied = await client.get(db_base + "/requests", headers=headers)
            check(
                "database_failure_sanitized_503",
                denied.status_code == 503
                and "invalid@" not in denied.text
                and "SELECT" not in denied.text,
            )
        result["passed"] = True
    finally:
        for process in processes:
            stop_process(process)
        for log in logs:
            log.close()
        database.engine.dispose()
        REPORT.write_text(json.dumps(result, ensure_ascii=False, indent=2), "utf-8")


if __name__ == "__main__":
    asyncio.run(main())

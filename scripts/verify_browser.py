"""独立Chromium、真实TCP及测试库验证工作台，不操作用户现有浏览器。"""

import asyncio
import json
import os
import subprocess
import sys
import uuid
from pathlib import Path

import httpx
import yaml
from playwright.async_api import async_playwright, expect
from sqlalchemy import select
from sqlalchemy.orm import Session
from verify_e2e import port, stop_process

from model_gateway.db import Application, Database, key_hash

ROOT = Path(__file__).resolve().parent.parent
KEY = "browser-test-only-0123456789abcdefghijklmnop"


async def main():
    os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", str(ROOT / ".local/browsers"))
    upstream, gateway = port(), port()
    config = yaml.safe_load((ROOT / "gateway.yaml").read_text("utf-8"))
    config["database_url"] = config["database_url"].replace(
        "/model_gateway?", "/model_gateway_test?"
    )
    config["namespace"] = "browser-" + uuid.uuid4().hex
    config["models"] = {
        "fixture": {
            "model": "fixture",
            "endpoint": f"http://127.0.0.1:{upstream}/v1/chat/completions",
            "concurrency": 2,
        }
    }
    target = ROOT / ".local/browser.yaml"
    target.write_text(yaml.safe_dump(config), "utf-8")
    database = Database(config["database_url"])
    with Session(database.engine) as session, session.begin():
        app = session.scalar(select(Application).where(Application.key_hash == key_hash(KEY)))
        if app is None:
            app = Application(id="browser-" + uuid.uuid4().hex[:16], key_hash=key_hash(KEY))
            session.add(app)
        app.enabled, app.model_allowlist, app.concurrency = True, ["fixture"], 2
    processes, logs = [], []
    report = {
        "passed": False,
        "transport": "Chromium -> real TCP gateway/MySQL/Redis -> SSE fixture",
        "checks": [],
        "model_quality_claim": False,
    }

    def checked(name):
        report["checks"].append({"name": name, "passed": True})

    try:
        for module, server_port, factory in (
            ("scripts.upstream_fixture:app", upstream, False),
            ("model_gateway.app:create_app", gateway, True),
        ):
            log = (ROOT / "reports" / f"browser-server-{server_port}.txt").open(
                "w", encoding="utf-8"
            )
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
            processes.append(
                subprocess.Popen(
                    command,
                    cwd=ROOT,
                    env={**os.environ, "GATEWAY_CONFIG": str(target)},
                    stdout=log,
                    stderr=log,
                    creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                )
            )
        base = f"http://127.0.0.1:{gateway}"
        headers = {"Authorization": "Bearer " + KEY}
        async with httpx.AsyncClient(trust_env=False, timeout=10) as client:
            for url in (f"http://127.0.0.1:{upstream}/stats", base + "/health"):
                for _ in range(100):
                    try:
                        if (await client.get(url)).status_code == 200:
                            break
                    except httpx.HTTPError:
                        pass
                    await asyncio.sleep(0.1)
                else:
                    raise AssertionError("浏览器验收服务未就绪")
            for _ in range(13):
                response = await client.post(
                    base + "/v1/chat/completions",
                    headers=headers,
                    json={"model": "fixture", "messages": [{"role": "user", "content": "normal"}]},
                )
                response.raise_for_status()
            async with async_playwright() as playwright:
                channel = os.environ.get("GATEWAY_BROWSER_CHANNEL") or None
                browser = await playwright.chromium.launch(headless=True, channel=channel)
                report["browser_version"] = browser.version
                report["browser_channel"] = channel or "bundled chromium"
                context = await browser.new_context(viewport={"width": 1360, "height": 960})
                page = await context.new_page()
                errors = []
                page.on("pageerror", lambda error: errors.append(str(error)))
                await page.goto(base)
                await page.locator("#key").fill("wrong")
                await page.locator("#login button").first.click()
                await expect(page.locator("#notice")).to_have_text("invalid_api_key")
                checked("invalid_key_rejected")
                await page.locator("#key").fill(KEY)
                await page.locator("#login button").first.click()
                await expect(page.locator("#start")).to_be_enabled()
                await expect(page.locator("#rows tr")).to_have_count(12)
                assert await page.locator("#key").input_value() == ""
                checked("login_models_and_records")
                first_ids = set(
                    await page.locator("#rows tr").evaluate_all(
                        "rows => rows.map(row => row.dataset.requestId)"
                    )
                )
                first_id = await page.locator("#rows tr").first.get_attribute("data-request-id")
                await page.locator("#more").click()
                await expect(page.locator("#rows tr").first).not_to_have_attribute(
                    "data-request-id", first_id
                )
                next_ids = set(
                    await page.locator("#rows tr").evaluate_all(
                        "rows => rows.map(row => row.dataset.requestId)"
                    )
                )
                assert next_ids and not first_ids.intersection(next_ids)
                checked("cursor_next_page")

                async def generate(mode):
                    await page.locator("#prompt").fill(mode)
                    await page.locator("#start").click()

                await generate("normal")
                await expect(page.locator("#state")).to_have_text("已完成")
                await expect(page.locator("#answer")).to_have_text("第一段第二段")
                await page.locator("#rows tr").first.click()
                await expect(page.locator("#detail")).to_contain_text('"status": "succeeded"')
                checked("stream_and_record_detail")
                await generate("hold:browser")
                await expect(page.locator("#answer")).to_have_text("第一段")
                request_id = await page.locator("#request-id").text_content()
                await page.locator("#stop").click()
                await expect(page.locator("#state")).to_have_text("已停止，保留部分输出")
                for _ in range(40):
                    record = (
                        await client.get(base + "/requests/" + request_id, headers=headers)
                    ).json()
                    if record["status"] == "cancelled":
                        break
                    await asyncio.sleep(0.1)
                assert record["status"] == "cancelled", record
                checked("stop_stream_and_cancelled_record")
                await generate("normal")
                await expect(page.locator("#state")).to_have_text("已完成")
                checked("generate_after_cancel")
                await generate("truncated")
                await expect(page.locator("#state")).to_contain_text("失败：upstream_incomplete")
                checked("truncated_stream_not_success")
                await generate("xss")
                await expect(page.locator("#state")).to_have_text("已完成")
                await expect(page.locator("#answer")).to_contain_text("<img")
                assert await page.locator("#answer img").count() == 0
                assert await page.evaluate("window.gatewayXss === undefined")
                checked("model_output_rendered_as_text")
                await page.locator("#status").select_option("cancelled")
                await expect(page.locator("#rows tr").first).to_contain_text("cancelled")
                checked("status_filter")
                await page.screenshot(path=ROOT / "reports/console-desktop.png", full_page=True)
                await page.set_viewport_size({"width": 390, "height": 844})
                await page.screenshot(path=ROOT / "reports/console-mobile.png", full_page=True)
                assert await page.evaluate(
                    "document.documentElement.scrollWidth <= window.innerWidth"
                )
                checked("mobile_without_horizontal_overflow")
                assert await page.evaluate(
                    "localStorage.length === 0 && sessionStorage.length === 0"
                )
                await page.locator("#logout").click()
                await expect(page.locator("#start")).to_be_disabled()
                await expect(page.locator("#rows tr")).to_have_count(0)
                await expect(page.locator("#answer")).to_have_text("")
                checked("logout_and_no_persisted_credentials")
                assert not errors, errors
                checked("no_uncaught_browser_error")
                await context.close()
                await browser.close()
            report["passed"] = True
    finally:
        for process in processes:
            stop_process(process)
        for log in logs:
            log.close()
        database.engine.dispose()
        (ROOT / "reports/browser-e2e.json").write_text(
            json.dumps(report, ensure_ascii=False, indent=2), "utf-8"
        )
    print(
        json.dumps(
            {"passed": report["passed"], "checks": len(report["checks"])}, ensure_ascii=False
        )
    )


if __name__ == "__main__":
    asyncio.run(main())

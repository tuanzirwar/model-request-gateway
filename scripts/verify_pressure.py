"""高压改动的本机完整回归，真实TUI使用独立测试应用和配置。"""

import argparse
import json
import os
import secrets
import subprocess
import sys
import time
import uuid
import xml.etree.ElementTree as ET
from pathlib import Path

import httpx
import yaml
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session
from verify_e2e import port, stop_process

from model_gateway.db import Application, Database, key_hash

ROOT = Path(__file__).resolve().parent.parent


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--real-model", action="store_true")
    parser.add_argument("--browser", action="store_true")
    args = parser.parse_args()
    config = yaml.safe_load((ROOT / "gateway.yaml").read_text("utf-8"))
    url = make_url(config["database_url"])
    if url.host not in {"localhost", "127.0.0.1"}:
        parser.error("仅允许本机专用测试环境")
    config["database_url"] = url.set(database="model_gateway_test").render_as_string(
        hide_password=False
    )
    env = dict(
        os.environ,
        GATEWAY_DATABASE_URL=config["database_url"],
        GATEWAY_TEST_MYSQL=config["database_url"],
        GATEWAY_REDIS_URL=config["redis_url"],
        GATEWAY_TEST_REDIS=config["redis_url"],
        PYTHONIOENCODING="utf-8",
    )
    if os.name == "nt":
        env.setdefault("GATEWAY_BROWSER_CHANNEL", "msedge")
    report = {
        "passed": False,
        "checks": [],
        "real_model": False,
        "browser": False,
        "scope": "local dedicated test database and Redis",
    }

    def run(name, command):
        command_env = dict(env)
        if command[0] in {"scripts/verify_e2e.py", "scripts/verify_browser.py"}:
            # 故障注入需要各子进程自己的配置，不能被测试入口的环境覆盖。
            command_env.pop("GATEWAY_DATABASE_URL", None)
            command_env.pop("GATEWAY_REDIS_URL", None)
        result = subprocess.run(
            [sys.executable, *command],
            cwd=ROOT,
            env=command_env,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        # 本机路径仅留在私有日志，公开回归日志不包含用户名和凭证。
        output = (result.stdout + result.stderr).replace(str(ROOT), "<workspace>")
        output = output.replace(str(ROOT).replace("\\", "/"), "<workspace>")
        for private, label in (
            (str(ROOT.parent), "<host-project>"),
            (str(Path.home()), "<user-home>"),
        ):
            output = output.replace(private, label).replace(private.replace("\\", "/"), label)
        (ROOT / "reports" / f"pressure-{name}.txt").write_text(output, "utf-8")
        report["checks"].append({"name": name, "exit_code": result.returncode})
        print(json.dumps(report["checks"][-1]), flush=True)
        if result.returncode:
            raise RuntimeError(f"回归失败：{name}，查看脱敏日志")

    database = None
    process = None
    log = None
    try:
        run("migration-upgrade", ["-m", "alembic", "upgrade", "head"])
        run(
            "tests",
            [
                "-m",
                "pytest",
                "-q",
                "--basetemp",
                ".local/pressure-final-tests",
                "--junitxml",
                "reports/pressure-junit.xml",
            ],
        )
        junit = ROOT / "reports/pressure-junit.xml"
        tree = ET.parse(junit)
        for node in tree.iter():
            node.attrib.pop("hostname", None)
        tree.write(junit, encoding="utf-8", xml_declaration=True)
        run("migrations", ["scripts/verify_migrations.py"])
        run("http-e2e", ["scripts/verify_e2e.py"])
        if args.browser:
            run("browser", ["scripts/verify_browser.py"])
            report["browser"] = True
        if args.real_model:
            app_id = "pressure-real-" + uuid.uuid4().hex[:16]
            key = secrets.token_urlsafe(32)
            config["namespace"] = app_id
            directory = ROOT / ".local" / app_id
            directory.mkdir()
            server_config = directory / "gateway.yaml"
            server_config.write_text(yaml.safe_dump(config), "utf-8")
            env["GATEWAY_CONFIG"] = str(server_config)
            database = Database(config["database_url"])
            with Session(database.engine) as session, session.begin():
                session.add(
                    Application(
                        id=app_id,
                        key_hash=key_hash(key),
                        model_allowlist=["coding"],
                        enabled=True,
                        concurrency=2,
                    )
                )
            server_port = port()
            tui = {
                "providers": [
                    {
                        "name": "Pressure regression",
                        "protocol": "openai",
                        "model": "coding",
                        "base_url": f"http://127.0.0.1:{server_port}/v1/chat/completions",
                        "api_key": key,
                        "wire_api": "chat_completions",
                        "thinking": False,
                    }
                ]
            }
            tui_config = directory / "config.yaml"
            tui_config.write_text(yaml.safe_dump(tui), "utf-8")
            log = (directory / "server.log").open("w", encoding="utf-8")
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
                    str(server_port),
                    "--log-level",
                    "warning",
                ],
                cwd=ROOT,
                env=env,
                stdout=log,
                stderr=log,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
            with httpx.Client(trust_env=False, timeout=1) as client:
                for _ in range(100):
                    try:
                        if client.get(f"http://127.0.0.1:{server_port}/health").status_code == 200:
                            break
                    except httpx.HTTPError:
                        pass
                    time.sleep(0.1)
                else:
                    raise RuntimeError("真实模型回归网关未就绪")
            run(
                "real-tui",
                [
                    "scripts/verify_real_tui.py",
                    "--config",
                    str(tui_config),
                    "--output",
                    "reports/pressure-real-tui.json",
                ],
            )
            report["real_model"] = True
        run("ruff-check", ["-m", "ruff", "check", "."])
        run("ruff-format", ["-m", "ruff", "format", "--check", "."])
        run("build", ["-m", "build"])
        report["passed"] = True
    finally:
        if process:
            stop_process(process)
        if log:
            log.close()
        if database:
            database.engine.dispose()
        (ROOT / "reports/pressure-validation.json").write_text(
            json.dumps(report, ensure_ascii=False, indent=2), "utf-8"
        )


if __name__ == "__main__":
    main()

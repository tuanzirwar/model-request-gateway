"""一键验收入口；缺少真实依赖时明确失败，不把跳过当作通过。"""

import argparse
import json
import os
import subprocess
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--browser", action="store_true")
    parser.add_argument("--real-model", action="store_true")
    args = parser.parse_args()
    if not os.environ.get("GATEWAY_TEST_REDIS") or not os.environ.get("GATEWAY_TEST_MYSQL"):
        parser.error("请配置专用GATEWAY_TEST_REDIS和GATEWAY_TEST_MYSQL，不使用业务库")
    checks = [
        ("ruff-check", ["-m", "ruff", "check", "."]),
        ("ruff-format", ["-m", "ruff", "format", "--check", "."]),
        (
            "unit-tests",
            [
                "-m",
                "pytest",
                "-q",
                "--basetemp=.local/pytest-final",
                "--junitxml=reports/junit.xml",
            ],
        ),
        ("migration-run", ["scripts/verify_migrations.py"]),
        ("e2e-run", ["scripts/verify_e2e.py"]),
    ]
    if args.browser:
        checks.append(("browser-run", ["scripts/verify_browser.py"]))
    if args.real_model:
        checks.append(("real-tui-run", ["scripts/verify_real_tui.py"]))
    checks.append(("build", ["-m", "build", "--no-isolation"]))
    result = {"passed": False, "checks": []}
    for name, command in checks:
        with (ROOT / "reports" / (name + ".txt")).open("w", encoding="utf-8") as log:
            completed = subprocess.run(
                [sys.executable, *command],
                cwd=ROOT,
                env={**os.environ, "PYTHONIOENCODING": "utf-8"},
                stdout=log,
                stderr=log,
            )
        # 公开日志使用占位路径，避免构建与错误堆栈暴露本机目录和用户名。
        log_path = ROOT / "reports" / (name + ".txt")
        content = log_path.read_text("utf-8")
        for path, placeholder in ((ROOT, "${PROJECT_ROOT}"), (Path.home(), "${HOME}")):
            for spelling in (str(path).replace("\\", "\\\\"), str(path), path.as_posix()):
                content = content.replace(spelling, placeholder)
        log_path.write_text(content, "utf-8")
        if name == "unit-tests":
            junit = ROOT / "reports/junit.xml"
            tree = ET.parse(junit)
            for node in tree.iter():
                node.attrib.pop("hostname", None)
            tree.write(junit, encoding="utf-8", xml_declaration=True)
        result["checks"].append({"name": name, "exit_code": completed.returncode})
        (ROOT / "reports/commands.json").write_text(json.dumps(result, indent=2), "utf-8")
        print(f"{name}: {'通过' if completed.returncode == 0 else '失败'}", flush=True)
        if completed.returncode:
            raise SystemExit(f"验收中止，见reports/{name}.txt")
    result["passed"] = True
    (ROOT / "reports/commands.json").write_text(json.dumps(result, indent=2), "utf-8")


if __name__ == "__main__":
    main()

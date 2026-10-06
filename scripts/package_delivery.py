"""汇总实际验收结果并打包源码，排除运行目录、配置密钥和虚拟环境。"""

import hashlib
import importlib.metadata
import json
import platform
import re
import zipfile
from datetime import UTC, datetime
from pathlib import Path

import redis
import yaml
from sqlalchemy import create_engine, text

ROOT = Path(__file__).resolve().parent.parent
EXCLUDED = {
    ".venv",
    ".local",
    ".pytest_cache",
    ".ruff_cache",
    "__pycache__",
    "build",
    ".git",
}


def load_report(name):
    result = json.loads((ROOT / "reports" / name).read_text("utf-8"))
    assert result["passed"] is True, name
    return result


def main():
    http = load_report("http-e2e.json")
    real = load_report("real-tui-model.json")
    migrations = load_report("migrations.json")
    benchmark = load_report("benchmark.json")
    commands = load_report("commands.json")
    browser = load_report("browser-e2e.json")
    deployment = load_report("deployment-config.json")
    tests = (ROOT / "reports/unit-tests.txt").read_text("utf-8")
    match = re.search(r"(\d+) passed", tests)
    assert match and "failed" not in tests and "skipped" not in tests
    test_count = int(match.group(1))
    version = importlib.metadata.version("model-request-gateway")
    assert all(row["exit_code"] == 0 for row in commands["checks"])
    assert "All checks passed" in (ROOT / "reports/ruff-check.txt").read_text("utf-8")
    assert "already formatted" in (ROOT / "reports/ruff-format.txt").read_text("utf-8")
    assert "Successfully built" in (ROOT / "reports/build.txt").read_text("utf-8")
    config = yaml.safe_load((ROOT / "gateway.yaml").read_text("utf-8"))
    engine = create_engine(config["database_url"])
    with engine.connect() as conn:
        mysql_version = conn.execute(text("SELECT VERSION()")).scalar_one()
    engine.dispose()
    client = redis.Redis.from_url(config["redis_url"])
    redis_version = client.info("server")["redis_version"]
    client.close()
    demo = json.loads((ROOT / ".local/demo-process.json").read_text("utf-8-sig"))
    report = {
        "passed": True,
        "generated_at": datetime.now(UTC).isoformat(),
        "environment": {
            "python": platform.python_version(),
            "platform": platform.platform(),
            "mysql_server": mysql_version,
            "redis_server": redis_version,
            "packages": {
                name: importlib.metadata.version(name)
                for name in (
                    "fastapi",
                    "uvicorn",
                    "httpx",
                    "sqlalchemy",
                    "pymysql",
                    "redis",
                    "alembic",
                    "pytest",
                    "ruff",
                    "prometheus-client",
                    "playwright",
                    "model-request-gateway",
                )
            },
        },
        "checks": {
            "unit_and_real_dependency_tests": test_count,
            "http_acceptance_checks": len(http["checks"]),
            "fresh_migration_checks": len(migrations["checks"]),
            "real_tui_model": real["passed"],
            "browser_checks": len(browser["checks"]),
            "ruff_check": True,
            "ruff_format": True,
            "wheel_and_sdist": True,
            "compose_config_parse": True,
            "compose_configuration_checks": len(deployment["checks"]),
            "docker_runtime_verified": deployment["container_runtime_tested"],
            "github_ci_verified": deployment["github_ci_tested"],
        },
        "benchmark": benchmark,
        "benchmark_scope": "当前交付代码；真实HTTP/MySQL/Redis及无推理替身，不含模型推理",
        "real_model_defaults": config["models"]["coding"].get("request_defaults", {}),
        "demo_url": f"http://127.0.0.1:{demo['port']}/docs",
        "limitations": [
            "数据库有界池组合改动缓解本机退化；不是模型推理QPS或唯一根因证明",
            "未验证Docker实际运行、终端视觉、Redis故障转移及生产收益",
            "已合并PR为设计依据；独立网关未作为上游PR合并",
        ],
    }
    (ROOT / "reports/final-validation.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2), "utf-8"
    )
    packages = [
        f"{name}=={version}" for name, version in sorted(report["environment"]["packages"].items())
    ]
    (ROOT / "reports/verified-versions.txt").write_text("\n".join(packages) + "\n", "utf-8")
    secret = yaml.safe_load((ROOT / ".local/tui-demo/config.yaml").read_text("utf-8"))["providers"][
        0
    ]["api_key"].encode()
    files = []
    for path in sorted(ROOT.rglob("*")):
        if not path.is_file():
            continue
        relative = path.relative_to(ROOT)
        if any(part in EXCLUDED or part.endswith(".egg-info") for part in relative.parts):
            continue
        if "dist" in relative.parts and f"-{version}" not in path.name:
            continue
        if path.name in {"gateway.yaml", ".env", ".install.txt"} or path.suffix == ".pyc":
            continue
        assert secret not in path.read_bytes(), f"交付文件含本机演示密钥：{relative}"
        files.append(path)
    destination = ROOT.parent / "output"
    destination.mkdir(exist_ok=True)
    archive = destination / f"model-request-gateway-{version}-20261006.zip"
    with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED) as bundle:
        for path in files:
            bundle.write(path, ROOT.name + "/" + path.relative_to(ROOT).as_posix())
    with zipfile.ZipFile(archive) as bundle:
        assert bundle.testzip() is None
        assert ROOT.name + "/docs/verification.md" in bundle.namelist()
        assert not any("/.local/" in name or "/.venv/" in name for name in bundle.namelist())
    digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    archive.with_suffix(".sha256").write_text(digest + "  " + archive.name + "\n", "utf-8")
    print(json.dumps({"archive": str(archive), "files": len(files), "passed": True}))


if __name__ == "__main__":
    main()

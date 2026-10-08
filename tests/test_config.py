"""错误配置在启动时拒绝，避免拼写错误和非法容量静默生效。"""

import pytest
import yaml

from model_gateway.config import load_settings


@pytest.fixture
def config_file(tmp_path, monkeypatch):
    monkeypatch.delenv("GATEWAY_DATABASE_URL", raising=False)
    monkeypatch.delenv("GATEWAY_REDIS_URL", raising=False)
    monkeypatch.delenv("GATEWAY_METRICS_KEY", raising=False)
    path = tmp_path / "gateway.yaml"
    monkeypatch.setenv("GATEWAY_CONFIG", str(path))
    raw = {
        "database_url": "sqlite:///test.db",
        "redis_url": "redis://localhost:6379/0",
        "models": {"coding": {"model": "upstream", "endpoint": "https://example.com/v1/chat"}},
    }
    return path, raw


@pytest.mark.parametrize(
    "field,value",
    [
        ("lease_seconds", float("nan")),
        ("first_seconds", float("inf")),
        ("total_seconds", True),
        ("idle_seconds", -1),
        ("max_body_bytes", 0),
        ("retention_days", "30"),
        ("max_frame_bytes", 2**25),
        ("namespace", "{bad}"),
        ("allow_local_http", "false"),
        ("unknown", 1),
        ("max_inflight_requests", 0),
        ("max_inflight_requests", 4097),
        ("redis_connections", True),
        ("upstream_connections", 4097),
        ("max_stream_bytes", 0),
        ("max_stream_bytes", 64 * 1048576 + 1),
        ("db_batch_size", 257),
        ("redis_batch_size", False),
        ("metadata_queue_size", 4097),
        ("db_batch_seconds", -0.001),
        ("db_batch_seconds", True),
        ("db_batch_seconds", float("nan")),
        ("db_batch_seconds", 0.051),
        ("metadata_transport", "rabbitmq"),
        ("metadata_transport", []),
        ("metadata_shards", True),
        ("metadata_shards", 17),
    ],
)
def test_invalid_global_configuration(config_file, field, value):
    path, raw = config_file
    raw[field] = value
    path.write_text(yaml.safe_dump(raw), "utf-8")
    with pytest.raises(ValueError):
        load_settings()


def test_zero_batch_window_and_disable_batching_are_supported(config_file):
    path, raw = config_file
    raw.update(db_batch_seconds=0, db_batch_size=1, redis_batch_size=1)
    path.write_text(yaml.safe_dump(raw), "utf-8")
    settings = load_settings()
    assert settings.db_batch_seconds == 0
    assert settings.db_batch_size == settings.redis_batch_size == 1


@pytest.mark.parametrize(
    "field,value",
    [
        ("endpoint", "file:///tmp/a"),
        ("endpoint", "https://user:secret@example.com/v1"),
        ("endpoint", "http://example.com/v1"),
        ("concurrency", True),
        ("concurrency", 1.5),
        ("model", ""),
        ("unknown", 1),
        ("request_defaults", {"model": "untrusted-override"}),
        ("request_defaults", {"max_tokens": -1}),
    ],
)
def test_invalid_model_configuration(config_file, field, value):
    path, raw = config_file
    raw["models"]["coding"][field] = value
    path.write_text(yaml.safe_dump(raw), "utf-8")
    with pytest.raises(ValueError):
        load_settings()


def test_limits_and_monitor_key_are_loaded(config_file, monkeypatch):
    path, raw = config_file
    raw.update(max_frame_bytes=128, max_body_bytes=1024, retention_days=7)
    path.write_text(yaml.safe_dump(raw), "utf-8")
    monkeypatch.setenv("GATEWAY_METRICS_KEY", "short")
    with pytest.raises(ValueError):
        load_settings()
    monkeypatch.setenv("GATEWAY_METRICS_KEY", "monitor-" + "x" * 32)
    settings = load_settings()
    assert settings.max_frame_bytes == 128 and settings.max_body_bytes == 1024
    assert settings.retention_days == 7 and settings.metrics_key.startswith("monitor-")

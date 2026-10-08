"""服务配置与上游白名单；不允许调用方指定任意网址。"""

import math
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlparse

import yaml


@dataclass(frozen=True)
class Model:
    alias: str
    upstream_model: str
    endpoint: str
    key: str
    concurrency: int = 4
    request_defaults: dict = field(default_factory=dict)
    capacity_group: str = ""

    @property
    def capacity_key(self):
        return self.capacity_group or self.alias


@dataclass(frozen=True)
class Settings:
    database_url: str
    redis_url: str
    namespace: str
    models: dict[str, Model]
    lease_seconds: float = 15
    total_seconds: float = 120
    first_seconds: float = 45
    idle_seconds: float = 20
    max_frame_bytes: int = 65536
    max_body_bytes: int = 1048576
    retention_days: int = 30
    metrics_key: str = ""
    db_workers: int = 4
    db_queue_size: int = 32
    db_queue_seconds: float = 1
    max_inflight_requests: int = 128
    upstream_connections: int = 100
    redis_connections: int = 128
    max_stream_bytes: int = 16 * 1048576
    db_batch_size: int = 64
    db_batch_seconds: float = 0
    metadata_queue_size: int = 256
    redis_batch_size: int = 64
    upstream_keepalive_seconds: float = 2


def load_settings():
    raw = yaml.safe_load(Path(os.environ.get("GATEWAY_CONFIG", "gateway.yaml")).read_text("utf-8"))
    allowed = {
        "database_url",
        "redis_url",
        "namespace",
        "models",
        "allow_local_http",
        "metrics_key_env",
        "lease_seconds",
        "total_seconds",
        "first_seconds",
        "idle_seconds",
        "max_frame_bytes",
        "max_body_bytes",
        "retention_days",
        "db_workers",
        "db_queue_size",
        "db_queue_seconds",
        "max_inflight_requests",
        "upstream_connections",
        "redis_connections",
        "max_stream_bytes",
        "db_batch_size",
        "db_batch_seconds",
        "metadata_queue_size",
        "redis_batch_size",
        "upstream_keepalive_seconds",
    }
    if not isinstance(raw, dict) or set(raw) - allowed:
        raise ValueError("配置必须为映射且不能包含未知字段")
    if not isinstance(raw.get("models"), dict) or not raw["models"]:
        raise ValueError("至少需要配置一个模型")
    if not isinstance(raw.get("allow_local_http", False), bool):
        raise ValueError("allow_local_http必须为布尔值")
    namespace = raw.get("namespace", "model-gateway")
    if not isinstance(namespace, str) or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,80}", namespace):
        raise ValueError("namespace必须为长度1至80的安全标识")
    for name in (
        "lease_seconds",
        "total_seconds",
        "first_seconds",
        "idle_seconds",
        "db_queue_seconds",
        "upstream_keepalive_seconds",
    ):
        if name in raw and (
            isinstance(raw[name], bool)
            or not isinstance(raw[name], (int, float))
            or not math.isfinite(raw[name])
            or raw[name] <= 0
        ):
            raise ValueError(f"{name}必须为有限正数")
    for name in (
        "max_frame_bytes",
        "max_body_bytes",
        "retention_days",
        "db_workers",
        "db_queue_size",
        "max_inflight_requests",
        "upstream_connections",
        "redis_connections",
        "max_stream_bytes",
        "db_batch_size",
        "metadata_queue_size",
        "redis_batch_size",
    ):
        if name in raw and (type(raw[name]) is not int or raw[name] <= 0):
            raise ValueError(f"{name}必须为正整数")
    models = {}
    for alias, spec in raw["models"].items():
        if not isinstance(alias, str) or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,100}", alias):
            raise ValueError("模型别名必须为长度1至100的安全标识")
        if not isinstance(spec, dict) or set(spec) - {
            "model",
            "endpoint",
            "key_env",
            "concurrency",
            "request_defaults",
            "capacity_group",
        }:
            raise ValueError("模型配置存在未知字段")
        if not isinstance(spec.get("model"), str) or not spec["model"].strip():
            raise ValueError("必须配置非空上游模型名")
        endpoint = spec.get("endpoint")
        if not isinstance(endpoint, str):
            raise ValueError("必须配置上游地址")
        parsed = urlparse(endpoint)
        if parsed.port is not None and not 1 <= parsed.port <= 65535:
            raise ValueError("上游端口必须在1至65535之间")
        if (
            parsed.scheme not in ("http", "https")
            or not parsed.hostname
            or parsed.username
            or parsed.password
            or parsed.fragment
        ):
            raise ValueError("上游必须为合法HTTP(S)配置网址")
        if parsed.scheme == "http" and not raw.get("allow_local_http", False):
            raise ValueError("非TLS上游必须在受控环境显式开启")
        concurrency = spec.get("concurrency", 4)
        if type(concurrency) is not int or concurrency < 1:
            raise ValueError("模型并发必须大于零")
        capacity_group = spec.get("capacity_group", alias)
        if not isinstance(capacity_group, str) or not re.fullmatch(
            r"[A-Za-z0-9_.:-]{1,100}", capacity_group
        ):
            raise ValueError("容量组必须为长度1至100的安全标识")
        if any(
            model.capacity_key == capacity_group and model.concurrency != concurrency
            for model in models.values()
        ):
            raise ValueError("同一容量组的并发上限必须一致")
        key_env = spec.get("key_env", "")
        if not isinstance(key_env, str):
            raise ValueError("key_env必须为环境变量名称")
        defaults = spec.get("request_defaults", {})
        if not isinstance(defaults, dict) or set(defaults) - {
            "temperature",
            "top_p",
            "max_tokens",
            "max_completion_tokens",
            "reasoning_effort",
            "presence_penalty",
            "frequency_penalty",
            "seed",
            "stop",
        }:
            raise ValueError("request_defaults只能包含受支持的生成参数")
        from fastapi import HTTPException

        from .validation import validate_payload

        try:
            validate_payload(
                {"model": alias, "messages": [{"role": "user", "content": "check"}], **defaults}
            )
        except HTTPException as exc:
            raise ValueError("模型默认生成参数无效") from exc
        models[alias] = Model(
            alias,
            spec["model"],
            endpoint,
            os.environ.get(key_env, ""),
            concurrency,
            defaults,
            capacity_group,
        )
    metrics_env = raw.get("metrics_key_env", "GATEWAY_METRICS_KEY")
    if not isinstance(metrics_env, str):
        raise ValueError("metrics_key_env必须为环境变量名称")
    metrics_key = os.environ.get(metrics_env, "")
    if metrics_key and len(metrics_key) < 32:
        raise ValueError("监控密钥至少32字符")
    database_url = os.environ.get("GATEWAY_DATABASE_URL", raw.get("database_url", ""))
    redis_url = os.environ.get("GATEWAY_REDIS_URL", raw.get("redis_url", ""))
    if not isinstance(database_url, str) or not database_url.startswith(
        ("mysql+pymysql://", "sqlite:")
    ):
        raise ValueError("数据库URL需为mysql+pymysql或测试用sqlite")
    if not isinstance(redis_url, str) or urlparse(redis_url).scheme not in ("redis", "rediss"):
        raise ValueError("Redis URL需为redis或rediss")
    settings = Settings(
        database_url,
        redis_url,
        namespace,
        models,
        metrics_key=metrics_key,
        **{
            name: raw[name]
            for name in (
                "lease_seconds",
                "total_seconds",
                "first_seconds",
                "idle_seconds",
                "max_frame_bytes",
                "max_body_bytes",
                "retention_days",
                "db_workers",
                "db_queue_size",
                "db_queue_seconds",
                "max_inflight_requests",
                "upstream_connections",
                "redis_connections",
                "max_stream_bytes",
                "db_batch_size",
                "db_batch_seconds",
                "metadata_queue_size",
                "redis_batch_size",
                "upstream_keepalive_seconds",
            )
            if name in raw
        },
    )
    if (
        settings.lease_seconds < 1
        or min(settings.total_seconds, settings.first_seconds, settings.idle_seconds) <= 0
    ):
        raise ValueError("超时与租约必须为正且租约至少一秒")
    if (
        settings.max_frame_bytes > settings.max_body_bytes
        or settings.max_body_bytes > 16 * 1024 * 1024
    ):
        raise ValueError("单帧上限不能超过请求体上限，请求体最多16MiB")
    if settings.db_workers > 64 or settings.db_queue_size > 4096:
        raise ValueError("数据库线程最多64，等待队列最多4096")
    if (
        max(
            settings.max_inflight_requests,
            settings.upstream_connections,
            settings.redis_connections,
        )
        > 4096
    ):
        raise ValueError("在途请求和连接池容量最多4096")
    if settings.max_stream_bytes > 64 * 1048576:
        raise ValueError("每次流式输出最多64MiB")
    if (
        max(settings.db_batch_size, settings.redis_batch_size) > 256
        or settings.metadata_queue_size > 4096
    ):
        raise ValueError("数据库每批最多256条，消息队列最多4096条")
    if (
        isinstance(settings.db_batch_seconds, bool)
        or not isinstance(settings.db_batch_seconds, (int, float))
        or not math.isfinite(settings.db_batch_seconds)
        or not 0 <= settings.db_batch_seconds <= 0.05
    ):
        raise ValueError("批处理收集窗口必须为0至50毫秒")
    return settings

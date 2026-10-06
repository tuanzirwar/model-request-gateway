"""进程级指标：固定标签避免请求ID和应用密钥造成高基数或泄漏。"""

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest


class Metrics:
    def __init__(self):
        self.registry = CollectorRegistry()
        self.stage = Histogram(
            "gateway_stage_seconds",
            "内部步骤耗时，不等同端到端p95",
            ["operation", "phase"],
            buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5),
            registry=self.registry,
        )
        self.terminal = Counter(
            "gateway_requests",
            "本进程完成清理的请求",
            ["model", "status"],
            registry=self.registry,
        )
        self.duration = Histogram(
            "gateway_request_seconds",
            "含上游和清理的请求耗时",
            ["model", "status"],
            registry=self.registry,
        )
        self.active = Gauge(
            "gateway_active_requests",
            "本进程持有接入额度的请求数",
            registry=self.registry,
        )
        self.lag = Gauge(
            "gateway_event_loop_lag_seconds", "事件循环调度延迟", registry=self.registry
        )

    def render(self):
        return generate_latest(self.registry)

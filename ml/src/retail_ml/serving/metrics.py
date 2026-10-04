"""Prometheus metrics (names are a contract with the Grafana serving dashboard)."""

from __future__ import annotations

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Histogram,
    Info,
    generate_latest,
)

PROBABILITY_BUCKETS = [round(0.05 * i, 2) for i in range(1, 21)]


class Metrics:
    """One registry per app (no duplicate-name errors across test apps). `import feast` puts
    prometheus_client in multiprocess mode, so values live in per-pid mmap files: correct for the
    single uvicorn worker, but more workers would need a `MultiProcessCollector`."""

    content_type = CONTENT_TYPE_LATEST

    def __init__(self) -> None:
        self.registry = CollectorRegistry()
        self.requests = Counter(
            "requests_total",
            "HTTP requests by route template and status code",
            ["route", "status"],
            registry=self.registry,
        )
        self.latency = Histogram(
            "request_latency_seconds",
            "HTTP request latency",
            ["route"],
            registry=self.registry,
        )
        self.probability = Histogram(
            "prediction_probability",
            "Predicted late-delivery probability",
            buckets=PROBABILITY_BUCKETS,
            registry=self.registry,
        )
        self.model_version = Info(
            "model_version", "Champion version being served", registry=self.registry
        )

    def render(self) -> bytes:
        return generate_latest(self.registry)

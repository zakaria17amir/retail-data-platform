"""Prometheus metrics (names are a contract with the Grafana serving dashboard)."""

from __future__ import annotations

import os

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)
from prometheus_client.multiprocess import MultiProcessCollector

PROBABILITY_BUCKETS = [round(0.05 * i, 2) for i in range(1, 21)]
RECOMMEND_BUCKETS = [0.005, 0.01, 0.02, 0.03, 0.05, 0.075, 0.1, 0.25, 0.5, 1.0, 2.5]


class Metrics:
    """One registry per app (no duplicate-name errors across test apps). `import feast` puts
    prometheus_client in multiprocess mode (values in per-pid mmap files under
    PROMETHEUS_MULTIPROC_DIR), so `render` aggregates every uvicorn worker's files."""

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
        # 1 for the version a worker serves, 0 once it reloaded away; max over workers keeps a
        # version visible while any worker still serves it
        self.model_version = Gauge(
            "model_version_info",
            "Champion version being served",
            ["version"],
            multiprocess_mode="max",
            registry=self.registry,
        )
        self.recommend_requests = Counter(
            "recommend_requests_total",
            "Answered /recommend requests by strategy",
            ["strategy"],
            registry=self.registry,
        )
        self.recommend_latency = Histogram(
            "recommend_latency_seconds",
            "/recommend handler latency (features, candidates, rerank)",
            buckets=RECOMMEND_BUCKETS,
            registry=self.registry,
        )

    def render(self) -> bytes:
        if "PROMETHEUS_MULTIPROC_DIR" not in os.environ:
            return generate_latest(self.registry)
        registry = CollectorRegistry()
        MultiProcessCollector(registry)  # type: ignore[no-untyped-call]
        return generate_latest(registry)

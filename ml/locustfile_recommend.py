"""Load test for `POST /recommend` (target p95 < 50 ms at 20 users): `make recommend-load`.

LOCUST_SESSION_IDS (comma-separated) are sessions with online `session_features` (rerank path);
LOCUST_UNKNOWN_SHARE of requests use a fresh unknown session (popularity fallback)."""

from __future__ import annotations

import os
import random
import uuid

from locust import HttpUser, between, task

SESSION_IDS = [s for s in (os.environ.get("LOCUST_SESSION_IDS") or "").split(",") if s]
UNKNOWN_SHARE = float(os.environ.get("LOCUST_UNKNOWN_SHARE") or (0.2 if SESSION_IDS else 1.0))


class RecommendUser(HttpUser):
    wait_time = between(0.05, 0.2)

    @task
    def recommend(self) -> None:
        if SESSION_IDS and random.random() >= UNKNOWN_SHARE:
            session_id, name = random.choice(SESSION_IDS), "/recommend [known]"
        else:
            session_id, name = f"locust-{uuid.uuid4().hex}", "/recommend [unknown]"
        self.client.post("/recommend", json={"session_id": session_id, "k": 10}, name=name)

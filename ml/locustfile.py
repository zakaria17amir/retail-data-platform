"""Load test for the late-delivery endpoint (target p95 < 50 ms with Redis warm):

locust -f ml/locustfile.py --headless -u 20 -r 5 -t 60s --host http://127.0.0.1:8000

LOCUST_SELLER_IDS (comma-separated) picks sellers that exist in the online store; the default
unknown seller still does the Redis lookup (a miss)."""

from __future__ import annotations

import os
import random

from locust import HttpUser, between, task

SELLER_IDS = (os.environ.get("LOCUST_SELLER_IDS") or "locust-unknown-seller").split(",")
ORDER = {
    "order_id": "locust",
    "customer_id": "locust",
    "customer_unique_id": "locust",
    "order_purchase_ts_utc": "2018-07-01T09:00:00Z",
    "order_approved_ts_utc": "2018-07-01T10:30:00Z",
    "order_estimated_delivery_ts_utc": "2018-07-20T00:00:00Z",
    "n_items": 2,
    "n_sellers": 1,
    "total_price": 100.0,
    "total_freight": 20.0,
    "product_category": "health_beauty",
    "payment_type": "credit_card",
    "payment_installments": 3,
    "customer_state": "SP",
    "customer_lat": -23.55,
    "customer_lng": -46.63,
    "seller_state": "SP",
    "seller_lat": -23.0,
    "seller_lng": -47.0,
}


class PredictUser(HttpUser):
    wait_time = between(0.05, 0.2)

    @task
    def predict(self) -> None:
        self.client.post(
            "/predict/late-delivery", json={**ORDER, "seller_id": random.choice(SELLER_IDS)}
        )

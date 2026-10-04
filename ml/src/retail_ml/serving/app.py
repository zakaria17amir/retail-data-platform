"""FastAPI serving for the late-delivery champion: `/predict/late-delivery`, `/health`, `/reload`,
`/metrics`. Run: `uvicorn retail_ml.serving.app:app`."""

from __future__ import annotations

import logging
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime

import numpy as np
import pandas as pd
from fastapi import FastAPI, HTTPException, Request, Response
from pydantic import BaseModel, ConfigDict, Field

from retail_ml.late_delivery.train import model_inputs
from retail_ml.serving.metrics import Metrics
from retail_ml.serving.model import (
    MODEL_NAME,
    LoadedModel,
    SellerLookup,
    load_champion,
    load_seller_lookup,
)

logger = logging.getLogger(__name__)
NO_CHAMPION = f"no champion for {MODEL_NAME} in the MLflow registry; train and promote one"


class LateDeliveryRequest(BaseModel):
    """The order as known at approval: contract fields minus the label (`is_late`), the delivery
    timestamp and the final `order_status`. Naive timestamps are read as UTC."""

    model_config = ConfigDict(extra="forbid")

    order_id: str
    customer_id: str
    customer_unique_id: str
    seller_id: str
    order_purchase_ts_utc: datetime
    order_approved_ts_utc: datetime
    order_estimated_delivery_ts_utc: datetime
    n_items: int = Field(ge=1)
    n_sellers: int = Field(ge=1)
    total_price: float = Field(ge=0)
    total_freight: float = Field(ge=0)
    product_category: str | None = None
    payment_type: str | None = None
    payment_installments: int = Field(ge=0)
    customer_state: str
    customer_lat: float | None = Field(default=None, ge=-90, le=90)
    customer_lng: float | None = Field(default=None, ge=-180, le=180)
    seller_state: str
    seller_lat: float | None = Field(default=None, ge=-90, le=90)
    seller_lng: float | None = Field(default=None, ge=-180, le=180)


class Prediction(BaseModel):
    probability: float
    model_version: str


class Health(BaseModel):
    model_loaded: bool
    model_version: str | None


@dataclass
class _State:
    model: LoadedModel | None = None
    lookup: SellerLookup | None = None


def create_app(
    load_model: Callable[[], LoadedModel | None] = load_champion,
    load_lookup: Callable[[], SellerLookup] = load_seller_lookup,
) -> FastAPI:
    metrics = Metrics()
    state = _State()

    def set_model(loaded: LoadedModel) -> None:
        if state.model:
            metrics.model_version.labels(state.model.version).set(0)
        state.model = loaded
        metrics.model_version.labels(loaded.version).set(1)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        # liveness must not depend on MLflow/Feast: start empty and report it in /health
        try:
            if loaded := load_model():
                set_model(loaded)
            else:
                logger.warning(NO_CHAMPION)
        except Exception:
            logger.exception("champion could not be loaded")
        try:
            state.lookup = load_lookup()
        except Exception:
            logger.exception("online feature store not ready; retried on first prediction")
        yield

    app = FastAPI(title="retail-ml serving", lifespan=lifespan)

    @app.middleware("http")
    async def record(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        start, status = time.perf_counter(), 500
        try:
            response = await call_next(request)
            status = response.status_code
            return response
        finally:
            route = getattr(request.scope.get("route"), "path", "unmatched")
            metrics.requests.labels(route, str(status)).inc()
            metrics.latency.labels(route).observe(time.perf_counter() - start)

    @app.get("/health")
    def health() -> Health:
        version = state.model.version if state.model else None
        return Health(model_loaded=state.model is not None, model_version=version)

    @app.post("/predict/late-delivery")
    def predict(req: LateDeliveryRequest) -> Prediction:
        model = state.model
        if model is None:
            raise HTTPException(503, NO_CHAMPION)
        try:
            state.lookup = state.lookup or load_lookup()
            seller = state.lookup(req.seller_id)
        except Exception as e:
            logger.exception("online feature lookup failed")
            raise HTTPException(503, f"online feature store unavailable: {e}") from None
        # unseen seller → None → NaN, the same value training saw for sellers without a snapshot
        X = model_inputs(pd.DataFrame([{**req.model_dump(), **seller}]))
        probability = float(np.asarray(model.model.predict(X), dtype=float)[0])
        metrics.probability.observe(probability)
        return Prediction(probability=probability, model_version=model.version)

    @app.post("/reload")
    def reload() -> Health:
        try:
            loaded = load_model()
        except Exception as e:
            logger.exception("reload failed")
            raise HTTPException(503, f"reload failed, serving unchanged: {e}") from None
        if loaded is None:
            raise HTTPException(503, f"{NO_CHAMPION}; serving unchanged")
        set_model(loaded)
        return health()

    @app.get("/metrics")
    def prometheus() -> Response:
        return Response(metrics.render(), media_type=metrics.content_type)

    return app


app = create_app()

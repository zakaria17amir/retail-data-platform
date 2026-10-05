import hashlib
import os
from collections.abc import Callable

import mlflow

EXPERIMENT = "genai"
_ENABLED = False


def enable_tracing(tracking_uri: str | None = None, experiment: str = EXPERIMENT) -> None:
    """MLflow 3 tracing; LangChain/LangGraph runs are auto-instrumented."""
    global _ENABLED
    mlflow.set_tracking_uri(
        tracking_uri or os.environ.get("MLFLOW_TRACKING_URI", "http://127.0.0.1:5000")
    )
    mlflow.set_experiment(experiment)
    mlflow.langchain.autolog()
    _ENABLED = True


def prompt_hash(prompt: str) -> str:
    return hashlib.sha256(prompt.encode()).hexdigest()[:12]


def traced[T](name: str, prompt: str, model_alias: str, fn: Callable[[], T]) -> T:
    """Run fn under one root span tagged with the prompt hash and model alias."""
    if not _ENABLED:
        return fn()
    with mlflow.start_span(name=name):
        mlflow.update_current_trace(
            tags={"prompt_hash": prompt_hash(prompt), "model_alias": model_alias}
        )
        return fn()

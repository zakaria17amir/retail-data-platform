from typing import Any

import mlflow
import numpy as np
import pandas as pd
import pytest
from mlflow import MlflowClient

from retail_ml.config import promotion_mode
from retail_ml.late_delivery.evaluate import brier_baseline
from retail_ml.late_delivery.promote import approve, promote

NAME = "late_delivery"


class Scorer(mlflow.pyfunc.PythonModel):
    """Stub model: probability = clip(a * signal + b), optionally poisoned with NaN."""

    def __init__(
        self, a: float, b: float, nan: bool = False, clip: bool = True, col: str = "signal"
    ) -> None:
        self.a, self.b, self.nan, self.clip, self.col = a, b, nan, clip, col

    def predict(self, context: Any, model_input: pd.DataFrame, params: Any = None) -> Any:
        p = self.a * model_input[self.col].to_numpy() + self.b
        if self.clip:
            p = p.clip(0, 1)
        if self.nan:
            p[0] = np.nan
        return p


@pytest.fixture
def data() -> tuple[pd.DataFrame, pd.Series]:
    rng = np.random.default_rng(0)
    y = pd.Series(rng.uniform(size=400) < 0.3)
    signal = np.where(y, 0.7, 0.3) + rng.normal(0, 0.25, 400)
    noisy = signal + rng.normal(0, 0.5, 400)
    return pd.DataFrame({"signal": signal, "noisy": noisy}), y


def _register(model: Scorer) -> str:
    with mlflow.start_run():
        info = mlflow.pyfunc.log_model(name="model", python_model=model)
    return str(mlflow.register_model(info.model_uri, NAME).version)


def _aliases(client: MlflowClient) -> dict[str, str]:
    return {k: str(v) for k, v in client.get_registered_model(NAME).aliases.items()}


GOOD = Scorer(0.8, 0.1)
WEAK = Scorer(0.1, 0.25)
BASELINE_BRIER = 0.25


def test_first_model_without_champion_promotes(mlflow_uri: str, data: Any) -> None:
    client = MlflowClient()
    v = _register(WEAK)
    decision = promote(client, NAME, v, *data, baseline_brier=BASELINE_BRIER)
    assert decision.promoted and decision.champion_pr_auc is None
    assert _aliases(client) == {"champion": v, "challenger": v}


def test_better_challenger_replaces_champion(mlflow_uri: str, data: Any) -> None:
    client = MlflowClient()
    v1 = _register(Scorer(0.3, 0.2, col="noisy"))
    promote(client, NAME, v1, *data, baseline_brier=BASELINE_BRIER)
    v2 = _register(GOOD)
    decision = promote(client, NAME, v2, *data, baseline_brier=BASELINE_BRIER)
    assert decision.promoted
    assert decision.challenger_pr_auc > (decision.champion_pr_auc or 0)
    assert _aliases(client)["champion"] == v2


def test_pr_auc_tie_keeps_champion(mlflow_uri: str, data: Any) -> None:
    client = MlflowClient()
    v1 = _register(GOOD)
    promote(client, NAME, v1, *data, baseline_brier=BASELINE_BRIER)
    v2 = _register(Scorer(0.8, 0.1))
    decision = promote(client, NAME, v2, *data, baseline_brier=BASELINE_BRIER)
    assert decision.challenger_pr_auc == decision.champion_pr_auc
    assert not decision.promoted
    assert _aliases(client) == {"champion": v1, "challenger": v2}


def test_unscorable_champion_is_replaced_by_passing_challenger(mlflow_uri: str, data: Any) -> None:
    client = MlflowClient()
    broken = _register(Scorer(0.8, 0.1, col="dropped_column"))
    approve(client, NAME, broken)
    v = _register(GOOD)
    decision = promote(client, NAME, v, *data, baseline_brier=BASELINE_BRIER)
    assert decision.promoted and decision.reason == "champion could not be scored"
    assert decision.champion_pr_auc is None
    assert _aliases(client)["champion"] == v


def test_champion_not_scored_when_challenger_fails_model_tests(mlflow_uri: str, data: Any) -> None:
    client = MlflowClient()
    broken = _register(Scorer(0.8, 0.1, col="dropped_column"))
    approve(client, NAME, broken)
    v = _register(Scorer(0.8, 0.1, nan=True))
    decision = promote(client, NAME, v, *data, baseline_brier=BASELINE_BRIER)
    assert not decision.promoted and "nan" in decision.reason
    assert decision.champion_pr_auc is None
    assert _aliases(client)["champion"] == broken


def test_promote_rejects_unknown_mode(mlflow_uri: str, data: Any) -> None:
    with pytest.raises(ValueError, match="promotion mode"):
        promote(MlflowClient(), NAME, "1", *data, baseline_brier=BASELINE_BRIER, mode="yolo")


@pytest.mark.parametrize(("raw", "mode"), [(None, "auto"), ("", "auto"), (" Manual ", "manual")])
def test_promotion_mode_from_env(
    raw: str | None, mode: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    if raw is None:
        monkeypatch.delenv("PROMOTION_MODE", raising=False)
    else:
        monkeypatch.setenv("PROMOTION_MODE", raw)
    assert promotion_mode() == mode


def test_invalid_promotion_mode_env_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PROMOTION_MODE", "yolo")
    with pytest.raises(ValueError, match="PROMOTION_MODE"):
        promotion_mode()


def test_worse_challenger_stays_challenger(mlflow_uri: str, data: Any) -> None:
    client = MlflowClient()
    v1 = _register(GOOD)
    promote(client, NAME, v1, *data, baseline_brier=BASELINE_BRIER)
    rng_noise = Scorer(0.0, 0.3)
    v2 = _register(rng_noise)
    decision = promote(client, NAME, v2, *data, baseline_brier=BASELINE_BRIER)
    assert not decision.promoted and "pr_auc" in decision.reason
    assert _aliases(client) == {"champion": v1, "challenger": v2}


@pytest.mark.parametrize(
    ("model", "failure"),
    [
        (Scorer(0.8, 0.1, nan=True), "nan"),
        (Scorer(2.0, 0.0, clip=False), "range"),
        (Scorer(0.8, 0.1), "brier"),
    ],
)
def test_failed_model_test_blocks_promotion_even_without_champion(
    mlflow_uri: str, data: Any, model: Scorer, failure: str
) -> None:
    client = MlflowClient()
    v = _register(model)
    baseline = 0.0 if failure == "brier" else BASELINE_BRIER
    decision = promote(client, NAME, v, *data, baseline_brier=baseline)
    assert not decision.promoted and failure in decision.reason
    assert "champion" not in _aliases(client)


def test_beating_logistic_but_not_the_prior_fails_the_brier_gate(
    mlflow_uri: str, data: Any
) -> None:
    client = MlflowClient()
    X, y = data
    brier = float(np.mean((GOOD.predict(None, X) - y.to_numpy(dtype=float)) ** 2))
    metrics = {
        "constant_prior": {"test_brier": brier - 0.01},
        "logistic": {"test_brier": brier + 0.01},
        "lightgbm": {"test_brier": brier},
    }
    assert brier_baseline(metrics) == pytest.approx(brier - 0.01)
    v = _register(GOOD)
    decision = promote(client, NAME, v, X, y, baseline_brier=brier_baseline(metrics))
    assert not decision.promoted and "brier" in decision.reason and "baseline" in decision.reason
    assert "champion" not in _aliases(client)


def test_manual_mode_sets_challenger_only_and_prints_approve_command(
    mlflow_uri: str, data: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    client = MlflowClient()
    v = _register(GOOD)
    decision = promote(client, NAME, v, *data, baseline_brier=BASELINE_BRIER, mode="manual")
    assert not decision.promoted
    assert _aliases(client) == {"challenger": v}
    assert f"retail-ml promote late_delivery --version {v}" in capsys.readouterr().out
    approve(client, NAME, v)
    assert _aliases(client)["champion"] == v

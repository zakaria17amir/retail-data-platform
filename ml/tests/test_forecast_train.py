import dataclasses
import json
from pathlib import Path
from typing import Any

import mlflow
import numpy as np
import pandas as pd
import pytest
from mlflow import MlflowClient

from retail_ml.cli import main
from retail_ml.data import sha256_file
from retail_ml.forecast.features import HORIZON
from retail_ml.forecast.predict import OUTPUT_COLUMNS, forecast_demand
from retail_ml.forecast.train import (
    DemandForecastConfig,
    load_demand_forecast_config,
    promote_forecast,
    train,
)

END = pd.Timestamp("2018-08-27")
CUTOFF = END - pd.Timedelta(days=HORIZON - 1)
WEEKLY = np.array([1.3, 1.2, 1.1, 1.0, 0.9, 0.6, 0.5])
MODELLED = [
    ("bed_bath_table", "SP", 40.0),
    ("health_beauty", "SP", 25.0),
    ("sports_leisure", "RJ", 12.0),
    ("toys", "MG", 8.0),
    ("furniture_decor", "RJ", 5.0),
]
SPARSE = ("auto", "AC")


def synthetic_demand(gold_dir: Path, seed: int = 3) -> pd.DataFrame:
    """Contract-shaped demand_daily: weekly seasonality + Poisson noise, one sparse series."""
    rng = np.random.default_rng(seed)
    frames = []
    for i, (cat, state, level) in enumerate([*MODELLED, (*SPARSE, 0.1)]):
        dates = pd.date_range(END - pd.Timedelta(days=400 - 7 * i), END, freq="D")
        mean = level * WEEKLY[dates.dayofweek] * np.linspace(0.8, 1.2, len(dates))
        frames.append(
            pd.DataFrame(
                {
                    "date": dates.date,
                    "product_category": cat,
                    "customer_state": state,
                    "orders": rng.poisson(mean).astype("int64"),
                    "is_modelled": (cat, state) != SPARSE,
                }
            )
        )
    df = pd.concat(frames, ignore_index=True)
    (gold_dir / "ml").mkdir(parents=True, exist_ok=True)
    df.to_parquet(gold_dir / "ml" / "demand_daily.parquet", index=False)
    return df


def small(cfg: DemandForecastConfig) -> DemandForecastConfig:
    return dataclasses.replace(cfg, lightgbm={**cfg.lightgbm, "n_estimators": 150})


@pytest.fixture(scope="module")
def trained(tmp_path_factory: pytest.TempPathFactory) -> Any:
    tmp = tmp_path_factory.mktemp("forecast")
    mp = pytest.MonkeyPatch()
    gold = tmp / "gold"
    mp.setenv("GOLD_DIR", str(gold))
    mp.chdir(tmp)
    uri = f"sqlite:///{(tmp / 'mlflow.db').as_posix()}"
    mp.setenv("MLFLOW_TRACKING_URI", uri)
    mlflow.set_tracking_uri(uri)
    demand = synthetic_demand(gold)
    cfg = small(load_demand_forecast_config())
    result = train(cfg, promotion_mode="auto")
    yield {"result": result, "cfg": cfg, "gold": gold, "demand": demand}
    mp.undo()


def _runs() -> list[Any]:
    client = MlflowClient()
    exp = client.get_experiment_by_name("demand_forecast")
    assert exp is not None
    return list(client.search_runs([exp.experiment_id], order_by=["attributes.start_time ASC"]))


def test_config_paths_follow_gold_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GOLD_DIR", str(tmp_path))
    cfg = load_demand_forecast_config()
    assert cfg.demand_path == tmp_path / "ml" / "demand_daily.parquet"
    assert cfg.output_path == tmp_path / "ml" / "pred_demand_forecast.parquet"
    assert cfg.experiment == cfg.registered_model == "demand_forecast"
    assert (cfg.backtest_folds, cfg.top_series) == (3, 10)


def test_seasonal_naive_logged_before_lightgbm_with_lineage(trained: Any) -> None:
    runs = _runs()
    assert [r.data.tags["model_type"] for r in runs] == ["seasonal_naive", "lightgbm"]
    data_hash = sha256_file(trained["gold"] / "ml" / "demand_daily.parquet")
    for r in runs:
        assert r.data.tags["data_sha256"] == data_hash
        assert r.data.tags["git_sha"]
        m = r.data.metrics
        assert {f"backtest_wape_fold{k}" for k in (1, 2, 3)} | {
            "backtest_wape_mean",
            "test_wape",
            "test_wape_top10",
        } <= set(m)
        assert r.data.params["test_start"] == CUTOFF.date().isoformat()
        assert r.data.params["test_end"] == END.date().isoformat()
        assert r.data.params["fold_starts"] == ",".join(
            (CUTOFF - pd.Timedelta(days=HORIZON * k)).date().isoformat() for k in (3, 2, 1)
        )
        per_series = [k for k in m if k.startswith("test_wape.")]
        assert len(per_series) == len(MODELLED)
    naive, lgbm = (r.data.metrics for r in runs)
    assert lgbm["test_wape"] < naive["test_wape"]
    assert lgbm["backtest_wape_mean"] < naive["backtest_wape_mean"]


def test_excluded_series_are_reported(trained: Any) -> None:
    excluded = trained["result"].excluded
    assert list(zip(excluded["product_category"], excluded["customer_state"], strict=True)) == [
        SPARSE
    ]
    for r in _runs():
        assert r.data.params["n_series_modelled"] == str(len(MODELLED))
        assert r.data.params["n_series_excluded"] == "1"
        text = mlflow.artifacts.load_text(f"runs:/{r.info.run_id}/excluded_series.csv")
        assert "auto,AC" in text


def test_lightgbm_registered_as_challenger_and_first_promoted(trained: Any) -> None:
    aliases = {
        k: str(v) for k, v in MlflowClient().get_registered_model("demand_forecast").aliases.items()
    }
    assert aliases == {"challenger": "1", "champion": "1"}
    assert trained["result"].decision.promoted


def test_forecast_demand_writes_next_28_days_and_the_test_window(trained: Any) -> None:
    out = forecast_demand(trained["cfg"], MlflowClient())
    written = pd.read_parquet(trained["cfg"].output_path)
    assert list(written.columns) == OUTPUT_COLUMNS
    assert len(written) == len(out)
    assert set(out["model_version"]) == {"1"}
    assert out["forecast"].notna().all() and (out["forecast"] >= 0).all()
    assert out["run_ts"].nunique() == 1
    dates = pd.to_datetime(out["date"])
    future, test = out["split"] == "forecast", out["split"] == "test"
    assert (future | test).all()
    assert len(out[future]) == len(out[test]) == HORIZON * len(MODELLED)
    assert dates[future].min() == END + pd.Timedelta(days=1)
    assert dates[future].max() == END + pd.Timedelta(days=HORIZON)
    assert (dates[test].min(), dates[test].max()) == (CUTOFF, END)
    assert out.loc[future, "actual"].isna().all()
    demand = trained["demand"].assign(date=pd.to_datetime(trained["demand"]["date"]))
    merged = (
        out[test]
        .assign(date=dates[test])
        .merge(demand, on=["date", "product_category", "customer_state"])
    )
    assert len(merged) == HORIZON * len(MODELLED)
    assert (merged["actual"] == merged["orders"]).all()
    assert SPARSE[0] not in set(out["product_category"])


def test_retrain_on_the_same_data_ties_and_keeps_the_champion(trained: Any) -> None:
    result = train(trained["cfg"], promotion_mode="auto")
    assert not result.decision.promoted
    assert result.decision.challenger_wape == result.decision.champion_wape
    aliases = {
        k: str(v) for k, v in MlflowClient().get_registered_model("demand_forecast").aliases.items()
    }
    assert aliases == {"champion": "1", "challenger": result.version}


def test_cli_forecast_demand(trained: Any, capsys: pytest.CaptureFixture[str]) -> None:
    trained["cfg"].output_path.unlink(missing_ok=True)
    assert main(["forecast", "demand"]) == 0
    assert trained["cfg"].output_path.exists()
    assert json.loads(capsys.readouterr().out)["rows"] == 2 * HORIZON * len(MODELLED)


class Stub(mlflow.pyfunc.PythonModel):
    """Forecast = a * actual + b on the hidden truth (by date), optionally with a NaN."""

    def __init__(self, truth: dict[str, float], a: float, b: float, nan: bool = False) -> None:
        self.truth, self.a, self.b, self.nan = truth, a, b, nan

    def predict(self, context: Any, model_input: pd.DataFrame, params: Any = None) -> Any:
        y = model_input["date"].dt.strftime("%Y-%m-%d").map(self.truth).to_numpy(dtype=float)
        p = self.a * y + self.b
        if self.nan:
            p[-1] = np.nan
        return p


STUB_NAME = "demand_stub"


@pytest.fixture
def stub_window() -> tuple[pd.DataFrame, pd.Series, dict[str, float]]:
    dates = pd.date_range("2018-01-01", periods=56, freq="D")
    y = np.random.default_rng(0).poisson(10, 56).astype(float)
    truth = dict(zip(dates.strftime("%Y-%m-%d"), y, strict=True))
    frame = pd.DataFrame(
        {"date": dates, "product_category": "a", "customer_state": "SP", "orders": y}
    )
    frame.loc[28:, "orders"] = np.nan
    return frame, pd.Series(y[28:], index=frame.index[28:]), truth


def _register(model: Stub) -> str:
    with mlflow.start_run():
        info = mlflow.pyfunc.log_model(
            name="model", python_model=model, pip_requirements=["mlflow"]
        )
    return str(mlflow.register_model(info.model_uri, STUB_NAME).version)


def _aliases() -> dict[str, str]:
    return {k: str(v) for k, v in MlflowClient().get_registered_model(STUB_NAME).aliases.items()}


def test_promote_better_replaces_worse_and_nan_or_negative_blocks(
    mlflow_uri: str, stub_window: Any
) -> None:
    frame, actual, truth = stub_window
    client = MlflowClient()
    weak = _register(Stub(truth, 0.5, 3.0))
    assert promote_forecast(client, STUB_NAME, weak, frame, actual).promoted
    good = _register(Stub(truth, 1.0, 0.5))
    d = promote_forecast(client, STUB_NAME, good, frame, actual)
    assert d.promoted and d.challenger_wape < (d.champion_wape or 0)
    worse = _register(Stub(truth, 0.5, 3.0))
    d = promote_forecast(client, STUB_NAME, worse, frame, actual)
    assert not d.promoted and "wape" in d.reason
    for stub, failure in [
        (Stub(truth, 1.0, 0.0, nan=True), "nan"),
        (Stub(truth, 1.0, -50.0), "negative"),
    ]:
        v = _register(stub)
        d = promote_forecast(client, STUB_NAME, v, frame, actual)
        assert not d.promoted and failure in d.reason
    assert _aliases()["champion"] == good


def test_promote_manual_mode_sets_challenger_only(
    mlflow_uri: str, stub_window: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    frame, actual, truth = stub_window
    v = _register(Stub(truth, 1.0, 0.0))
    d = promote_forecast(MlflowClient(), STUB_NAME, v, frame, actual, mode="manual")
    assert not d.promoted and _aliases() == {"challenger": v}
    assert f"retail-ml promote {STUB_NAME} --version {v}" in capsys.readouterr().out

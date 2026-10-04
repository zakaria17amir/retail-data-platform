import logging

import numpy as np
import pandas as pd
import pytest

from retail_ml.forecast.features import (
    FEATURES,
    HORIZON,
    LAGS,
    demand_features,
    modelled_series,
    seasonal_naive,
    wape,
    window,
)

VOCAB = {"product_category": ["a", "b"], "customer_state": ["RJ", "SP"]}


def _series(cat: str, state: str, values: object, start: str = "2018-01-01") -> pd.DataFrame:
    orders = np.asarray(values, dtype=float)
    return pd.DataFrame(
        {
            "date": pd.date_range(start, periods=len(orders), freq="D"),
            "product_category": cat,
            "customer_state": state,
            "orders": orders,
        }
    )


def test_no_feature_uses_a_lag_shorter_than_the_horizon() -> None:
    assert HORIZON == 28
    assert LAGS == (28, 35, 42, 56)
    lag_cols = [c for c in FEATURES if c.startswith("lag_")]
    assert sorted(int(c.removeprefix("lag_")) for c in lag_cols) == [28, 35, 42, 56]


def test_features_on_day_d_ignore_everything_after_d_minus_28() -> None:
    rng = np.random.default_rng(1)
    df = _series("a", "SP", rng.poisson(5, 150))
    d = df["date"].iloc[120]
    base = demand_features(df, VOCAB)
    changed = df.copy()
    changed.loc[changed["date"] > d - pd.Timedelta(days=28), "orders"] = 999.0
    row = df["date"] == d
    pd.testing.assert_frame_equal(base[row], demand_features(changed, VOCAB)[row])


def test_lag_and_rolling_values() -> None:
    values = np.arange(100, dtype=float)
    df = _series("a", "SP", values)
    f = demand_features(df, VOCAB)
    i = 90
    assert f["lag_28"].iloc[i] == values[i - 28]
    assert f["lag_56"].iloc[i] == values[i - 56]
    assert f["rmean_7_lag_28"].iloc[i] == pytest.approx(values[i - 34 : i - 27].mean())
    assert f["rmean_28_lag_28"].iloc[i] == pytest.approx(values[i - 55 : i - 27].mean())
    assert np.isnan(f["lag_28"].iloc[27]) and f["lag_28"].iloc[28] == 0.0
    d = df["date"].iloc[i]
    assert (f["dow"].iloc[i], f["month"].iloc[i], f["day"].iloc[i]) == (
        d.dayofweek,
        d.month,
        d.day,
    )


def test_features_follow_the_input_index_and_series_boundaries() -> None:
    df = pd.concat([_series("a", "SP", np.zeros(60)), _series("b", "RJ", np.ones(60))])
    df = df.reset_index(drop=True).sample(frac=1.0, random_state=0)
    f = demand_features(df, VOCAB)
    assert f.index.equals(df.index)
    late = df["date"] >= pd.Timestamp("2018-01-29")
    assert (f.loc[late & (df["product_category"] == "b"), "lag_28"] == 1.0).all()
    assert (f.loc[late & (df["product_category"] == "a"), "lag_28"] == 0.0).all()
    assert list(f.columns) == FEATURES


def test_series_ids_are_categoricals_with_a_fixed_vocabulary() -> None:
    df = pd.concat([_series("a", "SP", [1.0]), _series("new", "AM", [1.0])])
    f = demand_features(df.reset_index(drop=True), VOCAB)
    assert list(f["product_category"].cat.categories) == ["a", "b"]
    assert f["product_category"].iloc[0] == "a"
    assert pd.isna(f["product_category"].iloc[1]) and pd.isna(f["customer_state"].iloc[1])


def test_seasonal_naive_repeats_the_last_observed_week_over_the_horizon() -> None:
    hist = np.arange(1, 22, dtype=float)
    df = pd.concat(
        [
            _series("a", "SP", [*hist, *[np.nan] * 28]),
            _series("b", "RJ", [*(hist * 10), *([5.0] * 28)]),
        ]
    ).reset_index(drop=True)
    cutoff = pd.Timestamp("2018-01-22")
    f = seasonal_naive(df, cutoff)
    future = df["date"] >= cutoff
    assert f[~future].isna().all()
    a = f[future & (df["product_category"] == "a")].to_numpy()
    b = f[future & (df["product_category"] == "b")].to_numpy()
    np.testing.assert_array_equal(a, np.tile(hist[-7:], 4))
    np.testing.assert_array_equal(b, np.tile(hist[-7:] * 10, 4))
    # y[t - 7 * ceil(h / 7)]: h = 1 -> C - 7, h = 8 -> C - 7, h = 28 -> C - 1
    assert (a[0], a[7], a[27]) == (hist[14], hist[14], hist[20])


def test_seasonal_naive_counts_days_before_a_series_start_as_zero() -> None:
    cutoff = pd.Timestamp("2018-01-22")
    df = pd.concat(
        [
            _series("a", "SP", [*np.arange(1, 22), *[np.nan] * 28]),
            _series("b", "RJ", [4.0, 4.0, 4.0, *[np.nan] * 28], start="2018-01-19"),
        ]
    ).reset_index(drop=True)
    f = seasonal_naive(df, cutoff)
    b = f[(df["date"] >= cutoff) & (df["product_category"] == "b")].to_numpy()
    np.testing.assert_array_equal(b, np.tile([0, 0, 0, 0, 4, 4, 4], 4))


def test_window_masks_actuals_from_the_cutoff_and_ends_after_the_horizon() -> None:
    df = _series("a", "SP", np.arange(100, dtype=float))
    cutoff = pd.Timestamp("2018-02-01")
    w = window(df, cutoff)
    assert w["date"].max() == cutoff + pd.Timedelta(days=HORIZON - 1)
    assert w.loc[w["date"] >= cutoff, "orders"].isna().all()
    assert w.loc[w["date"] < cutoff, "orders"].notna().all()
    assert df["orders"].notna().all()


def test_wape() -> None:
    assert wape(np.array([1.0, 2.0, 3.0]), np.array([1.0, 2.0, 6.0])) == pytest.approx(0.5)
    assert np.isnan(wape(np.zeros(3), np.ones(3)))


def test_wape_drops_nan_pairs_and_logs_the_count(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING, logger="retail_ml.forecast.features"):
        value = wape(np.array([1.0, 2.0, np.nan]), np.array([1.5, np.nan, 3.0]))
    assert value == pytest.approx(0.5)
    assert "2 of 3" in caplog.text


def test_modelled_series_reports_the_excluded_ones() -> None:
    df = pd.concat(
        [
            _series("a", "SP", [1.0, 2.0]).assign(is_modelled=True),
            _series("b", "RJ", [0.0, 1.0]).assign(is_modelled=False),
        ]
    )
    modelled, excluded = modelled_series(df)
    assert set(modelled["product_category"]) == {"a"}
    assert "is_modelled" not in modelled
    assert excluded.to_dict("records") == [
        {"product_category": "b", "customer_state": "RJ", "orders": 1.0, "days": 2}
    ]

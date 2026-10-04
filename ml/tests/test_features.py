import math

import numpy as np
import pandas as pd
import pytest
from conftest import raw_order

from retail_ml.features import (
    CATEGORICAL,
    ORDER_FEATURES,
    SELLER_FEATURES,
    UNKNOWN,
    OrderFeatures,
    haversine_km,
    order_features,
)


def test_haversine_one_degree_of_longitude_on_equator() -> None:
    assert haversine_km(0.0, 0.0, 0.0, 1.0) == pytest.approx(111.19, abs=0.01)


def test_haversine_sao_paulo_rio() -> None:
    assert haversine_km(-23.55, -46.63, -22.91, -43.17) == pytest.approx(361, abs=5)


def test_order_features_columns_and_values() -> None:
    out = order_features(pd.DataFrame([raw_order()]))
    assert list(out.columns) == ORDER_FEATURES
    row = out.iloc[0]
    assert row["freight_ratio"] == pytest.approx(0.2)
    assert row["distance_km"] == pytest.approx(361, abs=5)
    assert row["estimated_days"] == pytest.approx(14.5625)
    assert (row["approve_hour"], row["approve_dow"], row["approve_month"]) == (10, 2, 1)
    assert row["same_state"] == 0
    assert row["n_items"] == 2 and row["payment_installments"] == 3


def test_order_features_is_pure_and_deterministic() -> None:
    df = pd.DataFrame([raw_order(), raw_order(order_id="o2", customer_state="RJ")])
    before = df.copy()
    a, b = order_features(df), order_features(df)
    pd.testing.assert_frame_equal(a, b)
    pd.testing.assert_frame_equal(df, before)
    assert a["same_state"].tolist() == [0, 1]


def test_zero_price_and_missing_coordinates_give_nan_not_errors() -> None:
    out = order_features(pd.DataFrame([raw_order(total_price=0.0, customer_lat=None)]))
    assert math.isnan(out.loc[0, "freight_ratio"])
    assert math.isnan(out.loc[0, "distance_km"])


def test_categoricals_have_explicit_unknown_level_and_nulls_map_to_it() -> None:
    out = order_features(pd.DataFrame([raw_order(payment_type=None)]))
    for col in CATEGORICAL:
        assert isinstance(out[col].dtype, pd.CategoricalDtype)
        assert UNKNOWN in out[col].cat.categories
    assert out.loc[0, "payment_type"] == UNKNOWN


def test_unseen_category_maps_to_unknown_with_fixed_vocabulary() -> None:
    vocab = {col: ["SP", "RJ", "toys", "credit_card"] for col in CATEGORICAL}
    out = order_features(pd.DataFrame([raw_order(product_category="brand_new")]), vocab)
    assert out.loc[0, "product_category"] == UNKNOWN
    assert out.loc[0, "customer_state"] == "SP"
    assert list(out["product_category"].cat.categories) == [*vocab["product_category"], UNKNOWN]


def test_transformer_learns_vocabulary_and_handles_unknown_seller() -> None:
    train = pd.DataFrame([raw_order(), raw_order(order_id="o2", product_category="health_beauty")])
    for col in SELLER_FEATURES:
        train[col] = 1.0
    tf = OrderFeatures().fit(train)
    new = pd.DataFrame([raw_order(product_category="brand_new", seller_state="AM")])
    out = tf.transform(new)
    assert list(out.columns) == ORDER_FEATURES + SELLER_FEATURES
    assert out.loc[0, "product_category"] == UNKNOWN
    assert out.loc[0, "seller_state"] == UNKNOWN
    assert list(out["product_category"].cat.categories) == ["health_beauty", "toys", UNKNOWN]
    assert np.isnan(out[SELLER_FEATURES].to_numpy(dtype=float)).all()

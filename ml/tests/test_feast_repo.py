from pathlib import Path

import pandas as pd
import pytest
from conftest import local_store

from retail_ml.config import redis_connection
from retail_ml.data import historical_seller_features, materialize
from retail_ml.features import SELLER_FEATURES


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("redis://redis:6379/0", "redis:6379,db=0"),
        ("redis://:s3cr%40t@redis:6380/2", "redis:6380,db=2,password=s3cr@t"),
        ("redis://default:pw@redis", "redis:6379,username=default,password=pw"),
        ("rediss://cache.example:6390", "cache.example:6390,ssl=true"),
        ("redis:6379", "redis:6379"),
        (None, "localhost:6379"),
    ],
)
def test_redis_url_becomes_feast_connection_string(
    url: str | None, expected: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    if url is None:
        monkeypatch.delenv("REDIS_URL", raising=False)
    else:
        monkeypatch.setenv("REDIS_URL", url)
    assert redis_connection() == expected


def _ts(s: str) -> pd.Timestamp:
    return pd.Timestamp(s, tz="UTC")


def _write_history(gold_dir: Path) -> None:
    # Snapshot on day D counts only deliveries before D 00:00; the D+1 row includes a delivery
    # made after the order's approval and must never be joined to it.
    rows = [
        ("s1", "2018-01-09", 1, 0.10, 5.0, "2018-01-09 02:00"),
        ("s1", "2018-01-10", 2, 0.20, 6.0, "2018-01-10 02:00"),
        ("s1", "2018-01-10", 3, 0.25, 6.5, "2018-01-10 05:00"),
        ("s1", "2018-01-11", 9, 0.90, 9.0, "2018-01-11 02:00"),
        ("s2", "2018-01-12", 4, 0.40, 7.0, "2018-01-12 02:00"),
    ]
    df = pd.DataFrame(rows, columns=["seller_id", "feature_ts", *SELLER_FEATURES, "created_ts"])
    df["feature_ts"] = pd.to_datetime(df["feature_ts"], utc=True)
    df["created_ts"] = pd.to_datetime(df["created_ts"], utc=True)
    (gold_dir / "ml").mkdir(parents=True, exist_ok=True)
    df.to_parquet(gold_dir / "ml" / "seller_features_daily.parquet", index=False)


def test_point_in_time_join_never_uses_a_snapshot_after_approval(
    gold_dir: Path, tmp_path: Path
) -> None:
    _write_history(gold_dir)
    store = local_store(tmp_path)
    orders = pd.DataFrame(
        {
            "order_id": ["a", "b", "c", "d"],
            "seller_id": ["s1", "s1", "s2", "unknown"],
            "order_approved_ts_utc": [
                _ts("2018-01-10 10:30"),
                _ts("2018-01-10 23:59"),
                _ts("2018-01-11 08:00"),
                _ts("2018-01-10 10:30"),
            ],
        }
    )
    out = historical_seller_features(store, orders).set_index("order_id")
    assert list(out.index) == ["a", "b", "c", "d"]
    # bounded-memory path: time-sorted chunks give the same rows in the caller's order
    chunked = historical_seller_features(store, orders, chunk_size=1).set_index("order_id")
    pd.testing.assert_frame_equal(chunked, out)
    # latest snapshot dated <= approval, created_ts breaks the same-day tie; never the 01-11 row
    assert out.loc["a", "seller_orders_90d"] == 3
    assert out.loc["b", "seller_late_rate_90d"] == 0.25
    # seller with no snapshot before approval / unseen seller -> missing, not an error
    assert out.loc[["c", "d"], SELLER_FEATURES].isna().all().all()


def test_materialize_loads_latest_snapshot_into_online_store(
    gold_dir: Path, tmp_path: Path
) -> None:
    _write_history(gold_dir)
    store = local_store(tmp_path)
    materialize(store, gold_dir / "ml" / "seller_features_daily.parquet")
    online = store.get_online_features(
        features=[f"seller_stats:{c}" for c in SELLER_FEATURES],
        entity_rows=[{"seller_id": "s1"}, {"seller_id": "s2"}],
    ).to_df()
    assert online["seller_orders_90d"].tolist() == [9, 4]

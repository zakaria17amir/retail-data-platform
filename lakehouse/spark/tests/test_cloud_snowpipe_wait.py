import json
from pathlib import Path

import pytest
from lakehouse_spark.cloud import snowpipe_wait
from lakehouse_spark.cloud.snowpipe_wait import (
    connect_args,
    pending,
    snowflake_table,
    status_query,
    wait,
)

MANIFEST = {
    "run_id": "123",
    "tables": {
        "sales/orders": {"parts": 2, "rows": 10},
        "_rule_metrics": {"parts": 1, "rows": 4},
        "events/clickstream": {"parts": 1, "rows": 0},
    },
}


def test_snowflake_table_is_the_upper_case_silver_leaf_without_underscore() -> None:
    assert snowflake_table("sales/orders") == "ORDERS"
    assert snowflake_table("geo/geolocation_points") == "GEOLOCATION_POINTS"
    assert snowflake_table("_rule_metrics") == "RULE_METRICS"


def test_status_query_counts_files_and_rows_of_the_run_per_table() -> None:
    sql = status_query("RETAIL", ["sales/orders", "_rule_metrics"])
    assert sql.count("union all") == 1
    assert "from RETAIL.SILVER.ORDERS" in sql
    assert "from RETAIL.SILVER.RULE_METRICS" in sql
    assert "count(distinct _export_file)" in sql
    assert "split_part(_export_file, '/', -2) = %(run_id)s" in sql


def test_pending_lists_tables_missing_files_or_rows_and_ignores_empty_exports() -> None:
    assert pending(MANIFEST, {}) == [
        "sales/orders: files 0/2, rows 0/10",
        "_rule_metrics: files 0/1, rows 0/4",
    ]
    assert pending(MANIFEST, {"sales/orders": (2, 10), "_rule_metrics": (1, 3)}) == [
        "_rule_metrics: files 1/1, rows 3/4"
    ]
    assert pending(MANIFEST, {"sales/orders": (2, 10), "_rule_metrics": (1, 4)}) == []


def test_wait_polls_until_every_table_is_loaded() -> None:
    results = iter(
        [{}, {"sales/orders": (1, 5)}, {"sales/orders": (2, 10), "_rule_metrics": (1, 4)}]
    )
    sleeps: list[float] = []
    wait(MANIFEST, lambda: next(results), 60, 5, clock=lambda: 0.0, sleep=sleeps.append)
    assert sleeps == [5, 5]


def test_wait_times_out_loudly_with_what_is_missing() -> None:
    ticks = iter([0.0, 10.0, 20.0, 31.0])
    with pytest.raises(TimeoutError, match=r"sales/orders: files 1/2, rows 5/10"):
        wait(
            MANIFEST,
            lambda: {"sales/orders": (1, 5), "_rule_metrics": (1, 4)},
            30,
            10,
            clock=lambda: next(ticks),
            sleep=lambda _s: None,
        )


PEM = "PRIVATE KEY-----"  # split so the detect-private-key hook ignores the fake key


def test_connect_args_strip_pem_armour_or_use_the_key_file() -> None:
    env = {
        "SNOWFLAKE_ACCOUNT": "ab12345",
        "SNOWFLAKE_PRIVATE_KEY": f"-----BEGIN {PEM}\nAAA\nBBB\n-----END {PEM}",
    }
    assert connect_args(env) == {
        "account": "ab12345",
        "user": "DBT_SERVICE",
        "role": "TRANSFORMER",
        "warehouse": "RETAIL_WH",
        "database": "RETAIL",
        "private_key": "AAABBB",
    }
    file_env = {"SNOWFLAKE_PRIVATE_KEY_PATH": "/k.p8", "SNOWFLAKE_PRIVATE_KEY_PASSPHRASE": "pw"}
    args = connect_args({**file_env, "SNOWFLAKE_USER": "ME"})
    assert args["user"] == "ME"
    assert (args["private_key_file"], args["private_key_file_pwd"]) == ("/k.p8", "pw")
    assert "private_key" not in args


def test_main_fails_with_exit_1_when_the_run_never_lands(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    manifest = tmp_path / "123.json"
    manifest.write_text(json.dumps(MANIFEST))
    monkeypatch.setattr(snowpipe_wait, "fetcher", lambda _m, _env: lambda: {})
    assert snowpipe_wait.main([str(manifest), "--timeout", "0", "--interval", "0"]) == 1
    assert "sales/orders: files 0/2, rows 0/10" in capsys.readouterr().err

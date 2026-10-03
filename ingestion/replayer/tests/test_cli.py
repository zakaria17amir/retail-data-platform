from pathlib import Path

import pytest
from replayer.cli import main


def test_seed_missing_csv_reports_filename(
    tmp_csv_dir: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    (tmp_csv_dir / "olist_sellers_dataset.csv").unlink()
    assert main(["seed", "--data-dir", str(tmp_csv_dir)]) == 1
    assert "olist_sellers_dataset.csv" in capsys.readouterr().err


def test_malformed_dsn_reports_cleanly(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "not-a-dsn")
    assert main(["status"]) == 1
    err = capsys.readouterr().err
    assert "invalid POSTGRES_DSN" in err
    assert "Traceback" not in err


@pytest.mark.parametrize(
    ("argv", "env", "message"),
    [
        (["live", "--speed", "0"], {}, "--speed must be > 0"),
        (["live", "--speed", "-5"], {}, "--speed must be > 0"),
        (["live"], {"REPLAY_SPEED": "fast"}, "REPLAY_SPEED must be a number"),
        (["live", "--from", "2017-01-01T00:00:00+00:00"], {}, "naive datetimes"),
    ],
)
def test_live_rejects_invalid_arguments(
    argv: list[str],
    env: dict[str, str],
    message: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    assert main(argv) == 1
    assert message in capsys.readouterr().err

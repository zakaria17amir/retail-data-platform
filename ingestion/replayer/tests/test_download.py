from pathlib import Path

import pytest
from replayer.cli import main
from replayer.schema import TABLES


def _no_credentials(monkeypatch: pytest.MonkeyPatch, home: Path) -> None:
    monkeypatch.setenv("KAGGLE_USERNAME", "")
    monkeypatch.setenv("KAGGLE_KEY", "")
    monkeypatch.setattr(Path, "home", lambda: home)


def test_download_without_credentials_exits_2(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _no_credentials(monkeypatch, tmp_path)
    assert main(["download", "--dest", str(tmp_path / "raw")]) == 2
    assert "KAGGLE_USERNAME" in capsys.readouterr().err


def test_download_skips_when_present(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _no_credentials(monkeypatch, tmp_path)
    for t in TABLES:
        (tmp_path / t.csv_file).touch()

    def fail(*args: object, **kwargs: object) -> str:
        raise AssertionError("kagglehub must not be called")

    monkeypatch.setattr("kagglehub.dataset_download", fail)
    assert main(["download", "--dest", str(tmp_path)]) == 0
    assert "customers: " in capsys.readouterr().out

from pathlib import Path

import pytest
from conftest import GOOD, FakeLLM
from test_sources import fixture_lake

from genai import cli


def test_enrich_cli_prints_throughput(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    fixture_lake(tmp_path)
    monkeypatch.setattr(cli, "make_llm", lambda: FakeLLM([GOOD]))
    assert cli.main(["enrich", "--all", "--root", str(tmp_path)]) == 0
    out = capsys.readouterr().out
    assert "2 accepted, 0 rejected, 0 skipped" in out
    assert "tok/s" in out
    cli.main(["enrich", "--limit", "1", "--category", "toys", "--root", str(tmp_path)])
    assert "0 accepted, 0 rejected, 1 skipped" in capsys.readouterr().out


def test_limit_and_all_are_exclusive() -> None:
    with pytest.raises(SystemExit):
        cli.main(["enrich", "--all", "--limit", "3"])


def test_default_limit_and_root(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ENRICH_LIMIT", "7")
    monkeypatch.setenv("LAKEHOUSE_BUCKET", "lake")
    args = cli.parser().parse_args(["enrich"])
    assert (cli.resolve_limit(args), args.root) == (7, "s3://lake")
    assert cli.resolve_limit(cli.parser().parse_args(["enrich", "--all"])) is None

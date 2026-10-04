from pathlib import Path

import pytest
from conftest import GOOD, FakeLLM
from test_sources import fixture_lake

from genai import cli
from genai.rag import Hit


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


def test_rag_subcommands_parse() -> None:
    p = cli.parser()
    assert p.parse_args(["rag", "init"]).rag_command == "init"
    assert p.parse_args(["rag", "index", "--root", "/lake"]).root == "/lake"
    s = p.parse_args(["rag", "search", "a soft pillow", "--k", "3", "--category", "toys"])
    assert (s.query, s.k, s.category, s.mode) == ("a soft pillow", 3, "toys", "hybrid")
    e = p.parse_args(["rag", "eval"])
    assert (e.n, str(e.queries).endswith("rag_queries.jsonl")) == (100, True)
    with pytest.raises(SystemExit):
        p.parse_args(["rag", "search", "q", "--mode", "bm25"])


def test_rag_search_prints_ranked_hits(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    hit = Hit("p1", "c1", "product_doc", "Wooden Bed Frame\nA bed", 0.0325, 1, 2)
    seen: dict[str, object] = {}

    def fake(query: str, k: int, **kw: object) -> list[Hit]:
        seen.update(query=query, k=k, **kw)
        return [hit]

    monkeypatch.setattr(cli, "search", fake)
    assert cli.main(["rag", "search", "bed", "--k", "5"]) == 0
    assert seen == {"query": "bed", "k": 5, "category": None, "mode": "hybrid"}
    out = capsys.readouterr().out
    assert "1  0.0325  p1  product_doc  v=1 t=2  Wooden Bed Frame A bed" in out


def test_default_limit_and_root(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ENRICH_LIMIT", "7")
    monkeypatch.setenv("LAKEHOUSE_BUCKET", "lake")
    args = cli.parser().parse_args(["enrich"])
    assert (cli.resolve_limit(args), args.root) == (7, "s3://lake")
    assert cli.resolve_limit(cli.parser().parse_args(["enrich", "--all"])) is None

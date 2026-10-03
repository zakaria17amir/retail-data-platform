from pathlib import Path

import pytest
from replayer.cli import main
from replayer.load import strip_bom


@pytest.mark.parametrize(
    ("chunk", "expected"),
    [(b"\xef\xbb\xbfa,b", b"a,b"), (b"a,b", b"a,b"), (b"", b"")],
)
def test_strip_bom(chunk: bytes, expected: bytes) -> None:
    assert strip_bom(chunk) == expected


def test_seed_missing_csv_reports_filename(
    tmp_csv_dir: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    (tmp_csv_dir / "olist_sellers_dataset.csv").unlink()
    assert main(["seed", "--data-dir", str(tmp_csv_dir)]) == 1
    assert "olist_sellers_dataset.csv" in capsys.readouterr().err

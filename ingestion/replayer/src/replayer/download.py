import csv
import os
import shutil
from pathlib import Path

from replayer.schema import TABLES

DATASET = "olistbr/brazilian-ecommerce"


class MissingCredentials(RuntimeError):
    pass


def _has_credentials() -> bool:
    if os.environ.get("KAGGLE_USERNAME") and os.environ.get("KAGGLE_KEY"):
        return True
    return (Path.home() / ".kaggle" / "kaggle.json").exists()


def download(dest: Path) -> Path:
    if all((dest / t.csv_file).exists() for t in TABLES):
        return dest
    if not _has_credentials():
        raise MissingCredentials("Kaggle credentials not found")

    import kagglehub  # type: ignore[import-untyped]

    source = Path(kagglehub.dataset_download(DATASET))
    missing = [t.csv_file for t in TABLES if not (source / t.csv_file).exists()]
    if missing:
        raise FileNotFoundError(f"missing CSV files in download {source}: {', '.join(missing)}")
    dest.mkdir(parents=True, exist_ok=True)
    for t in TABLES:
        shutil.copyfile(source / t.csv_file, dest / t.csv_file)
    return dest


def csv_row_counts(directory: Path) -> dict[str, int]:
    counts: dict[str, int] = {}
    for t in TABLES:
        with (directory / t.csv_file).open(encoding="utf-8", newline="") as handle:
            counts[t.name] = max(sum(1 for _ in csv.reader(handle)) - 1, 0)
    return counts

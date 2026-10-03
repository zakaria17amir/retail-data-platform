import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path

import psycopg
from dotenv import find_dotenv, load_dotenv
from psycopg.conninfo import conninfo_to_dict

from replayer.download import MissingCredentials, csv_row_counts, download
from replayer.live import run_live
from replayer.load import row_counts, seed
from replayer.sample import sample

DEFAULT_DSN = "postgresql://retail:retail@127.0.0.1:5432/retail"
MISSING_CREDENTIALS_MESSAGE = (
    "error: set KAGGLE_USERNAME and KAGGLE_KEY in .env (see docs/runbooks/windows-setup.md)"
)


def _print_counts(counts: dict[str, int]) -> None:
    for name, count in counts.items():
        print(f"{name}: {count}")


def _build_parser() -> argparse.ArgumentParser:
    default_dir = Path(os.environ.get("OLIST_DATA_DIR", "data/raw/olist"))
    parser = argparse.ArgumentParser(prog="replayer")
    commands = parser.add_subparsers(dest="command", required=True)
    seed_parser = commands.add_parser("seed", help="bulk load Olist CSVs into Postgres")
    seed_parser.add_argument("--data-dir", type=Path, default=default_dir)
    commands.add_parser("status", help="print row counts per Olist table")
    download_parser = commands.add_parser("download", help="download Olist CSVs from Kaggle")
    download_parser.add_argument("--dest", type=Path, default=default_dir)
    sample_parser = commands.add_parser("sample", help="write a small consistent Olist sample")
    sample_parser.add_argument("--src", type=Path, default=default_dir)
    sample_parser.add_argument("--dest", type=Path, required=True)
    sample_parser.add_argument("--n-orders", type=int, required=True)
    sample_parser.add_argument("--seed", type=int, default=42)
    live_parser = commands.add_parser(
        "live", help="replay Olist history into Postgres in real time"
    )
    live_parser.add_argument("--data-dir", type=Path, default=default_dir)
    live_parser.add_argument("--from", dest="start", type=datetime.fromisoformat)
    live_parser.add_argument("--until", type=datetime.fromisoformat)
    live_parser.add_argument(
        "--speed", type=float, default=float(os.environ.get("REPLAY_SPEED", "3600"))
    )
    live_parser.add_argument("--loop", action="store_true")
    live_parser.add_argument("--summary-file", type=Path)
    return parser


def _run(args: argparse.Namespace) -> dict[str, int]:
    if args.command == "download":
        return csv_row_counts(download(args.dest))
    if args.command == "sample":
        return sample(args.src, args.dest, args.n_orders, args.seed)
    dsn = os.environ.get("POSTGRES_DSN", DEFAULT_DSN)
    if args.command == "seed":
        return seed(dsn, args.data_dir)
    if args.command == "live":
        counts = run_live(
            dsn,
            args.data_dir,
            start=args.start,
            until=args.until,
            speed=args.speed,
            loop=args.loop,
        )
        if args.summary_file:
            args.summary_file.write_text(json.dumps(counts), encoding="utf-8")
        return counts
    return row_counts(dsn)


def main(argv: list[str] | None = None) -> int:
    load_dotenv(find_dotenv(usecwd=True))
    args = _build_parser().parse_args(argv)
    try:
        _print_counts(_run(args))
    except MissingCredentials:
        print(MISSING_CREDENTIALS_MESSAGE, file=sys.stderr)
        return 2
    except psycopg.OperationalError:
        info = conninfo_to_dict(os.environ.get("POSTGRES_DSN", DEFAULT_DSN))
        target = f"{info.get('host')}:{info.get('port')}/{info.get('dbname')}"
        print(f"error: could not connect to {target}", file=sys.stderr)
        return 1
    except psycopg.ProgrammingError:
        print("error: invalid POSTGRES_DSN", file=sys.stderr)
        return 1
    except (FileNotFoundError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

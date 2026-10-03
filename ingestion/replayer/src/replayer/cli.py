import argparse
import os
import sys
from pathlib import Path

import psycopg
from dotenv import find_dotenv, load_dotenv
from psycopg.conninfo import conninfo_to_dict

from replayer.load import row_counts, seed

DEFAULT_DSN = "postgresql://retail:retail@localhost:5432/retail"


def _print_counts(counts: dict[str, int]) -> None:
    for name, count in counts.items():
        print(f"{name}: {count}")


def main(argv: list[str] | None = None) -> int:
    load_dotenv(find_dotenv(usecwd=True))
    parser = argparse.ArgumentParser(prog="replayer")
    commands = parser.add_subparsers(dest="command", required=True)
    seed_parser = commands.add_parser("seed", help="bulk load Olist CSVs into Postgres")
    seed_parser.add_argument(
        "--data-dir", type=Path, default=Path(os.environ.get("OLIST_DATA_DIR", "data/raw/olist"))
    )
    commands.add_parser("status", help="print row counts per Olist table")
    args = parser.parse_args(argv)

    dsn = os.environ.get("POSTGRES_DSN", DEFAULT_DSN)
    try:
        if args.command == "seed":
            _print_counts(seed(dsn, args.data_dir))
        else:
            _print_counts(row_counts(dsn))
    except psycopg.OperationalError:
        info = conninfo_to_dict(dsn)
        target = f"{info.get('host')}:{info.get('port')}/{info.get('dbname')}"
        print(f"error: could not connect to {target}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

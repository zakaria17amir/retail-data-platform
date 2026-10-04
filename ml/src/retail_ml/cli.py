"""`retail-ml` entry point."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from dataclasses import asdict
from pathlib import Path

from mlflow import MlflowClient

from retail_ml import config
from retail_ml.data import apply_feature_definitions, feature_store, materialize


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="retail-ml")
    sub = parser.add_subparsers(dest="command", required=True)
    train = sub.add_parser("train", help="train, register and promote a model")
    train.add_argument("model", choices=["late_delivery"])
    train.add_argument("--config", type=Path, default=None)
    train.add_argument("--promotion-mode", choices=["auto", "manual"], default=None)
    promote = sub.add_parser("promote", help="human approval: set the champion alias")
    promote.add_argument("model", choices=["late_delivery"])
    promote.add_argument("--version", required=True)
    sub.add_parser("materialize", help="apply Feast definitions and load the online store")
    monitor = sub.add_parser("monitor", help="drift + delayed ground truth; exit 3 on breach")
    monitor.add_argument("model", choices=["late_delivery"])
    monitor.add_argument("--config", type=Path, default=None)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "promote":
        from retail_ml.late_delivery.promote import approve

        approve(MlflowClient(), args.model, args.version)
        print(f"{args.model} v{args.version} -> champion")
        return 0

    store = feature_store()
    apply_feature_definitions(store, config.feast_repo_path())
    if args.command == "materialize":
        materialize(store, config.gold_dir() / "ml" / "seller_features_daily.parquet")
        return 0

    cfg = config.load_late_delivery_config(args.config)
    if args.command == "monitor":
        from retail_ml.monitoring.monitor import monitor_late_delivery

        return monitor_late_delivery(cfg, store, MlflowClient())

    from retail_ml.late_delivery.train import train

    result = train(cfg, store, promotion_mode=args.promotion_mode or config.promotion_mode())
    print(
        json.dumps(
            {
                "version": result.version,
                "decision": asdict(result.decision),
                "metrics": result.metrics,
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

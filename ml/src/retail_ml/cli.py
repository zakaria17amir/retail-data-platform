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

MODELS = ["late_delivery", "demand_forecast"]


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="retail-ml")
    sub = parser.add_subparsers(dest="command", required=True)
    train = sub.add_parser("train", help="train, register and promote a model")
    train.add_argument("model", choices=MODELS)
    train.add_argument("--config", type=Path, default=None)
    train.add_argument("--promotion-mode", choices=["auto", "manual"], default=None)
    promote = sub.add_parser("promote", help="human approval: set the champion alias")
    promote.add_argument("model", choices=MODELS)
    promote.add_argument("--version", required=True)
    forecast = sub.add_parser("forecast", help="champion demand forecast -> gold Parquet")
    forecast.add_argument("target", choices=["demand"])
    forecast.add_argument("--config", type=Path, default=None)
    score = sub.add_parser("score", help="score open orders with the champion -> gold Parquet")
    score.add_argument("model", choices=["late_delivery"])
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

    if args.command == "forecast" or (args.command == "train" and args.model == "demand_forecast"):
        return _demand(args)

    store = feature_store()
    apply_feature_definitions(store, config.feast_repo_path())
    if args.command == "materialize":
        materialize(store, config.gold_dir() / "ml" / "seller_features_daily.parquet")
        return 0

    if args.command == "score":
        from retail_ml.late_delivery.score import score

        out = config.gold_dir() / "ml" / "pred_late_delivery.parquet"
        training_path = config.load_late_delivery_config().training_path
        scored = score(store, MlflowClient(), training_path, args.model)
        out.parent.mkdir(parents=True, exist_ok=True)
        scored.to_parquet(out, index=False)
        version = scored["model_version"].iloc[0] if len(scored) else None
        print(json.dumps({"rows": len(scored), "model_version": version, "path": str(out)}))
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


def _demand(args: argparse.Namespace) -> int:
    from retail_ml.forecast.train import load_demand_forecast_config

    cfg = load_demand_forecast_config(args.config)
    if args.command == "forecast":
        from retail_ml.forecast.predict import forecast_demand

        out = forecast_demand(cfg, MlflowClient())
        version = str(out["model_version"].iloc[0])
        print(
            json.dumps({"rows": len(out), "model_version": version, "path": str(cfg.output_path)})
        )
        return 0

    from retail_ml.forecast.train import train

    result = train(cfg, promotion_mode=args.promotion_mode or config.promotion_mode())
    print(
        json.dumps(
            {
                "version": result.version,
                "decision": asdict(result.decision),
                "metrics": result.metrics,
                "excluded_series": result.excluded.to_dict("records"),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

# Model card — `demand_forecast` v1

| | |
|---|---|
| Registry | MLflow `demand_forecast`, version 1, aliases `champion` and `challenger` |
| Run | `ed2bf7dcc4d740cb96e8a661af7b1b8f` (run name `lightgbm`), code `dddbd7d` |
| Data | `demand_daily.parquet` sha256 `f3c685717dff3ba0…d6278566a7a4` |
| Owner | `ml-engineer` (code in `ml/src/retail_ml/forecast/`, config `ml/configs/demand_forecast.yaml`) |
| Status | Portfolio / demo model on the public Olist dataset; batch only, feeds Power BI |

## Intended use

28-day daily forecast of distinct revenue orders per product_category × customer_state for
category/regional planning dashboards (`gold/pred_demand_forecast/`). Best suited to aggregate,
high-volume views (the 10 largest series, state or category roll-ups).

Out of scope: SKU-level replenishment, the 1,320 sparse series that are not modelled, or anything
that needs prediction intervals (none are produced).

## Model

One global LightGBM regressor (Poisson objective, 400 trees) over all modelled series. Features:
lags 28/35/42/56 days, 7- and 28-day rolling means lagged 28 days, day-of-week, month, day of month,
and the series ids as categoricals. Every lag is ≥ the horizon, so all 28 days are forecast directly
from history known at the cutoff (no recursion).

## Data window

`demand_daily` from 2016-09-04 to E = 2018-08-27 (the last day before the extract's truncated tail,
dbt rule). Series are modelled when they have ≥ 1 order on ≥ 50 % of the 180 days before the test
cutoff: **62 of 1,382 series, 59.96 % of all series-orders** (61.68 % within the 180-day window). 24
of the 62 are SP, 13 RJ, 11 MG, 7 PR, 6 RS, 1 BA.

Evaluation: rolling-origin backtest with three 28-day folds starting 2018-05-08, 2018-06-05,
2018-07-03, then the test window 2018-07-31 → 2018-08-27 (model refit on everything before it).

## Metrics vs baseline (MLflow, WAPE, lower is better)

| model | fold 1 | fold 2 | fold 3 | backtest mean | test | test, top-10 series |
|---|---|---|---|---|---|---|
| seasonal naive (baseline: last week repeated) | 0.8594 | 0.6826 | 0.7618 | 0.7679 | 0.6604 | 0.4510 |
| **LightGBM v1** | **0.6084** | **0.5566** | **0.6301** | **0.5984** | **0.5704** | **0.4295** |

LightGBM is better on every fold and on test (backtest −22.1 % relative, test −13.6 %), but only
−4.8 % on the ten largest series. Per series it loses to naive on 13 of 62 (19.45 % of test orders),
including housewares × SP and furniture_decor × SP in the top 10
(`ds/reports/error_analysis.md`).

## Calibration / bias

Point forecasts only. Over the test window LightGBM under-forecasts total volume (sum forecast / sum
actual 0.7467; naive 0.9190), most visibly for series whose level jumped in the last weeks before the
cutoff. The Poisson objective keeps forecasts non-negative; the promotion test also checks for
NaN/negatives.

## Limitations

- **Truncated extract tail inside the test window.** The last days (2018-08-24…27) still carry
  ramp-down volumes (daily totals 70/55/56/56 vs ~130–200 earlier); week-4 WAPE is 1.1068 (naive) /
  0.7829 (LightGBM) vs ≤ 0.6176 in weeks 1–3. Part of the test WAPE is extract artefact, not demand.
- **Slow to react to level shifts**: the most recent information is 28 days old, so jumps in the last
  four weeks before the cutoff are missed (where it loses to naive).
- Sparse series (≥ 50 % zero days) have WAPE ≈ 1 or worse for both models; per-series numbers for
  small series are noisy (≤ ~25 test orders).
- Only ~60 % of series-orders are covered; roll-ups that include unmodelled series are incomplete.
- One test window plus three backtest folds, all in May–Aug 2018; no Black-Friday/holiday period is
  in any evaluation window.
- No prediction intervals; WAPE is volume-weighted, so it is dominated by SP.

## Ethics and regional fairness

No personal data. Coverage is regionally skewed: 61 of 62 modelled series are in the South/Southeast
(SP, RJ, MG, PR, RS) and one in BA, so dashboards built on the forecast under-represent the North
and North-east. Present unmodelled regions as "not forecast", not as zero demand.

## Monitoring

The `forecast_demand` DAG (`retail-ml forecast demand`) writes test-window and forward forecasts with
`model_version`; the weekly `train_demand_forecast` DAG retrains and promotes only if the challenger
beats the champion's WAPE on the same window and passes the model tests (manual approval mode
available). v1 has no drift monitor of its own; track test-window WAPE per run in MLflow.

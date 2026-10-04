# Model card — `demand_forecast` v1 (champion), v2 (challenger)

| | |
|---|---|
| Registry | MLflow `demand_forecast`: version 1 alias `champion`, version 2 alias `challenger` (not promoted) |
| Runs | v1 `ed2bf7dcc4d740cb96e8a661af7b1b8f`, code `dddbd7d`; v2 `b296acbb67204f489160bd278c6068f9`, code `b64c4f8` (both run name `lightgbm`) |
| Data | current `demand_daily.parquet` sha256 `bc78232b2606f51b…7c26a51d0` (tail cut at 0.5 × trailing mean); v1 was trained on `f3c685717dff3ba0…d6278566a7a4` (0.2 cut) |
| Owner | `ml-engineer` (code in `ml/src/retail_ml/forecast/`, config `ml/configs/demand_forecast.yaml`) |
| Status | Portfolio / demo model on the public Olist dataset; batch only, feeds Power BI |

## Intended use

28-day daily forecast of distinct revenue orders per product_category × customer_state for
category/regional planning dashboards (`data/gold/ml/pred_demand_forecast.parquet`). Best suited to aggregate views
(state or category roll-ups); on the 10 largest series the seasonal-naive baseline is currently
better (see metrics).

Out of scope: SKU-level replenishment, the 1,321 sparse series that are not modelled, or anything
that needs prediction intervals (none are produced).

## Model

One global LightGBM regressor (Poisson objective, 400 trees) over all modelled series. Features:
lags 28/35/42/56 days, 7- and 28-day rolling means lagged 28 days, day-of-week, month, day of month,
and the series ids as categoricals. Every lag is ≥ the horizon, so all 28 days are forecast directly
from history known at the cutoff (no recursion).

## Data window

`demand_daily` from 2016-09-04 to E = 2018-08-23: the last day whose total distinct revenue orders
are ≥ 0.5 × the previous 28-day mean (dbt `ml_demand_min_ratio`, raised from 0.2, which kept the
ramp-down days 2018-08-24…27 at 70/55/56/56 orders/day). Series are modelled when they have ≥ 1
order on ≥ 50 % of the 180 days before the test cutoff: **61 of 1,382 series, 59.86 % of all
series-orders** (61.35 % within the 180-day window). 23 of the 61 are SP, 13 RJ, 11 MG, 7 PR, 6 RS,
1 BA. Versus the 0.2 cut, baby × RJ and musical_instruments × SP dropped out and toys × RJ came in.

Evaluation: rolling-origin backtest with three 28-day folds starting 2018-05-04, 2018-06-01,
2018-06-29, then the test window 2018-07-27 → 2018-08-23 (model refit on everything before it).

## Metrics vs baseline (MLflow, WAPE, lower is better)

Current data (E = 2018-08-23):

| model | fold 1 | fold 2 | fold 3 | backtest mean | test | test, top-10 series |
|---|---|---|---|---|---|---|
| seasonal naive (baseline: last week repeated) | 0.7766 | 0.6756 | 0.8044 | 0.7522 | 0.6349 | **0.4004** |
| LightGBM v2 (challenger) | 0.5946 | 0.5533 | 0.6278 | 0.5919 | 0.5438 | 0.4317 |
| **LightGBM v1 (champion), re-scored** | – | – | – | – | **0.5352** | 0.4184 |

v2 beats naive on every fold and on test (backtest −21.3 % relative, test −14.4 %) but is worse on
the ten largest series (+7.8 %). Promotion re-scores the champion on the challenger's test window:
v1 0.5352 < v2 0.5438, so v2 stays `challenger`. v1 was fit on data before 2018-07-31, so the first
4 test days (07-27…07-30) are in-sample for it; on the 24 days out-of-sample for both (07-31 →
08-23) v1 still wins, 0.5365 vs 0.5414 (naive 0.6250). Per series both v1 and v2 lose to naive on
11 of 61 (39.75 % of test orders), including 6 of the top 10 (bed_bath_table, health_beauty,
furniture_decor, housewares, auto, telephony — all SP).

v1's original evaluation (0.2 cut, E = 2018-08-27, folds from 2018-05-08, test 2018-07-31 →
2018-08-27): naive backtest 0.7679 / test 0.6604 / top-10 0.4510; LightGBM v1 0.5984 / 0.5704 /
0.4295 (per-series detail in `ds/reports/error_analysis.md`).

## Calibration / bias

Point forecasts only. Over the current test window LightGBM under-forecasts total volume (sum
forecast / sum actual v1 0.7041, v2 0.6931; naive 0.9122), most visibly for series whose level
jumped in the last weeks before the cutoff. The Poisson objective keeps forecasts non-negative; the
promotion test also checks for NaN/negatives.

## Limitations

- **Extract tail.** With the 0.5 cut the ramp-down days are out of the test window; week-4 WAPE fell
  from 1.1068 (naive) / 0.7829 (LightGBM) to 0.7055 / 0.5691 (v1 and v2), still above weeks 1–3
  (naive ≤ 0.6274, LightGBM ≤ 0.5473). E = 2018-08-23 has 100 orders across the modelled series vs
  129–169 on the four days before, so a little tail effect may remain.
- **Slow to react to level shifts**: the most recent information is 28 days old, so jumps in the last
  four weeks before the cutoff are missed (where it loses to naive).
- Sparse series (≥ 50 % zero days) have WAPE ≈ 1 or worse for both models; per-series numbers for
  small series are noisy (≤ ~25 test orders).
- Only ~60 % of series-orders are covered; roll-ups that include unmodelled series are incomplete.
- One test window plus three backtest folds, all in May–Aug 2018; no Black-Friday/holiday period is
  in any evaluation window.
- No prediction intervals; WAPE is volume-weighted, so it is dominated by SP.

## Ethics and regional fairness

No personal data. Coverage is regionally skewed: 60 of 61 modelled series are in the South/Southeast
(SP, RJ, MG, PR, RS) and one in BA, so dashboards built on the forecast under-represent the North
and North-east. Present unmodelled regions as "not forecast", not as zero demand.

## Monitoring

The `forecast_demand` DAG (`retail-ml forecast demand`) writes test-window and forward forecasts with
`model_version`; the weekly `train_demand_forecast` DAG retrains and promotes only if the challenger
beats the champion's WAPE on the same window and passes the model tests (manual approval mode
available). v1 has no drift monitor of its own; track test-window WAPE per run in MLflow.

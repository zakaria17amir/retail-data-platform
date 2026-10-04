# Model card — `late_delivery` v1

| | |
|---|---|
| Registry | MLflow `late_delivery`, version 1, aliases `champion` and `challenger` |
| Run | `0d2ff3e361a14c4f93b63c3a9c61116b` (run name `lightgbm`), code `3f75f92` |
| Data | `late_delivery_training.parquet` sha256 `9a59eb46834196fc…24719689`; seller features sha256 `c900104f…f943` |
| Owner | `ml-engineer` (code in `ml/src/retail_ml/late_delivery/`, config `ml/configs/late_delivery.yaml`) |
| Status | Portfolio / demo model on the public Olist dataset; not validated for production decisions |

## Intended use

Rank approved orders by risk of being delivered after the estimated delivery date, so an operations
team can review a **fixed alert budget** (e.g. the riskiest 10 % of orders) for proactive seller
follow-up or customer messaging. Served online by `POST /predict/late-delivery` and scored nightly
into `data/gold/ml/pred_late_delivery.parquet`.

Out of scope: deciding service levels, pricing, credit or fraud for individual customers; treating
the score as a calibrated probability; any use outside Olist-like marketplace data.

## Model and features

LightGBM classifier (native categoricals, early stopping on validation log loss, 190 trees of a
2,000 maximum) inside an sklearn pipeline with the shared `OrderFeatures` step. Inputs are known at
approval time: promised delivery window (`estimated_days`), seller→customer haversine distance,
same-state flag, freight ratio, item and seller counts, payment type and installments, product
category, customer and seller state, approval hour/weekday/month, and point-in-time seller history
from Feast (`seller_orders_90d`, `seller_late_rate_90d`, `seller_avg_delivery_days_90d`, from
deliveries before the approval date).

## Data window

Label `is_late` = delivered after the estimated date; only orders with a delivery timestamp are labelled (96,456 of
99,281 contract rows). Time split by approval timestamp:

| split | approval window | n | late rate |
|---|---|---|---|
| train | 2016-09-15 → 2018-02-28 | 57,110 | 7.59 % |
| validation | 2018-03-01 → 2018-05-31 | 20,679 | 11.94 % |
| test | 2018-06-01 → 2018-08-31 | 18,667 | 5.46 % |

## Metrics vs baselines (MLflow)

| model | val PR-AUC | val ROC-AUC | val Brier | test PR-AUC | test ROC-AUC | test Brier |
|---|---|---|---|---|---|---|
| constant prior (baseline) | 0.1194 | 0.5000 | 0.1071 | 0.0546 | 0.5000 | 0.0521 |
| logistic regression (baseline) | 0.2661 | 0.7214 | 0.0984 | 0.1041 | 0.7059 | 0.0533 |
| **LightGBM v1** | 0.2499 | 0.7079 | 0.1000 | **0.1340** | **0.7633** | **0.0497** |

**Alert budget (top 10 % of scores)**, computed in `ds/notebooks/01_late_delivery_eda.ipynb`: on
test, LightGBM catches 26.7 % of late orders at 14.6 % precision (lift 2.67; logistic 20.0 % at
10.9 %; random 10 %). On validation it catches 25.4 % at 30.4 % precision (logistic 28.6 % at
34.2 %). If the cutoff is set on validation and applied to test, LightGBM flags only 4.3 % of test
orders (recall 13.0 %), because scores shift down with the base rate. Re-set the cutoff per period as
a quantile, not a fixed probability. Recall at precision ≥ 0.5 is ≈ 0 for every model (LightGBM
test 0.0059) and is not a useful operating point.

## Calibration

Trained on a 7.59 % base rate. Mean prediction 0.0866 vs observed 0.1194 on validation (under), 0.0609
vs 0.0546 on test (over). Test reliability: low deciles over-predicted (first decile 0.0152 vs
0.0070), deciles 7–9 under-predicted, top decile over-predicted (0.1798 vs 0.1457). Brier passes the
promotion gate (0.0497 ≤ min(prior 0.0521, logistic 0.0533)) but the probabilities do not transfer across periods. Use the score for
ranking; recalibrate on a recent labelled window before quoting probabilities.

## Explainability

TreeSHAP on 3,000 test orders (`ds/reports/figures/late_delivery_shap_summary.png`): top drivers are
`estimated_days` (longer promise → lower risk), `approve_month` (non-monotonic calendar effect),
`customer_state` (BA/PE/CE raise risk, MG/PR/SP lower it), seller average delivery days (higher →
riskier) and distance (further → riskier). Details: `ds/reports/error_analysis.md`.

## Limitations

- **Label-rate shift between windows.** The late rate is 7.59 % / 11.94 % / 5.46 % across
  train / validation / test (monthly from 1.38 % in 2018-06 to 21.65 % in 2018-03). Lateness is driven
  by period-level logistics events that the features do not capture; any single-window metric is
  period-specific.
- **Validation and test disagree.** Logistic wins every validation metric, LightGBM every test
  metric. One validation window is not enough to rank them reliably; v1 is weakly supported.
- **The test window is not an untouched holdout**: promotion used the test PR-AUC (vs the champion)
  and the test Brier (vs min(prior, logistic)).
- **No embargo between splits.** Splits cut on approval date, but labels arrive at delivery: 3,560
  train rows (6.23 %, 31.97 % of them late) were delivered on/after the validation start and 2,369
  validation rows (11.46 %) on/after the test start. A model deployed on 2018-03-01 could not have
  seen those labels, so validation/test scores are mildly optimistic. Late orders resolve later, so
  they are over-represented among the leaked labels.
- **Segment relationships flipped in test**: short-haul (< 100 km) orders were 4.7 % late in
  development and 12.0 % in test; RJ/BA went from the latest states to average. The model
  under-predicts the former and over-predicts long-haul and north-eastern orders.
- `approve_month` is the 2nd most important feature but each calendar month is seen in only one or
  two years of training data; it partly memorises 2017/18 events (Nov 2017, Feb–Mar 2018).
- Unlabelled orders (not delivered: shipped, canceled, unavailable, …) are excluded from training
  and evaluation; 1–4 % of rows per month since 2017-07 (up to 7 % earlier in 2017).
- 3.7–6.0 % of rows per split have no seller history (new seller or no snapshot within 365 days); they get NaN
  features.

## Ethics and regional fairness

The model uses `customer_state` and `seller_state` directly, and distance/region act as proxies for
income and infrastructure in Brazil. On test, 38.2 % of BA orders are flagged at the 10 % budget vs
9.7 % for SP and 1.2 % for MG, at 7.8 % precision in BA vs 20.4 % in SP: customers in the north-east
would receive most alerts, and most of those alerts would be false. Acceptable for helpful actions
(proactive seller follow-up, honest delivery messaging); **not acceptable** for actions that reduce
service (deprioritising, refusing, surcharging) by region. Before such uses, monitor flagged share
and precision per state (Evidently report / `ml_monitoring`) and consider per-region thresholds or
dropping the state features. No personal attributes (age, gender, income) are used or available.

## Monitoring

Daily Evidently drift and delayed ground-truth performance by week (`monitor_late_delivery` DAG, `ml_monitoring` table); the
`train_late_delivery` DAG retrains on thresholds. Promotion requires a higher test PR-AUC than the
champion re-scored on the same test window, plus the model tests (no NaN, range [0, 1], test Brier ≤
min(constant prior, logistic)); manual approval mode is available (`PROMOTION_MODE=manual`). On the
frozen replay data the drift breach persists (drift share 0.3125 > 0.3) and a retrain cannot clear it,
so `monitor_late_delivery` stays paused by default (see ADR-0006).

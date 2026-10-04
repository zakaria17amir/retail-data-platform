# Iteration log — Phase 4 models

All metric values come from the MLflow tracking server (`http://127.0.0.1:5000`, experiments
`late_delivery` and `demand_forecast`), rounded to 4 dp. Deltas are differences between those logged
values. Numbers marked *(nb01)* / *(nb02)* were computed by `ds/notebooks/01_late_delivery_eda.ipynb`
/ `02_demand_eda.ipynb` from the registry champion; the CSVs are in `ds/reports/tables/`.
late_delivery has one training cycle (v1), so "iterations" means baseline → model within it;
demand_forecast has two (v1, then v2 after the dbt tail cut changed).

## late_delivery (binary, P(delivered after the estimated date) at approval)

Data: `data/gold/ml/late_delivery_training.parquet`, sha256 `9a59eb46…9689` (tag `data_sha256`),
seller features sha256 `c900104f…f943`, code `3f75f92`. Time split by approval timestamp:
train < 2018-03-01 (n 57,110), validation [2018-03-01, 2018-06-01) (n 20,679), test
[2018-06-01, 2018-09-01) (n 18,667). Primary metric PR-AUC; Brier gates promotion.

| # | run (MLflow run id) | val PR-AUC | val ROC-AUC | val Brier | test PR-AUC | test ROC-AUC | test Brier |
|---|---|---|---|---|---|---|---|
| 0 | `constant_prior` (6ba58f74…) | 0.1194 | 0.5000 | 0.1071 | 0.0546 | 0.5000 | 0.0521 |
| 1 | `logistic` (9b317459…) | 0.2661 | 0.7214 | 0.0984 | 0.1041 | 0.7059 | 0.0533 |
| 2 | `lightgbm` (0d2ff3e3…) → **v1 champion** | 0.2499 | 0.7079 | 0.1000 | 0.1340 | 0.7633 | 0.0497 |

Deltas:

| step | val PR-AUC | val ROC-AUC | val Brier | test PR-AUC | test ROC-AUC | test Brier |
|---|---|---|---|---|---|---|
| prior → logistic | +0.1467 | +0.2214 | −0.0087 | +0.0495 | +0.2059 | **+0.0012** (worse) |
| logistic → LightGBM | **−0.0162** | **−0.0135** | **+0.0016** (worse) | +0.0299 | +0.0574 | −0.0036 |
| prior → LightGBM | +0.1305 | +0.2079 | −0.0071 | +0.0794 | +0.2133 | −0.0024 |

`recall_at_p50` (recall at precision ≥ 0.5) is ≈ 0 for every run (max 0.0059, LightGBM test): no
model reaches 50 % precision at any useful recall, so the operational number is recall at a fixed
**alert budget** (flag the top 10 % of scores) *(nb01, `late_delivery_alert_budget.csv`)*:

| model | window | flagged | recall | precision | lift vs base rate |
|---|---|---|---|---|---|
| constant prior | any | 10 % | 0.10 expected (random tie-break) | = base rate | 1.0 |
| logistic | val, own top 10 % | 10.0 % | 0.2862 | 0.3419 | 2.86 |
| LightGBM | val, own top 10 % | 10.0 % | 0.2543 | 0.3037 | 2.54 |
| logistic | test, own top 10 % | 10.0 % | 0.2000 | 0.1093 | 2.00 |
| **LightGBM** | **test, own top 10 %** | **10.0 %** | **0.2667** | **0.1457** | **2.67** |
| logistic | test, val-derived cutoff | 9.1 % | 0.1824 | 0.1099 | 2.01 |
| LightGBM | test, val-derived cutoff | 4.3 % | 0.1304 | 0.1663 | 3.04 |

Decisions:

1. **Baselines first.** The constant prior sets the floor (ROC 0.5, PR-AUC = late rate); logistic
   regression (median-imputed, scaled numerics, one-hot categoricals) is the "simple model" bar.
2. **LightGBM registered and promoted as v1.** Promotion rule: model tests (no NaN, range [0, 1],
   test Brier ≤ logistic test Brier) and no previous champion. It passed (0.0497 ≤ 0.0533).
   Early stopping on validation log loss stopped at `best_iteration` 190 of 2,000.
3. **Validation and test disagree.** Logistic is better on validation on every metric; LightGBM is
   better on test on every metric. Validation (Mar–May 2018, late rate 11.9 %) contains the
   Feb/Mar 2018 lateness spike; test (Jun–Aug 2018, 5.5 %) is a low-lateness period. With one
   validation window the ranking is not stable, so the v1 choice is weakly supported. The test
   window also took part in the promotion decision (Brier gate), so it is not an untouched holdout.
4. **Report recall at a 10 % alert budget, not recall@P50.** Precision 0.5 is unreachable at these
   base rates; the budget framing matches how an ops team would consume the score.
5. **Not done (candidates for v2):** rolling-origin evaluation over several windows (the same
   approach as the demand backtest), an embargo between splits (see the model card), class-rate
   recalibration on a recent window, and dropping/regularising `approve_month` (2nd SHAP feature,
   learnt from about one year per calendar month).

## demand_forecast (daily orders per product_category × customer_state, 28-day horizon)

### Cycle 1 — tail cut at 0.2 × trailing mean (v1)

Data: `data/gold/ml/demand_daily.parquet`, sha256 `f3c685717dff…66a7a4`, code `dddbd7d`. 62 modelled series
of 1,382 (1,320 excluded as sparse); series end at E = 2018-08-27. Test window 2018-07-31 →
2018-08-27; rolling-origin backtest folds start 2018-05-08, 2018-06-05, 2018-07-03 (28 days each).
Metric WAPE (lower is better).

| # | run (MLflow run id) | fold 1 | fold 2 | fold 3 | backtest mean | test | test top-10 series |
|---|---|---|---|---|---|---|---|
| 0 | `seasonal_naive` (1ca2d5df…) | 0.8594 | 0.6826 | 0.7618 | 0.7679 | 0.6604 | 0.4510 |
| 1 | `lightgbm` (ed2bf7dc…) → **v1 champion** | 0.6084 | 0.5566 | 0.6301 | 0.5984 | 0.5704 | 0.4295 |
| | delta | −0.2510 | −0.1260 | −0.1317 | **−0.1695 (−22.1 %)** | **−0.0900 (−13.6 %)** | −0.0215 (−4.8 %) |

Decisions:

1. **Seasonal naive** (last observed week repeated, zero before a series' first order) is the
   baseline; one global Poisson LightGBM with lags ≥ 28 days only (direct 28-day forecast, no
   recursion) is the model.
2. **Series selection** (dbt `ml_demand_daily.is_modelled`): ≥ 1 order on ≥ 50 % of the 180 days
   before the test cutoff. 62 series hold 59.96 % of all series-orders (61.68 % inside the
   180-day window) *(nb02)*; the long tail is not forecast.
3. **Promoted as v1**: no NaN/negative forecasts, finite WAPE, no previous champion. LightGBM beats
   naive on every fold, on test and on the top-10 set.
4. **Gain is concentrated where volume is low** *(nb02, `demand_test_by_volume_quartile.csv`)*:
   test WAPE naive → LightGBM, lowest-volume quartile 1.0079 → 0.8075, highest 0.5094 → 0.4515. On the
   10 largest series the gain is only −0.0215.
5. **Not done (candidates for v2):** level/bias correction (LightGBM under-forecasts the test window,
   sum forecast / sum actual = 0.7467 vs naive 0.9190 *(nb02)*), a stricter truncated-tail rule
   (see error analysis), prediction intervals.

### Cycle 2 — tail cut at 0.5 × trailing mean (v2)

Data change only (same code/config): dbt `ml_demand_min_ratio` 0.2 → 0.5, because the cycle-1 test
window ended in the extract's ramp-down (2018-08-24…27 at 70/55/56/56 orders/day). Data
`demand_daily.parquet` sha256 `bc78232b2606…26a51d0`, code `b64c4f8`. E = 2018-08-23, test window
2018-07-27 → 2018-08-23, folds start 2018-05-04, 2018-06-01, 2018-06-29. 61 modelled series of
1,382 (baby × RJ and musical_instruments × SP out, toys × RJ in).

| # | run (MLflow run id) | fold 1 | fold 2 | fold 3 | backtest mean | test | test top-10 series |
|---|---|---|---|---|---|---|---|
| 2 | `seasonal_naive` (18c39efc…) | 0.7766 | 0.6756 | 0.8044 | 0.7522 | 0.6349 | 0.4004 |
| 3 | `lightgbm` (b296acbb…) → **v2, challenger** | 0.5946 | 0.5533 | 0.6278 | 0.5919 | 0.5438 | 0.4317 |
| | delta | −0.1820 | −0.1223 | −0.1766 | **−0.1603 (−21.3 %)** | **−0.0911 (−14.4 %)** | **+0.0313 (+7.8 %)** (worse) |
| | v1 champion re-scored on this test window | – | – | – | – | 0.5352 | 0.4184 |

Decisions:

1. **v2 not promoted** (`wape 0.5438 >= champion v1 0.5352`). The gate re-scores the champion on
   the challenger's window, so it compares like with like; but v1 was fit on data before
   2018-07-31, so 07-27…07-30 are in-sample for it. On 07-31 → 08-23 (out of sample for both) v1
   still wins: 0.5365 vs 0.5414 (naive 0.6250). The extra four recent days of history v1 trained on
   are a legitimate edge, so v1 stays champion; `forecast demand` wrote 3,416 rows (61 series × 56
   days) with `model_version` 1.
2. **The tail cut removed most of the week-4 distortion**: horizon-week WAPE naive 0.6233 / 0.5993 /
   0.6274 / 0.7055, v2 0.5473 / 0.5361 / 0.5277 / 0.5691 (cycle 1 week 4: 1.1068 / 0.7829).
3. **LightGBM's edge is now in the long tail only.** On the top-10 series naive is better (0.4004 vs
   v1 0.4184 / v2 0.4317); both LightGBM versions lose to naive on 11 of 61 series holding 39.75 %
   of test orders (cycle 1: 13 of 62, 19.45 %), including 6 of the top 10. Level/bias correction
   (bias v1 0.7041, v2 0.6931, naive 0.9122) is the next candidate.

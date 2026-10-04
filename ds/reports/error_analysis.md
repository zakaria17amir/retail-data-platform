# Error analysis — Phase 4 champions (v1)

Source: `ds/notebooks/01_late_delivery_eda.ipynb` and `02_demand_eda.ipynb`, executed on the dev data
(`data/gold/ml/`) against the registry champions on the MLflow server. Each notebook first
recomputes the champion's headline metrics and checks them against MLflow: late delivery agrees
to < 1e-4 (max |Δ| 6.7e-5 on PR-AUC: the notebook joins seller features with a `merge_asof` that
mirrors Feast's point-in-time semantics instead of calling the Feast file store); demand matches
exactly (test WAPE 0.5704 / 0.6604 and the 10 per-series `test_wape.*` metrics). Raw tables:
`ds/reports/tables/`.

## 1. late_delivery v1 (LightGBM) — test window 2018-06-01 → 2018-09-01

n = 18,667, 1,020 late (5.46 %). Alert budget = top 10 % of test scores (threshold from the test
window itself); overall at that budget: recall 0.2667, precision 0.1457, lift 2.67. "flagged" is the
share of the segment's orders above that one global threshold, so it shows where alerts land.
AUCs need ≥ 10 positives. Late rate / mean prediction / flagged are percentages.

### By customer_state (top 10 by test volume)

| customer_state | n | late_rate | mean_pred | pr_auc | roc_auc | brier | flagged | recall@budget | precision@budget |
|---|---|---|---|---|---|---|---|---|---|
| SP | 8636 | 7.7 % | 6.1 % | 0.1851 | 0.7775 | 0.0673 | 9.7 % | 0.2575 | 0.2036 |
| RJ | 2131 | 4.5 % | 5.6 % | 0.1042 | 0.7466 | 0.0420 | 5.9 % | 0.1667 | 0.1280 |
| MG | 2067 | 1.6 % | 3.4 % | 0.0570 | 0.7186 | 0.0163 | 1.2 % | 0.0588 | 0.0800 |
| PR | 951 | 2.5 % | 3.7 % | 0.0930 | 0.8078 | 0.0241 | 2.5 % | 0.0833 | 0.0833 |
| RS | 910 | 2.6 % | 5.1 % | 0.0676 | 0.6654 | 0.0263 | 4.8 % | 0.1250 | 0.0682 |
| BA | 605 | 5.5 % | 11.8 % | 0.1419 | 0.6060 | 0.0582 | 38.2 % | 0.5455 | 0.0779 |
| SC | 602 | 2.7 % | 6.3 % | 0.1092 | 0.6921 | 0.0269 | 5.6 % | 0.0625 | 0.0294 |
| DF | 457 | 3.3 % | 6.1 % | 0.1675 | 0.7100 | 0.0315 | 6.8 % | 0.2667 | 0.1290 |
| ES | 350 | 5.4 % | 7.2 % | 0.2248 | 0.6721 | 0.0495 | 16.6 % | 0.3684 | 0.1207 |
| GO | 337 | 3.6 % | 5.6 % | 0.3479 | 0.7579 | 0.0326 | 4.7 % | 0.3333 | 0.2500 |

- **Geography of lateness flipped between windows.** In development data (train + validation)
  RJ was 15.4 % late and BA 16.0 %, SP 5.4 %; in test RJ is 4.5 %, BA 5.5 % and SP 7.7 %. The model
  learnt the old ranking: it over-predicts BA (11.8 % vs 5.5 % observed) and flags 38.2 % of BA
  orders at 7.8 % precision, while SP — where most test late orders are — gets precision 0.2036.
  BA has the lowest ROC-AUC (0.6060) of the top 10.
- MG is the lowest-risk state (1.6 %, predicted 3.4 %) and almost none of its late orders are
  caught (recall 0.0588); SC is similar (recall 0.0625).

### By product_category (top 10 by test volume)

| product_category | n | late_rate | mean_pred | pr_auc | roc_auc | brier | flagged | recall@budget | precision@budget |
|---|---|---|---|---|---|---|---|---|---|
| health_beauty | 2245 | 7.2 % | 7.5 % | 0.1869 | 0.7604 | 0.0632 | 17.7 % | 0.4259 | 0.1734 |
| bed_bath_table | 1706 | 3.9 % | 5.3 % | 0.1103 | 0.7760 | 0.0367 | 6.9 % | 0.2388 | 0.1356 |
| housewares | 1454 | 5.8 % | 5.0 % | 0.1420 | 0.7699 | 0.0532 | 5.6 % | 0.1294 | 0.1341 |
| watches_gifts | 1340 | 4.3 % | 6.4 % | 0.1830 | 0.7785 | 0.0391 | 10.5 % | 0.3621 | 0.1489 |
| sports_leisure | 1213 | 4.5 % | 6.0 % | 0.0989 | 0.7371 | 0.0431 | 8.9 % | 0.2364 | 0.1204 |
| computers_accessories | 1113 | 4.7 % | 4.8 % | 0.1443 | 0.7806 | 0.0424 | 4.9 % | 0.1346 | 0.1296 |
| furniture_decor | 966 | 4.6 % | 6.2 % | 0.1410 | 0.7777 | 0.0420 | 9.6 % | 0.2955 | 0.1398 |
| auto | 933 | 6.0 % | 7.2 % | 0.1118 | 0.7147 | 0.0560 | 14.9 % | 0.2321 | 0.0935 |
| telephony | 644 | 5.9 % | 6.5 % | 0.1076 | 0.7076 | 0.0553 | 10.1 % | 0.2105 | 0.1231 |
| baby | 607 | 6.1 % | 6.9 % | 0.1410 | 0.7335 | 0.0553 | 12.7 % | 0.2703 | 0.1299 |

- Ranking quality is fairly even across categories (ROC-AUC 0.7076–0.7806). The weak spots are
  housewares (under-predicted, 5.0 % vs 5.8 %, recall 0.1294) and auto (precision 0.0935).

### By seller→customer distance (km)

| distance_km | n | late_rate | mean_pred | pr_auc | roc_auc | brier | flagged | recall@budget | precision@budget |
|---|---|---|---|---|---|---|---|---|---|
| [0, 100) | 4160 | 12.0 % | 7.4 % | 0.2083 | 0.7037 | 0.1033 | 13.7 % | 0.2550 | 0.2228 |
| [100, 300) | 2622 | 5.0 % | 5.6 % | 0.1244 | 0.7763 | 0.0461 | 7.7 % | 0.1894 | 0.1232 |
| [300, 600) | 5798 | 2.6 % | 4.4 % | 0.1030 | 0.7653 | 0.0245 | 3.5 % | 0.1812 | 0.1343 |
| [600, 1000) | 3184 | 3.2 % | 5.2 % | 0.0937 | 0.7357 | 0.0309 | 6.7 % | 0.1845 | 0.0892 |
| [1000, 2000) | 1796 | 4.1 % | 8.6 % | 0.0989 | 0.7187 | 0.0423 | 21.5 % | 0.5068 | 0.0959 |
| [2000, ∞) | 1028 | 5.5 % | 10.0 % | 0.1682 | 0.7480 | 0.0525 | 27.4 % | 0.5789 | 0.1170 |

- **Biggest single error source.** In development data lateness rises monotonically with
  distance (4.7 % under 100 km → 15.5 % over 2,000 km). In test, short-haul orders (< 100 km) are
  the latest (12.0 %) and are under-predicted (7.4 %), while long-haul orders are over-predicted
  (10.0 % vs 5.5 %) and soak up alerts (27.4 % flagged, precision 0.1170).
  The < 100 km bucket holds 498 of the 1,020 test late orders (4,160 × 12.0 %).

### By freight ratio (freight / price)

| freight_ratio | n | late_rate | mean_pred | pr_auc | roc_auc | brier | flagged | recall@budget | precision@budget |
|---|---|---|---|---|---|---|---|---|---|
| [0, 0.1) | 2581 | 7.9 % | 7.2 % | 0.1866 | 0.7432 | 0.0689 | 14.8 % | 0.3448 | 0.1832 |
| [0.1, 0.2) | 5126 | 6.1 % | 6.0 % | 0.1534 | 0.7582 | 0.0551 | 9.5 % | 0.2603 | 0.1691 |
| [0.2, 0.3) | 3801 | 5.0 % | 5.9 % | 0.1198 | 0.7572 | 0.0458 | 9.2 % | 0.2275 | 0.1225 |
| [0.3, 0.5) | 4023 | 5.0 % | 5.8 % | 0.1257 | 0.7779 | 0.0458 | 9.0 % | 0.2300 | 0.1271 |
| [0.5, 1.0) | 2500 | 3.7 % | 5.7 % | 0.0944 | 0.7762 | 0.0356 | 8.4 % | 0.2796 | 0.1232 |
| [1.0, ∞) | 636 | 3.1 % | 6.4 % | 0.1060 | 0.7093 | 0.0315 | 11.9 % | 0.2500 | 0.0658 |

- The freight effect also reversed: development late rate rises with freight ratio (8.4 % →
  11.4 %), test falls (7.9 % → 3.1 %). Low-ratio orders are calibrated; high-ratio orders are
  over-predicted.

### Calibration (test deciles of the champion score, `late_delivery_reliability_test.csv`)

Mean prediction 0.0609 vs observed 0.0546 overall. The bottom five deciles are over-predicted
(e.g. decile 1: 0.0152 vs 0.0070), deciles 7–9 under-predicted (decile 8: 0.0771 vs 0.1024) and the top
decile over-predicted (0.1798 vs 0.1457). On validation the model under-predicts the level (0.0866 vs
0.1194). Scores rank; they are not reliable probabilities across windows.

### SHAP (TreeSHAP, 3,000 test orders, log-odds) — `figures/late_delivery_shap_summary.png`

| rank | feature | mean abs SHAP | direction (Spearman value vs SHAP, numerics) |
|---|---|---|---|
| 1 | estimated_days | 0.5952 | −0.98: a longer promised window → lower risk |
| 2 | approve_month | 0.4137 | 0.02: non-monotonic, i.e. calendar-month effects (Feb/Mar and Nov were late months in training) |
| 3 | customer_state | 0.3485 | categorical: BA +0.792, PE +0.749, CE +0.497 / MG −0.418, PR −0.332, SP −0.260 |
| 4 | seller_avg_delivery_days_90d | 0.2643 | +0.95: slower sellers → higher risk |
| 5 | distance_km | 0.1237 | +0.91: further → higher risk |
| 6 | product_category | 0.1053 | categorical: construction_tools_construction +0.363, baby +0.305 / garden_tools −0.219, pet_shop −0.184 |
| 7 | seller_state | 0.0726 | categorical: SP +0.050 / RS −0.255 |
| 8 | seller_late_rate_90d | 0.0591 | +0.32 |
| 9 | seller_orders_90d | 0.0445 | −0.38: busier sellers → lower risk |
| 10 | same_state | 0.0402 | −0.85: same state → lower risk |

Categorical values are the mean SHAP per level (levels with ≥ 30 rows in the sample). The
sum of SHAP values plus the base value reproduces the model output (checked in the notebook). The
top drivers (promised window, month, region, distance) are exactly the ones whose relationship to
lateness changed between windows, which is why test calibration is off.

## 2. demand_forecast v1 (LightGBM) — test window 2018-07-31 → 2018-08-27

62 modelled series, 4,335 test series-orders. Test WAPE LightGBM 0.5704 vs seasonal naive 0.6604
(MLflow). Bias (sum forecast / sum actual): LightGBM 0.7467, naive 0.9190, so LightGBM under-forecasts
the level. Per-series table for all 62 series: `tables/demand_test_per_series.csv`.

### Per-series WAPE, top 10 series by pre-test volume

| series | history orders | test orders | WAPE naive | WAPE LightGBM | Δ (LGBM − naive) | level shift |
|---|---|---|---|---|---|---|
| bed_bath_table × SP | 4067 | 333 | 0.4174 | 0.4085 | −0.0089 | 1.43 |
| health_beauty × SP | 3380 | 381 | 0.4383 | 0.3282 | −0.1101 | 1.32 |
| sports_leisure × SP | 3032 | 232 | 0.4138 | 0.3849 | −0.0289 | 1.46 |
| furniture_decor × SP | 2549 | 161 | 0.4286 | 0.4769 | **+0.0483** | 1.34 |
| computers_accessories × SP | 2473 | 185 | 0.6000 | 0.4446 | −0.1554 | 1.37 |
| housewares × SP | 2447 | 310 | 0.3484 | 0.5206 | **+0.1723** | 1.64 |
| watches_gifts × SP | 1939 | 182 | 0.4396 | 0.4148 | −0.0248 | 1.17 |
| toys × SP | 1513 | 79 | 0.4684 | 0.4552 | −0.0131 | 1.34 |
| auto × SP | 1435 | 168 | 0.5000 | 0.4654 | −0.0346 | 1.50 |
| telephony × SP | 1414 | 100 | 0.7000 | 0.5488 | −0.1512 | 1.25 |

Level shift = test-window daily mean / mean of the 28 days before the cutoff. All ten are SP, and
for nine of them the test level is ≥ 1.25× the previous four weeks.

### Where LightGBM loses (13 of 62 series, 19.45 % of test orders)

| series | history orders | test orders | zero days in test | level shift | WAPE naive | WAPE LightGBM | Δ |
|---|---|---|---|---|---|---|---|
| housewares × SP | 2447 | 310 | 0.0 % | 1.64 | 0.3484 | 0.5206 | +0.1723 |
| furniture_decor × SP | 2549 | 161 | 3.6 % | 1.34 | 0.4286 | 0.4769 | +0.0483 |
| housewares × RJ | 659 | 80 | 7.1 % | 1.82 | 0.6000 | 0.6202 | +0.0202 |
| fashion_bags_accessories × SP | 716 | 60 | 17.9 % | 1.94 | 0.5333 | 0.6744 | +0.1411 |
| auto × MG | 420 | 47 | 25.0 % | 1.47 | 0.7021 | 0.7239 | +0.0218 |
| sports_leisure × MG | 824 | 40 | 28.6 % | 1.03 | 0.8000 | 0.8006 | +0.0006 |
| health_beauty × PR | 344 | 37 | 25.0 % | 2.64 | 0.6757 | 0.7372 | +0.0615 |
| bed_bath_table × RS | 517 | 23 | 50.0 % | 0.92 | 1.0000 | 1.0318 | +0.0318 |
| sports_leisure × PR | 403 | 23 | 39.3 % | 0.92 | 0.7391 | 0.7900 | +0.0509 |
| garden_tools × MG | 470 | 17 | 50.0 % | 0.89 | 0.7647 | 0.9765 | +0.2118 |
| office_furniture × SP | 494 | 15 | 67.9 % | 0.54 | 1.5333 | 1.5691 | +0.0358 |
| cool_stuff × RJ | 485 | 15 | 64.3 % | 0.83 | 1.2667 | 1.4314 | +0.1648 |
| computers_accessories × RS | 375 | 15 | 60.7 % | 0.68 | 1.0000 | 1.2504 | +0.2504 |

Two patterns:

1. **Level jumps in mid/high-volume series** (housewares × SP/RJ, fashion_bags × SP,
   health_beauty × PR: level shift 1.6–2.6×). The naive forecast repeats the most recent week and
   picks up the jump; LightGBM only sees lags ≥ 28 days plus 7/28-day means lagged 28 days, so
   it reverts towards the older level. This is the same mechanism as the overall under-forecast
   (bias 0.7467).
2. **Very sparse series** (≥ 50 % zero days in the test window, ≤ 23 test orders): both models
   have WAPE ≈ 1 or above; LightGBM's smooth positive forecasts lose to naive's zeros. These series
   are near the 50 %-active selection threshold.

Losers by state: SP 4 of 24, RJ 2 of 13, MG 3 of 11, PR 2 of 7, RS 2 of 6, BA 0 of 1.

### Error by volume quartile and horizon week

| volume quartile (pre-test orders) | series | test orders | WAPE naive | WAPE LightGBM |
|---|---|---|---|---|
| Q1 low | 17 | 506 | 1.0079 | 0.8075 |
| Q2 | 14 | 328 | 1.0366 | 0.9684 |
| Q3 | 15 | 904 | 0.7633 | 0.6349 |
| Q4 high | 16 | 2597 | 0.5094 | 0.4515 |

| horizon week | WAPE naive | WAPE LightGBM |
|---|---|---|
| 1 | 0.5385 | 0.5215 |
| 2 | 0.6007 | 0.5329 |
| 3 | 0.6176 | 0.5486 |
| 4 | 1.1068 | 0.7829 |

**Week 4 is distorted by the extract tail.** Daily actuals (all 62 series,
`tables/demand_test_daily_totals.csv`) fall from 170 on 2018-08-21 to 70, 55, 56, 56 on
2018-08-24…27, while the contract's approved-order counts per purchase day (late-delivery contract)
are 107, 67, 68, 70 on those days, then 45 and 16. The dbt truncated-tail rule (keep days with orders
≥ 0.2 × trailing 28-day mean) keeps these ramp-down days inside the test window. Both models'
week-4 WAPE, and part of the test WAPE, reflect extract truncation rather than demand. Raising
`ml_demand_min_ratio` (analytics-owned dbt var) would cut the test window before the ramp-down; this
is reported to the lead rather than changed here.

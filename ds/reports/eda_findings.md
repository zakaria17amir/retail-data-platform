# EDA findings

Conclusions from `ds/notebooks/01_late_delivery_eda.ipynb` (sections 1–4) and
`ds/notebooks/02_demand_eda.ipynb` (sections 1–3), run on the dev data in `data/gold/ml/`.
Late-delivery EDA uses the development windows only (train + validation, approval < 2018-06-01,
n 77,789, late rate 8.75 %); the test window is kept for the error analysis.

## Late delivery

- **Label balance and shift.** Late rate 7.59 % train, 11.94 % validation, 5.46 % test. Monthly late
  rate ranges from 1.38 % (2018-06) to 21.65 % (2018-03); 2017-11 is 14.11 % and 2018-02 15.32 %. Labelled
  share (orders with a delivery timestamp) is 93–99 % per month from 2017-01 and 96–99 % from 2017-07 (83 % in 2016-10), so censoring at the end of the extract
  is small; the shift is a real change in lateness.
- **Season.** By calendar month (dev): Feb 12.86 %, Mar 17.40 %, Nov 14.11 % vs 3.22–3.86 % for
  Jun–Aug. Approval weekday matters little (8.18–9.46 %).
- **State.** SP holds 40.95 % of dev orders at 5.41 % late; RJ 15.35 %, BA 15.99 %, ES 13.68 %, SC
  11.21 %; MG 6.50 %, PR 5.59 %.
- **Category.** Top-10 categories sit between 6.81 % (cool_stuff) and 9.93 % (bed_bath_table): a weak
  signal on its own.
- **Distance.** Monotonic: 4.70 % under 100 km, 8.93 % at 300–600 km, 15.47 % over 2,000 km; same-state
  orders 5.31 % vs 10.59 % cross-state.
- **Freight ratio.** Mild: 8.25–8.44 % below 0.2, 11.36 % at ≥ 1.0.
- **Seller concentration.** 2,433 sellers; the top 1 % (24) handle 25.14 % of orders and 26.29 % of
  late orders, the top 10 % (243) 66.98 % of orders and 66.02 % of late orders, so lateness is not
  concentrated beyond volume. The seller's trailing-90-day late rate at approval does carry signal:
  7.18 % late when it was 0, 9.50 % at 5–10 %, 11.69 % at 10–20 %, 12.95 % at ≥ 20 %; sellers with no
  history 7.83 %.

These relationships did not all hold in the test window (distance, freight and state effects
reversed); see `error_analysis.md`.

## Demand

- **Coverage.** 1,382 category × state series from 2016-09-04 to 2018-08-27; 62 modelled
  (≥ 1 order on ≥ 50 % of the 180 days before the test cutoff), holding 59.96 % of series-orders
  (61.68 % inside the 180-day window).
- **Sparsity.** In the 180-day window the modelled series have a median of 1.44 orders/day and are
  active on a median 71.9 % of days; excluded series have a median of 0.017 orders/day and 1.7 % active
  days. That gap is why the long tail is not forecast.
- **Seasonality.** Weekly: Monday 16.24 % of orders down to Saturday 10.94 % (Sunday 12.07 %), since
  2017-06. Yearly: Nov 2017 is the peak month (7,483 series-orders, Black Friday), 2018 months are
  6,208–7,234. Only one November is in the data, so yearly seasonality is not learnable; the model
  has only day-of-week, month and day-of-month as calendar features.
- **Truncated tail.** The extract runs out after late August 2018: approved orders per purchase day
  drop from 204 (2018-08-22) to 67–70 (08-25…27), 45, 16, and one order in September. dbt ends the
  demand series at E = 2018-08-27, but the ramp-down days before E are kept (see error analysis).

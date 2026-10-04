# Power BI — retail semantic model and report

> **Manual step for the human.** No agent can drive Power BI Desktop. The semantic model (TMDL) is
> committed as text; the report pages, the `.pbix` and the screenshots must be built by hand:
>
> 1. Build gold first (`make gold` from the repo root) so `data/gold/*.parquet` exists.
> 2. Open `bi/retail.pbip` in Power BI Desktop (Windows). If Desktop refuses the project, enable
>    *File → Options → Preview features*: "Power BI Project (.pbip) save option", "Store semantic
>    model using TMDL format" and "Store reports using enhanced metadata format (PBIR)", then restart.
> 3. *Home → Transform data → Edit parameters*: set `GoldFolder` to the absolute path of your
>    `data/gold` folder (default `E:\Data Engineer and Analytics\retail-data-platform\data\gold`).
> 4. *Home → Refresh*.
> 5. Build the pages below (one blank `Executive` page is pre-created), *File → Save* (stays PBIP),
>    and optionally *File → Save as* `bi/retail.pbix`.
> 6. Save one PNG per page to `bi/screenshots/<page>.png` (e.g. `executive.png`).

## Layout

```
bi/
├── retail.pbip                         # opens the report + model
├── retail.Report/                      # PBIR report, pages added by hand
│   ├── definition.pbir                 # byPath → ../retail.SemanticModel
│   └── definition/{version.json, report.json, pages/}
└── retail.SemanticModel/
    ├── definition.pbism
    └── definition/
        ├── database.tmdl, model.tmdl
        ├── expressions.tmdl            # GoldFolder parameter
        ├── relationships.tmdl
        └── tables/<model>.tmdl         # one per gold Parquet file
```

Every table is an import partition
`Parquet.Document(File.Contents(GoldFolder & "\<model>.parquet"))`. `fct_sessions`, `fct_events`,
`rpt_funnel`, `fct_recommendations` and `rpt_recommendation_ctr` additionally drop rows with a null
key (dbt-duckdb writes one all-null row for an empty external model).

## Measures (one metric, one definition)

Each measure mirrors a metric in `analytics/models/metrics/` and names it in a `// dbt metric:` comment.
No other business logic lives in Power BI.

| Measure | Table | dbt metric | DAX |
|---|---|---|---|
| Revenue | fct_order_items | `revenue` | `SUM(item_revenue)` where `is_revenue_order` |
| Orders | fct_orders | `orders` | `DISTINCTCOUNT(order_id)` where `is_revenue_order` |
| AOV | fct_orders | `aov` | `DIVIDE([Revenue], [Orders])` |
| Delivered Orders | fct_orders | `delivered_orders` | rows where `is_delivered` |
| Late Delivered Orders | fct_orders | `late_delivered_orders` | rows where `is_late` |
| Late Delivery Rate | fct_orders | `late_delivery_rate` | `DIVIDE([Late Delivered Orders], [Delivered Orders])` |
| Sessions | fct_sessions | `sessions` | `COUNTROWS(fct_sessions)` |
| Checkout Sessions | fct_sessions | `checkout_sessions` | rows where `has_checkout` |
| Conversion Rate | fct_sessions | `conversion_rate` | `DIVIDE([Checkout Sessions], [Sessions])` |
| Recommendations Shown | fct_recommendations | `recommendations_shown` | `COUNTROWS(fct_recommendations)` |
| Recommendations Clicked | fct_recommendations | `recommendations_clicked` | rows where `is_clicked` |
| CTR | fct_recommendations | `recommendation_ctr` | `DIVIDE([Recommendations Clicked], [Recommendations Shown])` |

## Relationships

All many-to-one, single direction.

- `fct_orders` → `dim_customer` (`customer_sk`, point-in-time SCD2 key), `dim_date`
  (`order_purchase_date`); `dim_customer` → `dim_geo` (`customer_zip_code_prefix`).
- `fct_order_items`, `fct_payments`, `fct_reviews` → `fct_orders` (`order_id`), so date and customer
  filters reach them through `fct_orders`.
- `fct_order_items` → `dim_product` (`product_sk`), `dim_seller` (`seller_sk`);
  `fct_payments` → `dim_payment_type`.
- `fct_sessions`, `fct_events`, `fct_recommendations`, `rpt_daily_sales`, `rpt_funnel`,
  `rpt_recommendation_ctr` → `dim_date` on their day column;
  `rpt_delivery_sla`, `rpt_product_performance` → `dim_date` on `order_month` (first of month).
- `rpt_customer_360` and `rpt_data_quality` stand alone.

Product/seller filters reach `fct_order_items` only, so on product/seller visuals use Revenue (and
`rpt_product_performance` columns), not Orders/AOV.

`fct_order_items`, `fct_payments` and `fct_reviews` keep CDC-deleted rows with `is_deleted`. Revenue
already excludes deleted lines (`is_revenue_order`); filter `is_deleted = FALSE` on any visual that
uses payment or review columns directly.

## Page spec

| Page | Visuals | Fields / measures |
|---|---|---|
| **Executive** | KPI cards; revenue trend; revenue by state; slicers | Revenue, Orders, AOV, Late Delivery Rate, Conversion Rate; line Revenue by `dim_date[month_start_date]`; filled map / bar Revenue by `dim_customer[customer_state]`; slicers `dim_date[calendar_year]`, `dim_customer[customer_state]` |
| **Funnel** | funnel; conversion trend; events by type | funnel `rpt_funnel` sum of `sessions`, `product_view_sessions`, `add_to_cart_sessions`, `checkout_sessions`; card + line Conversion Rate by `dim_date[date_day]`; bar `fct_events[event_count]` by `event_type` (empty until clickstream data lands) |
| **Customers (RFM, cohorts)** | RFM matrix; recency × monetary scatter; LTV distribution; new vs repeat | matrix `rpt_customer_360[recency_score]` × `[frequency_score]`, count of `customer_unique_id`, avg `monetary`; scatter `recency_days` × `monetary`; histogram/bar of `ltv`; cohorts: column Orders by `dim_date[month_start_date]` with legend `fct_orders[is_first_order]` |
| **Products/Sellers** | top categories; product trend; top sellers | bar Revenue by `dim_product[product_category_name_english]` (Top N); line `rpt_product_performance` sum of `units`, `revenue` by `order_month`, avg `avg_review_score`; table `dim_seller[seller_id]`, `seller_state`, Revenue; map Revenue by `dim_seller[seller_state]` |
| **Delivery SLA** | late-rate KPI and trend; by state; delivery time | card Late Delivery Rate, Delivered Orders; line Late Delivery Rate by `dim_date[month_start_date]`; bar Late Delivery Rate by `dim_customer[customer_state]`; average of `fct_orders[purchase_to_customer_hours]` by month |
| **Forecast vs Actual** | placeholder until Phase 4 | line Revenue and Orders by `dim_date[date_day]`; text box "forecast (category × state, 28-day horizon) arrives in Phase 4" |
| **Data Quality** | check results; rejects | table `rpt_data_quality[checked_at]`, `check_type`, `table_name`, `check_name`, `severity`, `success`, `rows_in`, `rows_rejected`, `pct_rejected`; card count of `check_name` filtered `success = false`; bar sum `rows_rejected` by `table_name`; slicers `run_id`, `check_type`, `severity` |

Use the measures for any rate on a page; do not average the pre-computed `late_rate`/`conversion_rate`
columns of the `rpt_` tables across rows.

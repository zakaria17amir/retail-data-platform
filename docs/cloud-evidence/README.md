# Cloud evidence

This folder holds the evidence that the cloud slice ran. It's empty until the first manual apply
([runbook](../runbooks/cloud.md), §9). Capture it early, because the Snowflake trial lasts 30 days.
Before committing:
- redact AWS account ids, ARNs, emails, the Snowflake org/account and IP addresses;
- never commit keys, `terraform.tfvars`, `backend.hcl` or state.

| File | What | How |
|---|---|---|
| `01-apply-aws.txt` | Tail of `terraform apply` for `envs/demo`, pass 1 and pass 2 | copy from the terminal |
| `02-apply-snowflake.txt` | Tail of `terraform apply` for `terraform/snowflake` | copy from the terminal |
| `03-outputs.txt` | `terraform output` for both roots, redacted | `terraform -chdir=… output` |
| `04-cloud-batch-run.png` | `cloud-batch.yml` run with the job summary (run ids) | GitHub Actions page |
| `05-emr-job-runs.png` | EMR Serverless job runs `silver` + `export`, `SUCCESS`, with durations | EMR Studio or console, or `aws emr-serverless list-job-runs` |
| `06-s3-export.txt` | `export/silver/<domain>/<table>/<run_id>/` listing for one run | `aws s3 ls --recursive` |
| `07-snowpipe-copy-history.png` | `COPY_HISTORY` and `SYSTEM$PIPE_STATUS` for the silver pipes | Snowsight worksheet |
| `08-silver-counts.csv` | Row counts per `SILVER` table and run id (`split_part(_EXPORT_FILE, '/', 3)`) | Snowsight, download results |
| `09-dbt-query-history.png` | Query history of the dbt build as `DBT_SERVICE` | Snowsight → Monitoring → Query History |
| `10-gold-tables.png` | `SHOW TABLES IN SCHEMA RETAIL.GOLD` and a mart sample | Snowsight worksheet |
| `11-powerbi-snowflake.png` | Power BI report page sourced from Snowflake as `REPORTER` | Power BI Desktop |
| `12-serving-health.txt` | `curl …/health` through the ALB (optional, only if serving was enabled) | terminal |
| `13-cost-explorer.png` | AWS Cost Explorer by service, filtered on tag `project = retail-data-platform` | Billing → Cost Explorer |
| `14-budget.png` | Budget `retail-data-platform-demo-monthly` status | Billing → Budgets |
| `15-snowflake-credits.png` | Warehouse metering, Snowpipe usage and the `RETAIL_MONITOR` status | Snowsight → Admin → Cost Management |
| `16-teardown.txt` | Tail of `make cloud-down` + the tagging-API check (only bootstrap left) | terminal |

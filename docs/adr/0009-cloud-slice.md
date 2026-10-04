# ADR-0009: Cloud slice — batch golden path on AWS + Snowflake, manual apply

## Status
Accepted

## Context
Spec §11 asks for a cheap, tear-down-able cloud slice plus a mapping document. It has to fit a $25
AWS budget and a 30-day Snowflake trial, with no cloud credentials in the dev environment and the
same Spark code as local.

## Decision
- **Slice = batch golden path.** S3 → EMR Serverless (silver) → Parquet export → Snowpipe →
  dbt-snowflake `GOLD` → Power BI, plus optional ECS Fargate serving. Streaming, Feast, MLflow and
  GenAI are mapped in [cloud-architecture.md](../cloud-architecture.md), not deployed. Bronze is
  copied once from local MinIO.
- **Export to Parquet for Snowpipe.** Snowpipe can't follow a Delta log, so `cloud/export.py` writes
  each table's current snapshot to `export/silver/<domain>/<table>/<run_id>/`. The files use
  `TIMESTAMP_MICROS` and are staged, then moved in under stable `part-NNNNN.parquet` names. A
  retried run id reuses the paths, and Snowpipe's load history skips them. Every run appends a full
  snapshot, so the dbt Snowflake sources keep only the latest run, parsed from `_EXPORT_FILE`. dbt
  puts everything in `GOLD` (`generate_schema_name`) and authenticates with a key pair.
- **EMR release `emr-spark-8.0.0`.** It's the only GA release on Spark 4.0.x: Spark 4.0.2,
  Delta 4.0.0, Python 3.11. Local runs Spark 4.0.4 / Delta 4.0.1, so the gap is patch-level only.
  `8.1.0` jumps to Spark 4.1.1 / Delta 4.2.0. GE needs Python ≥ 3.12 + pandas, so quality stays local.
- **Fargate scale-to-zero.** Serving sits behind `enable_serving` (default `false`) with
  `desired_count` 0. The ALB bills hourly even with no tasks, so by default it isn't created.
- **OIDC.** Exact `sub` matches on `ref:refs/heads/main` and `environment:cloud`, with no wildcards.
  The `cloud` environment must be restricted to `main` in GitHub settings. There are two roles: a
  Terraform-scoped deploy role, and a run role (EMR start/get, PassRole, artifacts put, ECR push).
  No long-lived keys.
- **Cost guard-rails as code.** Budget $25 (50/80/100 % email alerts). 7-day lifecycle on `landing/`
  and noncurrent versions. EMR auto-stop 5 min, max 16 vCPU / 64 GB. Snowflake XS with 60 s
  auto-suspend and a 5-credit resource monitor. The monitor covers warehouses only: Snowpipe's
  serverless credits aren't capped.
- **Manual apply.** CI runs fmt/validate/tflint, and `plan` only when credentials exist. A human
  runs `apply`. It takes two passes (AWS → Snowflake → AWS), because the storage integration and the
  AWS role reference each other. Snowflake Terraform runs as `ACCOUNTADMIN`, which integrations and
  resource monitors need. State is in S3 with `use_lockfile`; the DynamoDB lock table is kept
  because DynamoDB locking is deprecated but still supported.

## Consequences
- No GE gate in the cloud batch, and a patch-level engine gap. `landing` and `RAW` exist but are unused.
- An existing GitHub OIDC provider must be `terraform import`ed. Rejected: MWAA (~$358/month).

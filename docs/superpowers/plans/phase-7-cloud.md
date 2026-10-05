# Phase 7 — Cloud Path (AWS + Snowflake) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A deployable, cheap, tear-down-able cloud slice of the batch golden path, plus a full mapping
document:
- S3 lakehouse;
- EMR Serverless running the same silver Spark jobs;
- Snowpipe into Snowflake;
- dbt-snowflake gold;
- ECS Fargate serving;
- Budget/CloudWatch guard-rails;
- GitHub Actions with OIDC.

Everything is Terraform, validated in CI. `apply` is a manual, human step.

**Architecture:**
- **AWS bootstrap.** `terraform/aws/bootstrap/` holds the remote state bucket and the DynamoDB lock
  table.
- **AWS demo env.** `terraform/aws/envs/demo/` wires these modules:
  - `storage`: buckets `landing`, `lakehouse`, `artifacts`, with versioning, SSE-S3, lifecycle, public
    access blocked;
  - `iam`: GitHub OIDC provider + deploy/run roles, EMR job role, ECS task roles, least privilege;
  - `observability`: AWS Budget $25 with email alerts, log groups, alarms;
  - `emr-serverless`: application + job role; job scripts in `artifacts`;
  - `serving`: ECR, ECS Fargate service behind an ALB, Secrets Manager for MLflow/model config;
  - `snowpipe-integration`: S3 event → SNS → SQS, plus an IAM role trusted by Snowflake's storage
    integration.
- **Snowflake.** `terraform/snowflake/` holds:
  - database `RETAIL`, schemas `RAW`/`SILVER`/`GOLD`;
  - warehouse `XS` with 60 s auto-suspend;
  - roles `LOADER`/`TRANSFORMER`/`REPORTER`;
  - storage integration + external stage on the export prefix;
  - one pipe per silver table;
  - a dbt service user with key-pair auth.
- **Data flow in the cloud:**
  1. EMR Serverless runs silver (Delta on `s3://<lakehouse>/silver`).
  2. An export job writes plain Parquet snapshots to `s3://<lakehouse>/export/silver/<table>/<run_id>/`.
     Snowpipe can't follow the Delta log, so it ingests these.
  3. Snowpipe auto-ingests them into `SILVER.<table>`.
  4. dbt-snowflake builds `GOLD`.
  5. Power BI connects to Snowflake (documented).
- **Orchestration in the cloud.** A GitHub Actions `cloud-batch.yml`, with a schedule plus manual
  dispatch, assumes the OIDC run role, uploads job code, starts the EMR Serverless job runs and waits,
  then runs `dbt build --target snowflake`. It's "MWAA in production". It is disabled unless repo
  variables are set.

**Tech Stack:** Terraform ≥ 1.9 (AWS provider 6.x, Snowflake provider `snowflakedb/snowflake` 2.x;
pinned versions ≥ 7 days before 2026-09-27), tflint, EMR Serverless, ECS Fargate, Snowpipe,
dbt-snowflake 1.12.1 (the ephemeral pinned env from Phase 3), GitHub Actions OIDC.

**Spec:** `docs/superpowers/specs/retail-data-platform-design.md` §11 (+ §12 CI).

## Global Constraints

- **No credentials exist in this environment, and nothing is applied.**
  - Every module must pass `terraform fmt -check`, `terraform init -backend=false` + `terraform
    validate`, and `tflint` (pinned).
  - Snowflake/AWS `plan` runs only where credentials are configured. The CI job skips `plan` without
    them, and says so in the summary.
  - Never write credentials, account ids or emails into code. Everything comes from variables with no
    defaults for secrets, with an example `terraform.tfvars.example`.
- **Cost guard-rails are code:**
  - an AWS Budget of $25/month with 50/80/100 % email alerts;
  - S3 lifecycle (expire `landing/` after 7 days, noncurrent versions after 7 days);
  - an EMR Serverless application with auto-stop after 5 idle minutes and maximum capacity limits;
  - ECS `desired_count` default 0, so the service exists but costs nothing until scaled;
  - a Snowflake warehouse XS with 60 s auto-suspend and a resource monitor at a small credit quota;
  - `make cloud-down` destroys everything except the state bucket.
- **Least privilege:**
  - the GitHub OIDC trust is restricted to `repo:<owner>/<repo>:ref:refs/heads/main` (+ environment
    `cloud`);
  - the EMR job role reads `artifacts` and reads/writes `lakehouse`;
  - Snowflake's role reads `export/` only;
  - the ECS task role reads its secret and the `artifacts` model prefix.
- **Same code, cloud entrypoints.** The silver jobs run unchanged on EMR Serverless. If the chosen EMR
  release's Spark/Delta version differs from local (Spark 4.0.4 / Delta 4.0.1), pick the release that
  matches.
  - If none matches, document the gap.
  - Add a compatibility check: a CI step that imports the job modules against the EMR release's Python
    version.
  - Paths come from env/args (`LAKEHOUSE_URI`), not MinIO settings.
- Module conventions:
  - `variables.tf`/`outputs.tf`/`versions.tf`/`main.tf` per module;
  - descriptions on every variable and output;
  - tags `project=retail-data-platform`, `env`;
  - region variable default `eu-west-1`.
- Conventional commits; parallel tasks in separate worktrees with disjoint files. Per the user's
  directive there are no per-task reviews, only one whole-branch review at the end of the phase.

## Review Focus

1. The OIDC trust condition can't be assumed from forks or other branches.
2. No bucket is public, and no wildcard `s3:*` on `*`.
3. Destroy works: no `prevent_destroy` except the state bucket, and buckets use `force_destroy` behind a
   variable for the demo.
4. Snowflake grants: `REPORTER` read-only on `GOLD`, `TRANSFORMER` owns `GOLD` and reads `SILVER`,
   `LOADER` writes `SILVER` via pipes only.
5. The export job is idempotent per `run_id` and Snowpipe doesn't double-load (pipe file dedupe on the
   same path).

## File Structure

```
terraform/aws/bootstrap/*, terraform/aws/envs/demo/{main.tf, variables.tf, outputs.tf, providers.tf, terraform.tfvars.example}, terraform/aws/modules/{storage,iam,observability}/*   (T1)
terraform/aws/modules/{emr-serverless,serving,snowpipe-integration}/*, terraform/aws/envs/demo/compute.tf   (T2)
terraform/snowflake/*                                                                                       (T3)
lakehouse/spark/src/lakehouse_spark/cloud/{entrypoint.py, export.py}, lakehouse/spark/tests/test_cloud_*.py, analytics/ snowflake sources (exception)   (T4)
.github/workflows/{terraform.yml, cloud-batch.yml}, Makefile cloud-*, .tflint.hcl                         (T5)
docs/cloud-architecture.md, docs/adr/0009-cloud-slice.md, docs/runbooks/cloud.md, docs/cloud-evidence/README.md, README, spec §11   (T6)
```

Waves: **1** = T1 ∥ T2 ∥ T3 ∥ T4 (disjoint files; T2 references T1 module outputs by the documented
names below) → merge → **2** = T5 ∥ T6 → final review.

**Cross-module names (contract):**
- `module.storage` outputs: `lakehouse_bucket`, `landing_bucket`, `artifacts_bucket`,
  `lakehouse_bucket_arn`, `artifacts_bucket_arn`.
- `module.iam` outputs: `github_deploy_role_arn`, `github_run_role_arn`, `emr_job_role_arn`.
- `module.snowpipe_integration` takes the `snowflake_iam_user_arn` + `snowflake_external_id`
  variables (from the Snowflake storage integration; two-step apply documented) and outputs
  `snowflake_role_arn`, `sqs_queue_arn`.

---

### Task 1: AWS bootstrap, storage, IAM, observability

**Owner:** `platform-engineer`.
- **bootstrap:** state bucket (versioned, encrypted, `prevent_destroy`) + lock table.
- **envs/demo:** backend `s3` partial config (`backend.hcl.example`), providers, and wiring of the
  `storage`, `iam` and `observability` modules.
- **Modules** per Global Constraints and Review Focus 1–3. The `iam` module covers:
  - the GitHub OIDC provider (thumbprint-less, current AWS guidance);
  - the deploy role (Terraform-scoped);
  - the run role (EMR start-job-run, S3 artifacts put, ECR push);
  - the EMR job role.
- [ ] Verify fmt/validate/tflint locally. Commit `feat(terraform): aws bootstrap, storage, iam and
  budget guard-rails`.

### Task 2: AWS compute: EMR Serverless, serving, Snowpipe integration

**Owner:** `platform-engineer`.
- **emr-serverless:** an application on the release chosen in T4 (variable), auto-start/stop 5 min,
  max capacity (e.g. 16 vCPU / 64 GB), CloudWatch logging.
- **serving:**
  - ECR repo (scan on push, lifecycle keep 5);
  - ECS cluster + Fargate task (the serving image; env `MLFLOW_TRACKING_URI` or the model S3 path from
    a Secrets Manager secret);
  - ALB with health check `/health` and an SG restricted to a CIDR variable;
  - `desired_count` 0 default.
- **snowpipe-integration:** SNS topic + SQS (or Snowflake's managed SQS via `notification_channel`).
  Prefer **direct S3 → Snowflake SQS** notification as Snowflake documents. Add an IAM role trusting
  the Snowflake IAM user/external id.
- `envs/demo/compute.tf` wires them using the contract names.
- [ ] Verify fmt/validate/tflint. Commit `feat(terraform): emr serverless, ecs serving, snowpipe
  integration`.

### Task 3: Snowflake

**Owner:** `platform-engineer` (+ analytics knowledge).
- Provider `snowflakedb/snowflake`, pinned; auth vars (key-pair).
- Objects:
  - database, schemas, warehouse XS (60 s, initially suspended);
  - resource monitor;
  - roles + grants per Review Focus 4;
  - storage integration (outputs the IAM user ARN and external id for T2's role) + external stage
    (Parquet);
  - tables in `SILVER` for the 11 silver tables, columns from the silver contracts
    (`lakehouse/spark/src/lakehouse_spark/silver/tables.py`), `MATCH_BY_COLUMN_NAME`;
  - one `PIPE` per table with `AUTO_INGEST`;
  - dbt service user (RSA public key variable, default role `TRANSFORMER`, default warehouse).
- [ ] Verify fmt/validate/tflint. Commit `feat(terraform): snowflake database, roles, pipes and dbt user`.

### Task 4: Cloud job entrypoints and Snowflake dbt sources

**Owner:** `data-engineer` (+ an `analytics-engineer` exception for sources).
- **Release choice.** Determine the EMR Serverless release whose Spark/Delta versions match local
  (web-check AWS docs; prefer one with Spark 4.0.x + Delta 4.0.x). Record the choice and any gap; it's
  a variable in T2.
- **`lakehouse_spark/cloud/entrypoint.py`:**
  - runs `silver`/`quality`/`export` with `LAKEHOUSE_URI` (s3://) and no MinIO config;
  - the Spark conf for Delta;
  - packaged as a zip of `lakehouse_spark` + a `--py-files` layout.
- **`cloud/export.py`:** for each silver table (current state), write Parquet to
  `export/silver/<table>/<run_id>/` (overwrite that run_id; idempotent), plus a `_SUCCESS` marker.
- **Tests:** local Spark (`make test-spark`) for the export (row parity, idempotent re-run); the
  entrypoint arg parsing.
- **dbt:** sources for target `snowflake` resolve to `RETAIL.SILVER.<table>` while duckdb keeps
  `delta_scan`. `make dbt-parse` stays green for both targets.
- [ ] Commit `feat(lakehouse): emr serverless entrypoint and silver parquet export; feat(analytics):
  snowflake silver sources`.

### Task 5: CI and Make

**Owner:** `platform-engineer`.
- **`.github/workflows/terraform.yml`** (on PR paths `terraform/**`):
  - fmt-check, validate for every root (bootstrap, envs/demo, snowflake);
  - tflint (pinned action + `.tflint.hcl` with the aws ruleset pinned);
  - `plan` only if the `AWS_PLAN_ROLE_ARN` / `SNOWFLAKE_*` secrets exist (else a notice).
- **`.github/workflows/cloud-batch.yml`:**
  - `workflow_dispatch` + weekly schedule;
  - `if: vars.CLOUD_ENABLED == 'true'`;
  - OIDC assume of the run role, upload of job code to artifacts, EMR start-job-run silver → export
    (with waits), then dbt-snowflake build (key-pair secret);
  - a summary with run ids.
- **Make:** `cloud-bootstrap`, `cloud-plan`, `cloud-up`, `cloud-down` (with confirmation prompts),
  `cloud-validate` (fmt/validate/tflint for all roots, runnable locally).
- [ ] Commit `ci: terraform validate/lint and gated cloud batch workflow`.

### Task 6: Docs

**Owner:** `platform-engineer`.
- **`docs/cloud-architecture.md`:**
  - the deployed slice (mermaid diagram);
  - the full component mapping table (local → AWS managed) with reasons and rough monthly cost;
  - the "MWAA in production" story.
- **ADR-0009:** the slice choice, export-to-Parquet for Snowpipe, EMR release/version gap, Fargate
  scale-to-zero, OIDC, cost guard-rails, manual apply.
- **`docs/runbooks/cloud.md`:**
  - prerequisites (AWS account, Snowflake trial, AWS CLI, key pair);
  - bootstrap;
  - the two-step apply (Snowflake integration ↔ AWS role);
  - first batch run;
  - Power BI on Snowflake;
  - teardown;
  - the evidence capture checklist.
- `docs/cloud-evidence/README.md`: what to capture and where.
- README Phase 7 row + "Run it"; spec §11 amendments.
- [ ] Commit `docs: cloud architecture, ADR-0009, cloud runbook`.

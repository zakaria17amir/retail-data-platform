# Cloud runbook (Phase 7, AWS + Snowflake slice)

This runbook deploys the batch golden path: S3 → EMR Serverless → Parquet export → Snowpipe →
dbt-snowflake → Power BI, with optional ECS serving. Read [cloud-architecture.md](../cloud-architecture.md)
for the architecture and [ADR-0009](../adr/0009-cloud-slice.md) for the decisions. Nothing here has
been applied yet: CI only validates. Every `apply` is a manual step. Run commands from the repo root
in Git Bash.

## 1. Prerequisites

- **AWS account.**
  - An admin AWS CLI v2 profile for the bootstrap and the applies (`aws sts get-caller-identity`).
  - Region `eu-west-1` (the variable default).
  - A default VPC in that region, which serving uses.
- **Tools on PATH:**
  - Terraform ≥ 1.9 (1.16.4 was used for validation);
  - `tflint` 0.64.0 (`TFLINT=...` if it isn't on PATH);
  - Docker, only if you enable serving;
  - `gh`, optional.
- **Snowflake trial.** Pick AWS, EU (Ireland), the same region as the buckets. Find the identifier in
  a Snowsight worksheet:
  `SELECT CURRENT_ORGANIZATION_NAME(), CURRENT_ACCOUNT_NAME();`
  This gives `organization_name` and `account_name`. dbt's `SNOWFLAKE_ACCOUNT` is `<org>-<account>`.
- **Two key pairs** (Snowflake key-pair auth: unencrypted PKCS#8; keep them outside the repo, e.g.
  `~/.snowflake/`). You need one for the Terraform user and one for the dbt service user:
  ```sh
  mkdir -p ~/.snowflake && chmod 700 ~/.snowflake
  openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out ~/.snowflake/tf_key.p8 -nocrypt
  chmod 600 ~/.snowflake/tf_key.p8
  openssl rsa -in ~/.snowflake/tf_key.p8 -pubout -out ~/.snowflake/tf_key.pub
  grep -v -- ----- ~/.snowflake/tf_key.pub | tr -d '\n'      # one-line public key
  ```
  Repeat with `~/.snowflake/dbt_key.p8` / `dbt_key.pub`. `.gitignore` also ignores `*.p8`, `*.pem`,
  `*_key.pub` and `backend.hcl` as a backstop. Then create the Terraform user as `ACCOUNTADMIN`.
  Terraform needs `ACCOUNTADMIN` for the storage integration and the resource monitor.
  ```sql
  CREATE USER TERRAFORM TYPE = SERVICE RSA_PUBLIC_KEY = '<one-line tf_key.pub>' DEFAULT_ROLE = ACCOUNTADMIN;
  GRANT ROLE ACCOUNTADMIN TO USER TERRAFORM;
  ```
  The dbt user `DBT_SERVICE` is created by Terraform from `dbt_rsa_public_key`.
- **GitHub settings.** These are the names `cloud-batch.yml` and `terraform.yml` read.
  1. Create an **environment `cloud`**, and set *Deployment branches* to **`main` only**. The OIDC trust
     accepts `environment:cloud`, so this rule is what stops other branches.
  2. **Repository variables:**

     | Variable | Value (Terraform output) |
     |---|---|
     | `CLOUD_ENABLED` | `true` (anything else disables `cloud-batch.yml`) |
     | `AWS_REGION` | `eu-west-1` |
     | `AWS_RUN_ROLE_ARN` | `github_run_role_arn` |
     | `EMR_APPLICATION_ID` | `emr_application_id` |
     | `EMR_JOB_ROLE_ARN` | `emr_job_role_arn` |
     | `ARTIFACTS_BUCKET` | `artifacts_bucket` |
     | `LAKEHOUSE_BUCKET` | `lakehouse_bucket` |
     | `SNOWFLAKE_ACCOUNT` | `<org>-<account>` |
     | `SNOWFLAKE_USER` | `DBT_SERVICE` (`dbt_user`) |
     | `SNOWFLAKE_ROLE` | `TRANSFORMER` |
     | `SNOWFLAKE_WAREHOUSE` | `RETAIL_WH` (`warehouse`) |
     | `SNOWFLAKE_DATABASE` | `RETAIL` (`database`) |
     | `SNOWPIPE_TIMEOUT_SECONDS` | optional: how long to poll Snowflake for the run's files before failing (default `900`) |

  3. **Secrets:** `SNOWFLAKE_PRIVATE_KEY`, the PEM content of `dbt_key.p8`.
  4. **Optional, for `plan` in `terraform.yml`.** It runs on push to `main` or on manual dispatch,
     never on PRs, because the OIDC trust accepts only `environment:cloud`. Without these values the
     job skips `plan` with a notice. The plan role is read-only: it reads the state bucket and
     describes the project's resources, and the job runs `plan -lock=false`, so it never takes the
     lock (`TF_LOCK_TABLE` is only passed to the backend config). The plan prints to a public log;
     `alert_email`, `serving_allowed_cidr` and the Snowflake account names are `sensitive` and show as
     `(sensitive value)`, but ARNs still contain the account id.
     - Variables: `AWS_PLAN_ROLE_ARN` (`github_deploy_role_arn`), `TF_STATE_BUCKET`, `TF_LOCK_TABLE`.
     - Secrets: `TFVARS_DEMO` and `TFVARS_SNOWFLAKE` (the two `terraform.tfvars` contents), and
       `SNOWFLAKE_TF_PRIVATE_KEY` (`tf_key.p8`).

## 2. Bootstrap (once)

```sh
cp terraform/aws/bootstrap/terraform.tfvars.example terraform/aws/bootstrap/terraform.tfvars   # set state_bucket_name
make cloud-bootstrap      # state bucket (versioned, SSE-S3, prevent_destroy) + DynamoDB lock table
```

Bootstrap keeps its own state locally (`terraform/aws/bootstrap/terraform.tfstate`, git-ignored),
so back that file up. Then set up the other two roots. In `terraform/aws/envs/demo` and
`terraform/snowflake`:
- copy `backend.hcl.example` to `backend.hcl`;
- copy `terraform.tfvars.example` to `terraform.tfvars`;
- fill in the state bucket and lock table names, plus `github_repo` and `alert_email` for AWS.

The backends use S3-native locking (`use_lockfile = true`) alongside the DynamoDB table, which stays
because DynamoDB locking is deprecated but still supported. If the AWS account already has a GitHub OIDC
provider (`token.actions.githubusercontent.com`), import it before the first apply:

```sh
terraform -chdir=terraform/aws/envs/demo init -backend-config=backend.hcl
terraform -chdir=terraform/aws/envs/demo import module.iam.aws_iam_openid_connect_provider.github \
  arn:aws:iam::<account-id>:oidc-provider/token.actions.githubusercontent.com
```

`make cloud-validate` runs fmt, validate, tflint and any `terraform test` suites, all without credentials.
`make cloud-plan` previews both roots.

## 3. Two-step apply (Snowflake integration ↔ AWS role)

The storage integration needs the AWS role ARN. The role's trust policy needs the integration's IAM
user and external id, and the S3 notification needs the pipes' SQS queue. So the AWS root is applied
twice:

1. **AWS, pass 1.** Leave `snowflake_iam_user_arn`, `snowflake_external_id` and `snowflake_sqs_arn`
   empty in `envs/demo/terraform.tfvars`. This creates storage, IAM, observability and EMR. The
   Snowflake role and the S3 notification are skipped. `snowflake_role_arn` is already output: the
   name is deterministic, so the ARN is a prediction.
2. **Snowflake.** In `terraform/snowflake/terraform.tfvars` set:
   - `lakehouse_bucket` and `storage_aws_role_arn` = the pass-1 outputs `lakehouse_bucket` and
     `snowflake_role_arn` (`retail-data-platform-demo-<account-id>-lakehouse`,
     `arn:aws:iam::<account-id>:role/retail-data-platform-demo-snowflake-export-read`);
   - `dbt_rsa_public_key` = the one-line `dbt_key.pub`;
   - `user = "TERRAFORM"`.

   Pass the private key with `export TF_VAR_private_key="$(cat ~/.snowflake/tf_key.p8)"`.
3. **AWS, pass 2.** Copy three Snowflake outputs into `envs/demo/terraform.tfvars`:
   - `storage_aws_iam_user_arn` → `snowflake_iam_user_arn`;
   - `storage_aws_external_id` → `snowflake_external_id`;
   - `pipe_notification_channel` → `snowflake_sqs_arn`.

   This pass creates the read-only role on `export/silver/*` and the S3 → SQS notification.

One Make target per step: `make cloud-up-aws` (1), `make cloud-up-snowflake` (2) and
`make cloud-up-aws-integration` (3). For later changes, `make cloud-up` applies AWS, then Snowflake.
Terraform prompts before each apply; `CONFIRM=yes` auto-approves.

If step 2 stops on the stage or a pipe because the role doesn't exist yet:
1. Read `STORAGE_AWS_IAM_USER_ARN` and `STORAGE_AWS_EXTERNAL_ID` from `DESC INTEGRATION
   RETAIL_S3_EXPORT`.
2. Apply AWS with only those two variables.
3. Re-run the Snowflake apply.
4. Apply AWS a third time with `snowflake_sqs_arn`.

Check from a Snowsight worksheet:

```sql
DESC INTEGRATION RETAIL_S3_EXPORT;
LIST @RETAIL.SILVER.EXPORT_STAGE;     -- empty before the first export, but no access error
```

## 4. Seed bronze

The streaming layer isn't deployed, so copy the local bronze Delta tables once (they need a filled
local lakehouse: [ingestion runbook](ingestion.md)). Copy only `bronze/`: silver starts fresh in S3
with its own checkpoints.

```sh
AWS_ACCESS_KEY_ID=minio AWS_SECRET_ACCESS_KEY=minio12345 \
  aws s3 sync s3://lakehouse/bronze /tmp/rdp-bronze --endpoint-url http://127.0.0.1:9000
aws s3 sync /tmp/rdp-bronze "s3://$(terraform -chdir=terraform/aws/envs/demo output -raw lakehouse_bucket)/bronze"
```

Use the MinIO user, password and port from your `.env`.

## 5. First batch run (`cloud-batch.yml`)

Set the GitHub variables from §1 (Terraform outputs: `terraform -chdir=terraform/aws/envs/demo
output`). Then dispatch the workflow from `main`:

```sh
gh workflow run cloud-batch.yml --ref main
gh run watch
```

It also runs on a weekly schedule (Mondays 06:00 UTC). Every run uses the `cloud` environment and
the GitHub run id as `<run_id>`. The steps:
1. Assume `AWS_RUN_ROLE_ARN` through OIDC.
2. Upload the job bundle (`lakehouse/spark/cloud/package.sh`: `lakehouse_spark.zip` + `entrypoint.py`)
   to `s3://<artifacts>/jobs/<git-sha>/`.
3. Run EMR `silver --run-id <id>`, then `export --run-id <id>`, waiting for each with a 45-minute
   timeout.
4. Download `s3://<lakehouse>/export/_manifests/<run_id>.json` (parts and rows per table) and poll
   Snowflake (`lakehouse_spark.cloud.snowpipe_wait`) until every table holds exactly that run's files
   and rows, up to `SNOWPIPE_TIMEOUT_SECONDS`.
5. Run `dbt build --target snowflake --exclude-resource-type unit_test --vars "{export_run_id: '<id>'}"`
   (key-pair from `SNOWFLAKE_PRIVATE_KEY`), so GOLD is built from exactly this run. The dbt unit tests
   run on duckdb in CI.
6. Write the job-run ids and states to the job summary.

Then verify:

```sh
aws s3 ls --recursive "s3://<lakehouse>/export/silver/" | head      # <domain>/<table>/<run_id>/part-NNNNN.parquet + _SUCCESS
aws emr-serverless list-job-runs --application-id <emr_application_id> --max-items 5
```

```sql
SELECT SYSTEM$PIPE_STATUS('RETAIL.SILVER.ORDERS_PIPE');
SELECT * FROM TABLE(RETAIL.INFORMATION_SCHEMA.COPY_HISTORY(TABLE_NAME => 'RETAIL.SILVER.ORDERS',
  START_TIME => DATEADD(hour, -24, CURRENT_TIMESTAMP())));
SELECT split_part(_EXPORT_FILE, '/', 3) AS run_id, COUNT(*) FROM RETAIL.SILVER.ORDERS GROUP BY 1;
SHOW TABLES IN SCHEMA RETAIL.GOLD;
```

If the job fails, look in the EMR log group (`terraform output emr_log_group_name`). A `Traceback`
there also fires the CloudWatch alarm email. Snowpipe loads asynchronously: if the poll times out it
lists each table's missing files and rows (`table: files a/b, rows c/d`); check `COPY_HISTORY` and
`SYSTEM$PIPE_STATUS`, then re-run the workflow or raise `SNOWPIPE_TIMEOUT_SECONDS`. A manual
`make cloud-dbt` (the `SNOWFLAKE_*` env vars set) reads each table's latest run; for an exact run add
`--vars "{export_run_id: '<id>'}"` to the dbt command.

## 6. Enabling serving (optional)

Serving is off by default (`enable_serving = false`). When it's on, the ALB bills hourly even with
0 tasks.

1. In `envs/demo/terraform.tfvars` set:
   - `enable_serving = true`;
   - `serving_image_tag = "<git-sha>"`;
   - `serving_allowed_cidr = "<your-ip>/32"` (`0.0.0.0/0` is rejected).

   Run `make cloud-up-aws`. This creates ECR, the ECS cluster and service at `desired_count = 0`, the ALB
   and the secret.
2. Push the image. Tags are immutable, so use a new tag for every push:
   ```sh
   make ml-build
   ECR=$(terraform -chdir=terraform/aws/envs/demo output -raw ecr_repository_url)
   aws ecr get-login-password --region eu-west-1 | docker login --username AWS --password-stdin "${ECR%%/*}"
   docker tag retail-ml "$ECR:<git-sha>" && docker push "$ECR:<git-sha>"
   ```
3. Set the secret value. Without one, tasks fail at secret injection:
   `aws secretsmanager put-secret-value --secret-id <serving_config_secret_arn> --secret-string <mlflow-uri>`.
   Without a reachable MLflow, `/health` still returns 200 with `model_loaded=false`.
4. Scale with `serving_desired_count = 1` and `make cloud-up-aws`. Use the tfvar rather than
   `aws ecs update-service`, because the next apply would reset the count.
5. Check with `curl -s "$(terraform -chdir=terraform/aws/envs/demo output -raw serving_url)/health"`.
6. Scale back to 0, or set `enable_serving = false`, when you're done.

## 7. Power BI on Snowflake

The committed PBIP reads the local Parquet marts ([bi/README.md](../../bi/README.md)). For the cloud
evidence, connect Power BI Desktop to Snowflake:
1. Grant read access to your Snowsight user: `GRANT ROLE REPORTER TO USER <you>;`. `REPORTER` can
   only read `GOLD`.
2. *Get Data → Snowflake*:
   - Server: `<org>-<account>.snowflakecomputing.com`;
   - Warehouse: `RETAIL_WH`;
   - Advanced options: Role `REPORTER`, Database `RETAIL`.
3. Sign in with Microsoft Entra ID, username/password, or *Key Pair Auth (ADBC)*. The connector
   supports all three. Snowflake is deprecating single-factor passwords.
4. Silver timestamps are `TIMESTAMP_LTZ`. `DBT_SERVICE` has `TIMEZONE = UTC` (Terraform), but your
   user gets the account default (`America/Los_Angeles`), so `_utc` columns look shifted. Run
   `ALTER USER <you> SET TIMEZONE = 'UTC';`, or account-wide `ALTER ACCOUNT SET TIMEZONE = 'UTC';`.
5. In Navigator, pick the marts in `RETAIL.GOLD` and choose **Import**. With Import, the warehouse
   only resumes on refresh and suspends after 60 s; DirectQuery resumes it on every visual.

## 8. Teardown

```sh
make cloud-down     # asks for "yes" (CONFIRM=yes skips): destroys Snowflake, then envs/demo
```

Buckets (`force_destroy`), ECR (`force_delete`) and the secret (no recovery window) are removed with
their contents. The bootstrap state bucket and lock table remain. The state bucket has
`prevent_destroy`; to remove it, empty and delete it by hand. Afterwards:
- set `CLOUD_ENABLED` to `false`, or the weekly schedule keeps trying;
- optionally drop the `TERRAFORM` user and the trial;
- confirm that only bootstrap remains:
  `aws resourcegroupstaggingapi get-resources --tag-filters Key=project,Values=retail-data-platform`.

## 9. Evidence capture checklist

Capture these into [`docs/cloud-evidence/`](../cloud-evidence/README.md), using the file names
listed there. Capture early, because the Snowflake trial lasts 30 days. Redact account ids, emails
and ARNs.
- [ ] `terraform apply` tails for both roots, including pass 2, and `terraform output`.
- [ ] `cloud-batch.yml` run summary (run ids) and the EMR job runs list (both `SUCCESS`, durations).
- [ ] `aws s3 ls` of `export/silver/` for one run id.
- [ ] Snowflake: `COPY_HISTORY` / pipe status, `SILVER` row counts by run id, query history of the
  dbt build, `SHOW TABLES IN SCHEMA RETAIL.GOLD`.
- [ ] Power BI report page on Snowflake (`REPORTER`).
- [ ] Serving `/health`, if enabled.
- [ ] Cost: Cost Explorer by service (tag `project`), Budget page, Snowflake metering / resource
  monitor.
- [ ] Teardown tail and the tagging-API check.

## 10. Cost guard-rails

- **AWS Budget** `retail-data-platform-demo-monthly`: $25, ACTUAL alerts at 50/80/100 % to
  `alert_email`. Confirm the SNS subscription email after the first apply.
- **EMR Serverless:**
  - auto-stop after 5 idle minutes;
  - maximum capacity of 16 vCPU / 64 GB;
  - billed only while jobs run.
- **Serving:** off by default. When enabled, `desired_count` defaults to 0 and the ALB is limited to
  one CIDR.
- **S3:** `landing/` objects and noncurrent versions expire after 7 days. Log groups keep 7 days.
- **Snowflake:**
  - `RETAIL_WH` is XS, with 60 s auto-suspend, initially suspended;
  - `RETAIL_MONITOR` (`credit_quota`, default 5) notifies at 50/80 % and suspends at 100 %;
  - the monitor covers warehouses only, so Snowpipe's serverless credits are not capped. Watch them
    in the trial's usage page.
- **Schedule:** the weekly `cloud-batch.yml` schedule runs only while `CLOUD_ENABLED == 'true'`.
- **Teardown:** run `make cloud-down` as soon as the evidence is captured. Rough monthly estimates
  are in [cloud-architecture.md](../cloud-architecture.md#component-mapping-local--aws-managed).

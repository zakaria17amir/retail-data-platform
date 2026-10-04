SHELL := /bin/sh
# .env is a default; a POSTGRES_DSN set in the shell must win so tests/one-offs never hit the dev DB by accident
SHELL_POSTGRES_DSN := $(POSTGRES_DSN)
-include .env
ifneq ($(SHELL_POSTGRES_DSN),)
POSTGRES_DSN := $(SHELL_POSTGRES_DSN)
endif
export

PROFILE ?= core

.PHONY: up down destroy ps logs sync lint test test-integration test-spark test-ingest test-realtime replay sim reset-bronze download seed seed-sample sample status silver quality maintain gold dbt-parse sqlfluff airflow-cli ml-build ml recommend-load cloud-validate cloud-bootstrap cloud-plan cloud-up cloud-down cloud-dbt

# --wait treats exited one-shot *-init containers as failures: wait on the long-running ones, then `docker wait` on each init service and require exit 0
up:
	docker compose --profile $(PROFILE) up -d
	docker compose --profile $(PROFILE) up -d --wait $$(docker compose --profile $(PROFILE) config --services | grep -v -e '-init$$')
	@for s in $$(docker compose --profile $(PROFILE) config --services | grep -e '-init$$'); do \
		docker wait $$(docker compose --profile $(PROFILE) ps -aq $$s) >/dev/null; \
		docker compose --profile $(PROFILE) ps -a --format '{{.Service}} {{.State}} {{.ExitCode}}' | grep -q "^$$s exited 0$$" \
			|| { echo "$$s failed"; docker compose --profile $(PROFILE) logs $$s; exit 1; }; \
	done

down:
	docker compose --profile '*' down

destroy:
	@printf 'This deletes all Compose volumes. Continue? [y/N] '; read ans; 	if [ "$$ans" = y ]; then docker compose --profile '*' down -v; else echo Aborted.; fi

ps:
	docker compose --profile '*' ps

logs:
	docker compose --profile '*' logs -f $(SERVICE)

sync:
	uv sync --all-packages --group dev

lint:
	uv run ruff check .
	uv run ruff format --check .
	uv run mypy ingestion lakehouse tests

test:
	uv run pytest -m "not integration and not spark and not ingest and not realtime"

test-integration:
	uv run pytest -m integration

test-spark:
	docker compose --profile ingest run --rm --no-deps spark pytest -p no:cacheprovider -W ignore::pytest.PytestUnknownMarkWarning /opt/lakehouse/tests -m spark

download:
	uv run --package replayer replayer download --dest $(OLIST_DATA_DIR)

seed:
	uv run --package replayer replayer seed --data-dir $(OLIST_DATA_DIR)

seed-sample:
	uv run --package replayer replayer seed --data-dir tests/fixtures/olist_sample

sample:
	uv run --package replayer replayer sample --src $(OLIST_DATA_DIR) --dest tests/fixtures/olist_sample --n-orders 200

status:
	uv run --package replayer replayer status
	uv run --package lakehouse-spark bronze status

replay:
	uv run --package replayer replayer live $(REPLAY_ARGS)

sim:
	uv run --package clickstream-sim clickstream-sim run $(SIM_ARGS)

test-ingest:
	@echo "WARNING: reseeds $$POSTGRES_DB with the sample fixture and clears bronze; set ALLOW_RESEED=1 to proceed"
	uv run pytest -m ingest tests/integration

test-realtime:
	@echo "WARNING: writes e2e sessions to bronze and an e2e order to $$POSTGRES_DB; set ALLOW_RESEED=1 to proceed"
	uv run pytest -m realtime tests/integration -rP

silver:
	docker compose --profile ingest run --rm --no-deps spark spark-submit /opt/lakehouse/src/lakehouse_spark/silver/job.py $(SILVER_ARGS)

quality:
	uv run --package lakehouse-quality quality run

maintain:
	docker compose --profile ingest run --rm --no-deps spark spark-submit /opt/lakehouse/src/lakehouse_spark/silver/maintain.py

reset-bronze:
	sh ingestion/reset-bronze.sh $(or $(SEED),seed)

# dbt-duckdb creates neither the DuckDB file's parent directory nor GOLD_DIR; paths resolve from analytics/
DBT_WAREHOUSE_DIR = mkdir -p "$$(dirname "$${DUCKDB_PATH:-../data/warehouse/retail.duckdb}")" "$${GOLD_DIR:-../data/gold}"
# dbt-snowflake stays out of the workspace (it would downgrade certifi for every member): ephemeral pinned env
DBT_SNOWFLAKE = uv tool run --python 3.12 --exclude-newer 2026-09-27T00:00:00Z --from 'dbt-core==1.12.5' --with 'dbt-snowflake==1.12.1' dbt

gold:
	cd analytics && $(DBT_WAREHOUSE_DIR) && uv run dbt deps && uv run dbt build --target local

dbt-parse:
	cd analytics && $(DBT_WAREHOUSE_DIR) && uv run dbt deps && uv run dbt parse --target local && $(DBT_SNOWFLAKE) parse --target snowflake --target-path target-snowflake

sqlfluff:
	cd analytics && uv run sqlfluff lint models

airflow-cli:
	docker compose exec airflow airflow $(ARGS)

ml-build:
	GIT_SHA=$$(git rev-parse --short HEAD) docker compose --profile ml build serving

ml:
	GIT_SHA=$$(git rev-parse --short HEAD) docker compose --profile ml run --rm --build ml-cli $(ARGS)

# /recommend p95 target < 50 ms at 20 users; needs `make up PROFILE=realtime` and published candidates
recommend-load:
	uv run --project ml locust -f ml/locustfile_recommend.py --headless -u 20 -r 5 -t 60s --host http://127.0.0.1:$${SERVING_PORT:-8000}

# Cloud (phase 7). Applies are manual: terraform prompts unless CONFIRM=yes. Backends read backend.hcl,
# variables terraform.tfvars (both git-ignored copies of the *.example files). TFLINT = tflint binary.
TF_ROOTS = terraform/aws/bootstrap terraform/aws/envs/demo terraform/snowflake
TFLINT ?= tflint
TF_AUTO = $(if $(filter yes,$(CONFIRM)),-auto-approve)
TF_BOOTSTRAP = terraform -chdir=terraform/aws/bootstrap
TF_DEMO = terraform -chdir=terraform/aws/envs/demo
TF_SNOWFLAKE = terraform -chdir=terraform/snowflake

cloud-validate:
	terraform fmt -check -recursive terraform
	@set -e; for d in $(TF_ROOTS); do echo "== validate $$d"; \
		terraform -chdir=$$d init -backend=false -input=false -lockfile=readonly >/dev/null; \
		terraform -chdir=$$d validate -no-color; done
	"$(TFLINT)" --init --config "$(CURDIR)/.tflint.hcl"
	"$(TFLINT)" --chdir terraform --recursive --config "$(CURDIR)/.tflint.hcl" --format compact
	@set -e; for t in $$(find terraform -type d -name tests -not -path '*/.terraform/*'); do d=$${t%/tests}; \
		echo "== test $$d"; terraform -chdir=$$d init -backend=false -input=false >/dev/null; terraform -chdir=$$d test -no-color; done

cloud-bootstrap:
	$(TF_BOOTSTRAP) init -input=false
	$(TF_BOOTSTRAP) apply $(TF_AUTO)

cloud-plan:
	$(TF_DEMO) init -input=false -backend-config=backend.hcl
	$(TF_DEMO) plan
	$(TF_SNOWFLAKE) init -input=false -backend-config=backend.hcl
	$(TF_SNOWFLAKE) plan

# two-step apply (Snowflake integration <-> AWS role): run again after filling the snowflake_* variables
cloud-up:
	$(TF_DEMO) init -input=false -backend-config=backend.hcl
	$(TF_DEMO) apply $(TF_AUTO)
	$(TF_SNOWFLAKE) init -input=false -backend-config=backend.hcl
	$(TF_SNOWFLAKE) apply $(TF_AUTO)

# everything except the bootstrap state bucket/lock table
cloud-down:
	@[ "$(CONFIRM)" = yes ] || { printf 'Destroy the Snowflake objects and the AWS demo env (state bucket kept)? Type yes: '; \
		read ans; [ "$$ans" = yes ]; } || { echo 'Aborted (CONFIRM=yes skips this prompt).'; exit 1; }
	$(TF_SNOWFLAKE) init -input=false -backend-config=backend.hcl
	$(TF_SNOWFLAKE) destroy -auto-approve
	$(TF_DEMO) init -input=false -backend-config=backend.hcl
	$(TF_DEMO) destroy -auto-approve

# dbt build on Snowflake (cloud-batch.yml); SNOWFLAKE_* env, key-pair auth via SNOWFLAKE_PRIVATE_KEY
cloud-dbt:
	cd analytics && $(DBT_SNOWFLAKE) deps && $(DBT_SNOWFLAKE) build --target snowflake

SHELL := /bin/sh
# .env is a default; a POSTGRES_DSN set in the shell must win so tests/one-offs never hit the dev DB by accident
SHELL_POSTGRES_DSN := $(POSTGRES_DSN)
-include .env
ifneq ($(SHELL_POSTGRES_DSN),)
POSTGRES_DSN := $(SHELL_POSTGRES_DSN)
endif
export

PROFILE ?= core

.PHONY: up down destroy ps logs sync lint test test-integration test-spark test-ingest replay sim reset-bronze download seed seed-sample sample status silver quality maintain

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
	uv run pytest -m "not integration and not spark and not ingest"

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

silver:
	docker compose --profile ingest run --rm --no-deps spark spark-submit /opt/lakehouse/src/lakehouse_spark/silver/job.py $(SILVER_ARGS)

quality:
	uv run --package lakehouse-quality quality run

maintain:
	docker compose --profile ingest run --rm --no-deps spark spark-submit /opt/lakehouse/src/lakehouse_spark/silver/maintain.py

reset-bronze:
	sh ingestion/reset-bronze.sh $(or $(SEED),seed)

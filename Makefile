SHELL := /bin/sh
# .env is a default; a POSTGRES_DSN set in the shell must win so tests/one-offs never hit the dev DB by accident
SHELL_POSTGRES_DSN := $(POSTGRES_DSN)
-include .env
ifneq ($(SHELL_POSTGRES_DSN),)
POSTGRES_DSN := $(SHELL_POSTGRES_DSN)
endif
export

PROFILE ?= core

.PHONY: up down destroy ps logs sync lint test test-integration test-spark download seed seed-sample sample status

# --wait treats exited one-shot *-init containers as failures, so wait on the long-running ones only
up:
	docker compose --profile $(PROFILE) up -d
	docker compose --profile $(PROFILE) up -d --wait $$(docker compose --profile $(PROFILE) config --services | grep -v -e '-init$$')
	@docker compose --profile $(PROFILE) ps -a --format '{{.Service}} {{.State}} {{.ExitCode}}' | grep -q '^minio-init exited 0$$' \
		|| { echo "minio-init failed"; docker compose --profile $(PROFILE) logs minio-init; exit 1; }

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
	uv run mypy ingestion

test:
	uv run pytest -m "not integration"

test-integration:
	uv run pytest -m integration

test-spark:
	docker compose --profile ingest run --rm --no-deps spark pytest -p no:cacheprovider /opt/lakehouse/tests -m spark

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

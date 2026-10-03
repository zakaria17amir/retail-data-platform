SHELL := /bin/sh
-include .env
export

PROFILE ?= core

.PHONY: up down destroy ps logs sync lint test test-integration download seed seed-sample sample status

# --wait treats exited one-shot *-init containers as failures, so wait on the long-running ones only
up:
	docker compose --profile $(PROFILE) up -d
	docker compose --profile $(PROFILE) up -d --wait $$(docker compose --profile $(PROFILE) config --services | grep -v -e '-init$$')

down:
	docker compose --profile '*' down

destroy:
	@printf 'This deletes all Compose volumes. Continue? [y/N] ' && read ans && [ "$$ans" = y ] \
		&& docker compose --profile '*' down -v || echo 'Aborted.'

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

download:
	uv run replayer download --dest $(OLIST_DATA_DIR)

seed:
	uv run replayer seed --data-dir $(OLIST_DATA_DIR)

seed-sample:
	uv run replayer seed --data-dir tests/fixtures/olist_sample

sample:
	uv run replayer sample --src $(OLIST_DATA_DIR) --dest tests/fixtures/olist_sample --n-orders 200

status:
	uv run replayer status

# Phase 0 — Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A clean clone runs `make up` + `make seed` and ends with the raw Olist dataset in Postgres and an empty `lakehouse` bucket in MinIO, with lint/type/unit/integration CI green.

**Architecture:** Root `uv` workspace with one package so far (`ingestion/replayer`). Docker Compose `core` profile = Postgres 16 (logical replication on) + MinIO. The `replayer` CLI owns the Olist schema DDL and bulk load so Postgres stays generic; a committed 200-order sample fixture lets CI and unit tests run without Kaggle credentials.

**Tech Stack:** Python 3.12, uv, ruff, mypy, pytest, psycopg 3, polars, kagglehub, Docker Compose, Postgres 16, MinIO, GitHub Actions, pre-commit.

**Spec:** `docs/superpowers/specs/retail-data-platform-design.md` (sections 2, 4.1, 5 containers, 12, 13 row 0).

## Global Constraints

- Python 3.12 pinned in `.python-version`; project-local `.venv/` via `uv sync`; never system Python.
- `ruff check`, `ruff format --check`, `mypy --strict` pass on every package.
- All Docker images pinned to an exact tag (no `latest`, no bare major).
- Every Compose service has a `healthcheck` and `deploy.resources.limits.memory`.
- Config via env vars with defaults documented in `.env.example`; no secrets in git.
- Shell scripts and Makefile recipes POSIX-sh compatible; `.gitattributes` forces LF.
- Owners per `AGENTS.md`: `platform-engineer` → root files, Compose, Makefile, CI, docs/runbooks; `data-engineer` → `ingestion/`.

## Review Focus

1. Olist CSVs contain quoted fields with embedded commas and newlines (review messages) — loader must use a real CSV parser; pinned by Task 3 `test_copy_handles_quoted_newlines`.
2. Re-running `make seed` on a loaded database must not duplicate rows — pinned by Task 3 `test_seed_is_idempotent`.
3. `order_reviews.review_id` is **not** unique in the real data — schema must accept duplicates; pinned by Task 3 `test_duplicate_review_ids_load`.
4. Missing Kaggle credentials must produce a one-line actionable error, not a traceback — pinned by Task 4 `test_download_without_credentials_exits_2`.
5. A wrong DSN must fail fast without printing the password — pinned by Task 3 `test_connection_error_hides_password`.

---

## File Structure

```
.python-version  .gitattributes  .editorconfig  .pre-commit-config.yaml  .env.example
pyproject.toml                       # uv workspace root: dev deps, ruff/mypy/pytest config
uv.lock
Makefile  docker-compose.yml  README.md
docs/adr/0000-template.md  docs/adr/0001-olist-hybrid-data-strategy.md
docs/runbooks/windows-setup.md  scripts/wslconfig.example
ingestion/replayer/pyproject.toml
ingestion/replayer/src/replayer/__init__.py
ingestion/replayer/src/replayer/cli.py          # argparse entry point: download | seed | sample | status
ingestion/replayer/src/replayer/schema.py       # TABLES registry + DDL apply
ingestion/replayer/src/replayer/load.py         # COPY-based bulk load, counts
ingestion/replayer/src/replayer/download.py     # kagglehub download → data dir
ingestion/replayer/src/replayer/sample.py       # consistent N-order subsample
ingestion/replayer/sql/olist_schema.sql
ingestion/replayer/tests/{conftest.py,test_schema.py,test_load.py,test_download.py,test_sample.py}
tests/fixtures/olist_sample/*.csv               # 200-order consistent subset (committed)
.github/workflows/ci.yml
```

---

### Task 1: Repository scaffold (owner: platform-engineer)

**Files:**
- Create: `.python-version`, `.gitattributes`, `.editorconfig`, `pyproject.toml`, `.pre-commit-config.yaml`, `.env.example`, `README.md`, `docs/adr/0000-template.md`, `docs/adr/0001-olist-hybrid-data-strategy.md`, `docs/runbooks/windows-setup.md`, `scripts/wslconfig.example`
- Create (placeholder so the workspace resolves): `ingestion/replayer/pyproject.toml`, `ingestion/replayer/src/replayer/__init__.py`

**Interfaces:**
- Produces: root `pyproject.toml` with `[tool.uv.workspace] members = ["ingestion/*"]`; dev dependency group `dev = ["ruff", "mypy", "pytest", "pytest-cov", "pre-commit"]`; `[tool.ruff] line-length = 100, target-version = "py312"`, `select = ["E","F","I","UP","B","SIM"]`; `[tool.mypy] strict = true`; `[tool.pytest.ini_options] markers = ["integration: needs running Compose services"]`, `testpaths = ["ingestion"]`.
- Produces: `ingestion/replayer/pyproject.toml` — name `replayer`, `requires-python = ">=3.12,<3.13"`, src layout, `[project.scripts] replayer = "replayer.cli:main"`, dependencies `psycopg[binary]`, `polars`, `kagglehub`, `python-dotenv`; build backend `hatchling`.
- Produces: `.env.example` keys with defaults:
  `POSTGRES_USER=retail`, `POSTGRES_PASSWORD=retail`, `POSTGRES_DB=retail`, `POSTGRES_PORT=5432`,
  `POSTGRES_DSN=postgresql://retail:retail@localhost:5432/retail`,
  `MINIO_ROOT_USER=minio`, `MINIO_ROOT_PASSWORD=minio12345`, `MINIO_PORT=9000`, `MINIO_CONSOLE_PORT=9001`,
  `LAKEHOUSE_BUCKET=lakehouse`, `OLIST_DATA_DIR=data/raw/olist`,
  `KAGGLE_USERNAME=`, `KAGGLE_KEY=`, `OLLAMA_BASE_URL=http://host.docker.internal:11434`.

- [ ] **Step 1: Write `.python-version` (`3.12`), `.gitattributes` (`* text=auto eol=lf`, `*.pbix binary`, `*.png binary`), `.editorconfig` (LF, utf-8, 4-space Python, 2-space YAML).**
- [ ] **Step 2: Write root `pyproject.toml` and the placeholder `ingestion/replayer` package per Interfaces; run `uv sync --all-packages --group dev`.**
  Expected: `.venv/` created with Python 3.12.x (`uv run python --version`), `uv.lock` written.
- [ ] **Step 3: Write `.pre-commit-config.yaml`** with hooks `ruff` + `ruff-format` (astral-sh/ruff-pre-commit, pinned rev), `end-of-file-fixer`, `trailing-whitespace`, `check-yaml`, `detect-private-key`. Run `uv run pre-commit install && uv run pre-commit run --all-files`. Expected: all hooks pass.
- [ ] **Step 4: Write `.env.example`** per Interfaces, one comment line per key.
- [ ] **Step 5: Write `README.md` skeleton:** title, one-paragraph pitch, the architecture diagram from spec §3 verbatim, "Run it" (`cp .env.example .env`, `make up`, `make download`, `make seed`, `make status`), "Status" table of phases 0-8 with Phase 0 marked in progress, Olist attribution with CC BY-NC-SA 4.0 link.
- [ ] **Step 6: Write ADR template** (`docs/adr/0000-template.md`: Status / Context / Decision / Consequences / Alternatives) and `0001-olist-hybrid-data-strategy.md` from spec §4 and §14 row 1 (≤ 30 lines).
- [ ] **Step 7: Write `scripts/wslconfig.example`** (`[wsl2]` `memory=16GB` `processors=6` `swap=4GB`) and `docs/runbooks/windows-setup.md`: Docker Desktop with WSL2 backend, copy `wslconfig.example` to `%USERPROFILE%\.wslconfig` then `wsl --shutdown`, Ollama native install, Git `core.autocrlf=false` recommendation, Kaggle credential setup (`KAGGLE_USERNAME`/`KAGGLE_KEY` in `.env` from kaggle.com → Settings → API).
- [ ] **Step 8: Verify:** `uv run ruff check . && uv run ruff format --check . && uv run mypy ingestion`. Expected: no errors.
- [ ] **Step 9: Report** files created and verbatim output of Step 8 (lead commits).

---

### Task 2: Compose `core` profile and Makefile (owner: platform-engineer)

**Files:**
- Create: `docker-compose.yml`, `Makefile`

**Interfaces:**
- Consumes: `.env.example` keys from Task 1 (Compose reads `.env`).
- Produces: services `postgres` (profile `core`, image `postgres:16.6`, `command: ["postgres","-c","wal_level=logical","-c","max_wal_senders=10","-c","max_replication_slots=10"]`, port `${POSTGRES_PORT}:5432`, volume `pgdata`, healthcheck `pg_isready -U ${POSTGRES_USER} -d ${POSTGRES_DB}`, memory limit `1g`); `minio` (profile `core`, pinned `minio/minio:RELEASE.*` tag resolved at implementation time and recorded in an inline YAML comment with its digest, `server /data --console-address ":9001"`, ports `${MINIO_PORT}:9000` and `${MINIO_CONSOLE_PORT}:9001`, volume `miniodata`, healthcheck `mc ready local`, memory limit `1g`); `minio-init` (profile `core`, pinned `minio/mc` tag, `depends_on: minio: condition: service_healthy`, creates bucket `${LAKEHOUSE_BUCKET}` idempotently via `mc mb --ignore-existing`, `restart: "no"`). Network `retail`.
- Produces: Makefile targets (`.PHONY`, `SHELL := /bin/sh`, `PROFILE ?= core`):
  `up` (`docker compose --profile $(PROFILE) up -d --wait`), `down` (`docker compose --profile '*' down`), `destroy` (`down -v`, prompts `[y/N]`), `ps`, `logs` (`SERVICE` var), `sync` (`uv sync --all-packages --group dev`), `lint` (ruff check + format check + mypy), `test` (`uv run pytest -m "not integration"`), `test-integration` (`uv run pytest -m integration`), `download` (`uv run replayer download --dest $(OLIST_DATA_DIR)`), `seed` (`uv run replayer seed --data-dir $(OLIST_DATA_DIR)`), `seed-sample` (`uv run replayer seed --data-dir tests/fixtures/olist_sample`), `sample` (`uv run replayer sample --src $(OLIST_DATA_DIR) --dest tests/fixtures/olist_sample --n-orders 200`), `status` (`uv run replayer status`). Makefile loads `.env` if present (`-include .env` + `export`).

- [ ] **Step 1: Write `docker-compose.yml`** per Interfaces. Run `docker compose config --quiet`. Expected: exit 0.
- [ ] **Step 2: Write `Makefile`** per Interfaces.
- [ ] **Step 3: Verify services:** `cp .env.example .env` (if absent) then `make up`. Expected: `docker compose ps` shows `postgres` and `minio` `healthy`, `minio-init` `exited (0)`.
- [ ] **Step 4: Verify logical replication and bucket:** `docker compose exec postgres psql -U retail -d retail -c "show wal_level"` → `logical`; `docker compose run --rm minio-init mc ls local` (or equivalent) lists `lakehouse/`.
- [ ] **Step 5: Verify teardown/restart:** `make down && make up` → healthy again in < 60 s.
- [ ] **Step 6: Report** verbatim outputs of Steps 1, 3, 4. Leave services running for Task 3.

---

### Task 3: Olist schema and bulk load (owner: data-engineer)

**Files:**
- Create: `ingestion/replayer/sql/olist_schema.sql`, `ingestion/replayer/src/replayer/schema.py`, `ingestion/replayer/src/replayer/load.py`, `ingestion/replayer/src/replayer/cli.py` (subcommands `seed`, `status`)
- Test: `ingestion/replayer/tests/conftest.py`, `tests/test_schema.py`, `tests/test_load.py`

**Interfaces:**
- Consumes: `POSTGRES_DSN` env (default from `.env.example`); running Compose `core`.
- Produces: `schema.py`: `TABLES: tuple[Table, ...]` where `Table(name: str, csv_file: str, columns: tuple[str, ...])` in load order; `apply_schema(conn: psycopg.Connection) -> None` (executes `olist_schema.sql`, idempotent). `load.py`: `copy_csv(conn, table: Table, path: Path) -> int` (rows copied, `COPY ... FROM STDIN WITH (FORMAT csv, HEADER true)` streaming the file, UTF-8), `seed(dsn: str, data_dir: Path) -> dict[str, int]` (apply schema, `TRUNCATE` all tables in one transaction, copy each, return counts), `row_counts(dsn: str) -> dict[str, int]`. `cli.py`: `main(argv: list[str] | None = None) -> int`; `seed` prints `table: count` lines; `status` prints `table: count`; connection failures print `error: could not connect to <host>:<port>/<db>` and return 1; never echo the DSN.
- Schema (`olist_schema.sql`): `CREATE SCHEMA IF NOT EXISTS olist`; tables `customers` (PK `customer_id`), `orders` (PK `order_id`), `order_items` (PK `(order_id, order_item_id)`), `order_payments` (PK `(order_id, payment_sequential)`), `order_reviews` (surrogate `review_pk bigint generated always as identity` PK; **no** unique on `review_id`), `products` (PK `product_id`), `sellers` (PK `seller_id`), `geolocation` (surrogate identity PK), `product_category_name_translation` (PK `product_category_name`). Column names exactly as the Olist CSV headers. Types: `*_timestamp`/`*_date`/`*_at` columns → `timestamp`; `price`, `freight_value`, `payment_value` → `numeric(12,2)`; `*_lat`/`*_lng` → `double precision`; integer-looking columns (`order_item_id`, `payment_sequential`, `payment_installments`, `review_score`, `product_*_g|_cm`, `product_name_lenght`, `product_description_lenght`, `product_photos_qty`, `customer_zip_code_prefix`, `seller_zip_code_prefix`, `geolocation_zip_code_prefix`) → `integer`; everything else `text`. No foreign keys (raw layer). Every table gets `updated_at timestamptz not null default now()` and trigger `set_updated_at` (`CREATE OR REPLACE FUNCTION olist.set_updated_at() ... NEW.updated_at = now()`), `BEFORE UPDATE`. `CREATE PUBLICATION olist_cdc FOR TABLES IN SCHEMA olist` guarded by `IF NOT EXISTS` via a DO block.

- [ ] **Step 1: Write `conftest.py`:** fixture `dsn` from `POSTGRES_DSN` env, `pytest.skip` when unset or unreachable; fixture `tmp_csv_dir` writing a tiny consistent dataset (2 customers, 2 orders, 3 items, 2 payments, 3 reviews **with a duplicated `review_id`**, 2 products, 1 seller, 2 geolocation rows, 1 category translation; one review message containing a quoted comma and an embedded newline). Mark DB tests `@pytest.mark.integration`.
- [ ] **Step 2: Write failing tests:** `test_tables_registry_covers_all_nine_csvs` (unit: `{t.csv_file for t in TABLES}` equals the nine Olist filenames); `test_apply_schema_is_idempotent` (apply twice, no error, nine tables exist in `olist`); `test_seed_loads_counts` (counts equal fixture rows); `test_copy_handles_quoted_newlines` (the review message round-trips byte-exact); `test_duplicate_review_ids_load` (3 reviews loaded); `test_seed_is_idempotent` (seed twice → same counts); `test_updated_at_trigger_fires` (update a row → `updated_at` increases); `test_publication_exists` (`select 1 from pg_publication where pubname='olist_cdc'`); `test_connection_error_hides_password` (unit: `main(["status"])` with `POSTGRES_DSN=postgresql://u:secret@nohost:1/db` returns 1 and captured stderr does not contain `secret`).
- [ ] **Step 3: Run `uv run pytest ingestion/replayer -v`.** Expected: unit tests FAIL with import errors; integration tests FAIL (not skip) because services from Task 2 are running.
- [ ] **Step 4: Implement `olist_schema.sql`, `schema.py`, `load.py`, `cli.py`** per Interfaces (argparse; `python-dotenv` loads `.env` if present).
- [ ] **Step 5: Run `uv run pytest ingestion/replayer -v` and `uv run mypy ingestion && uv run ruff check ingestion`.** Expected: all PASS, no lint/type errors.
- [ ] **Step 6: Report** verbatim test output and `uv run replayer status` output against the fixture-seeded DB.

---

### Task 4: Download and sample fixture (owner: data-engineer)

**Files:**
- Create: `ingestion/replayer/src/replayer/download.py`, `ingestion/replayer/src/replayer/sample.py`; extend `cli.py` with `download`, `sample`
- Create: `tests/fixtures/olist_sample/*.csv` (generated, committed)
- Test: `ingestion/replayer/tests/test_download.py`, `tests/test_sample.py`

**Interfaces:**
- Consumes: `TABLES` from Task 3; `KAGGLE_USERNAME`/`KAGGLE_KEY` env.
- Produces: `download.py`: `download(dest: Path) -> Path` using `kagglehub.dataset_download("olistbr/brazilian-ecommerce")` then copying the nine CSVs into `dest` (skips if all nine already present); raises `MissingCredentials` (custom `RuntimeError` subclass) before calling kagglehub when both env vars are empty and `~/.kaggle/kaggle.json` is absent. CLI `download` prints `error: set KAGGLE_USERNAME and KAGGLE_KEY in .env (see docs/runbooks/windows-setup.md)` and returns 2 on `MissingCredentials`. `sample.py`: `sample(src: Path, dest: Path, n_orders: int, seed: int = 42) -> dict[str, int]` using polars: pick `n_orders` random `order_id`s, keep their orders, items, payments, reviews, the referenced customers, products, sellers, the category translations for those products, and geolocation rows whose zip prefix appears among those customers/sellers (cap 1 000 rows); write CSVs with the original filenames and headers; return counts.

- [ ] **Step 1: Write failing tests:** `test_download_without_credentials_exits_2` (monkeypatch env empty and `Path.home` to tmp; `main(["download","--dest",tmp])` returns 2, stderr contains `KAGGLE_USERNAME`); `test_download_skips_when_present` (nine empty files exist → returns dest without calling kagglehub, kagglehub monkeypatched to raise); `test_sample_is_referentially_consistent` (on `tmp_csv_dir` with `n_orders=1`: every `customer_id`/`product_id`/`seller_id` in sampled facts exists in sampled dims; no orders outside the chosen ids); `test_sample_is_deterministic` (same seed → identical files).
- [ ] **Step 2: Run tests → FAIL.**
- [ ] **Step 3: Implement `download.py`, `sample.py`, CLI wiring.**
- [ ] **Step 4: Run `uv run pytest ingestion/replayer -v`, mypy, ruff → PASS.**
- [ ] **Step 5 (needs Kaggle credentials in `.env`; otherwise stop and report):** `make download` → nine CSVs in `data/raw/olist`; `make sample` → `tests/fixtures/olist_sample/` populated; `make seed-sample && make status` → counts match sample output; `make seed && make status` → `orders: 99441`, `customers: 99441`, `order_items: 112650`, `order_reviews: 99224`, `products: 32951`, `sellers: 3095`, `geolocation: 1000163`, `order_payments: 103886`, `product_category_name_translation: 71`.
- [ ] **Step 6: Report** verbatim test output, fixture sizes (`du -sh tests/fixtures/olist_sample`, must be < 2 MB), and status outputs.

---

### Task 5: CI workflow (owner: platform-engineer)

**Files:**
- Create: `.github/workflows/ci.yml`

**Interfaces:**
- Consumes: `make lint`, `make test`, `make up`, `make seed-sample`, `make status` from Tasks 2-4; `tests/fixtures/olist_sample/` from Task 4.
- Produces: workflow `ci` on `push` to `main` and `pull_request`; job `lint-test` (ubuntu-latest, `astral-sh/setup-uv` with cache, `uv sync --all-packages --group dev`, `make lint`, `make test`); job `integration` (needs `lint-test`; `cp .env.example .env`; `make up`; `make seed-sample`; `make status | tee status.txt`; assert `grep -q '^orders: 200$' status.txt`; `make test-integration`; always `docker compose --profile core logs --tail 50` on failure and `make down`). Concurrency group per ref, cancel-in-progress.

- [ ] **Step 1: Write `ci.yml`** per Interfaces; all actions pinned to a major tag.
- [ ] **Step 2: Validate locally:** `uv run --with pyyaml python -c "import yaml,sys; yaml.safe_load(open('.github/workflows/ci.yml'))"`; if `actionlint` is available run it. Then rehearse the integration job's commands locally in order from a clean `make destroy`. Expected: `orders: 200` in status, integration tests pass.
- [ ] **Step 3: Report** verbatim outputs. (Lead pushes and confirms the Actions run is green once the GitHub repo exists.)

---

## Self-review notes

- Spec coverage for Phase 0 row: repo ✔ (T1), Compose `core` ✔ (T2), Olist in Postgres ✔ (T3-4), Makefile ✔ (T2), CI ✔ (T5). `.wslconfig` and Ollama are documented, not automated, because they live outside the repo (T1 Step 7).
- Interfaces consistent: `Table`, `TABLES`, `apply_schema`, `copy_csv`, `seed`, `row_counts`, `download`, `MissingCredentials`, `sample`, `main` used identically across T3-T5.
- Parallelism: T1 → (T2 ∥ T3-unit-parts) → T3 integration (needs T2) → T4 → T5. T2 and T3 can be dispatched together after T1 lands.
- User actions required: Kaggle credentials in `.env` (before T4 Step 5); apply `.wslconfig`; create the GitHub repository (before T5 can be confirmed green).

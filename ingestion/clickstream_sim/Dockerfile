FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim@sha256:e5b65587bce7de595f299855d7385fe7fca39b8a74baa261ba1b7147afa78e58

ENV UV_PYTHON_DOWNLOADS=never UV_COMPILE_BYTECODE=1 SIM_SCHEMAS_DIR=/app/ingestion/schemas/events

WORKDIR /app
COPY pyproject.toml uv.lock .python-version ./
COPY ingestion/clickstream_sim ingestion/clickstream_sim
COPY ingestion/replayer/pyproject.toml ingestion/replayer/pyproject.toml
COPY lakehouse/spark/pyproject.toml lakehouse/spark/pyproject.toml
COPY lakehouse/quality/pyproject.toml lakehouse/quality/pyproject.toml
COPY ingestion/schemas ingestion/schemas
RUN uv sync --locked --no-dev --package clickstream-sim

ENTRYPOINT ["uv", "run", "--no-sync", "--package", "clickstream-sim", "clickstream-sim"]

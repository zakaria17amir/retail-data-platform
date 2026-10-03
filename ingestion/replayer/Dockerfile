FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim@sha256:e5b65587bce7de595f299855d7385fe7fca39b8a74baa261ba1b7147afa78e58

ENV UV_PYTHON_DOWNLOADS=never UV_COMPILE_BYTECODE=1

WORKDIR /app
COPY pyproject.toml uv.lock .python-version ./
COPY ingestion/replayer ingestion/replayer
RUN uv sync --locked --no-dev --package replayer

ENTRYPOINT ["uv", "run", "--no-sync", "--package", "replayer", "replayer"]

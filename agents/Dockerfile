# Chainlit UI for the agents; build context is the repo root (agents/ plus its ../genai path dep).
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim@sha256:e5b65587bce7de595f299855d7385fe7fca39b8a74baa261ba1b7147afa78e58

ENV UV_PYTHON_DOWNLOADS=never UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy \
    PATH=/app/agents/.venv/bin:$PATH \
    GOLD_DIR=/app/data/gold DBT_PROJECT_DIR=/app/analytics CHART_DIR=/tmp/agents_charts

WORKDIR /app
COPY agents/pyproject.toml agents/uv.lock agents/
COPY genai/pyproject.toml genai/
COPY genai/src genai/src
RUN uv sync --locked --no-dev --no-install-project --project agents
COPY agents/src agents/src
RUN uv sync --locked --no-dev --project agents

# gold parquet and the dbt target/ (semantic_manifest.json, manifest.json) are mounted at runtime
RUN useradd --create-home --uid 1000 retail && chown retail /app
USER retail

EXPOSE 8010
CMD ["chainlit", "run", "agents/src/agents/ui/app.py", "-h", "--host", "0.0.0.0", "--port", "8010"]

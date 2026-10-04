FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim@sha256:e5b65587bce7de595f299855d7385fe7fca39b8a74baa261ba1b7147afa78e58

ENV UV_PYTHON_DOWNLOADS=never UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy PATH=/app/.venv/bin:$PATH

WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev --no-install-project
# README.m[d]: copied only if present (pyproject may reference it as the package readme)
COPY README.m[d] ./
COPY src src
COPY feature_repo feature_repo
COPY configs configs
RUN uv sync --locked --no-dev

RUN useradd --create-home --uid 1000 retail && chown retail /app
USER retail

ENTRYPOINT ["retail-ml"]

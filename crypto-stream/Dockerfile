# Pure-Python image with uv + Python 3.14 already baked in.
FROM ghcr.io/astral-sh/uv:python3.14-bookworm-slim

# uv tuning for containers: compile .pyc at build time, copy instead of hardlink.
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

WORKDIR /app

# 1) Install dependencies first (cached layer — only re-runs when the lock changes).
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project --no-dev

# 2) Install the project itself.
COPY src ./src
RUN uv sync --frozen --no-dev

# Put the venv's console scripts (incl. `crypto-stream`) on PATH.
ENV PATH="/app/.venv/bin:$PATH"

CMD ["crypto-stream"]

FROM python:3.12-slim

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

COPY pyproject.toml .
RUN uv sync --frozen --no-dev --no-install-project

COPY src/ src/
COPY alembic.ini .
COPY migrations/ migrations/

RUN uv sync --frozen --no-dev

CMD ["uv", "run", "alembic", "upgrade", "head"]

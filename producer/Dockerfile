FROM python:3.11-slim

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

COPY pyproject.toml .
RUN uv sync --no-dev --no-install-project

COPY src/ src/

RUN uv sync --no-dev

CMD ["uv", "run", "python", "-m", "src.main"]

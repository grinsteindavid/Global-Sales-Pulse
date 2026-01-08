FROM python:3.12-slim

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy
ENV PYTHONPATH=/app/etl_src:/app

COPY pyproject.toml .
RUN uv sync --frozen --no-install-project

COPY . .

CMD ["uv", "run", "pytest", "-v", "--tb=short"]

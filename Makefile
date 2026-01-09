.PHONY: help dev-up dev-down dev-logs prod-up prod-down prod-logs test test-unit test-int test-e2e db-migrate db-reset lint shell-etl shell-producer shell-grafana grafana-logs clean

DOCKER_COMPOSE = docker compose
DEV_COMPOSE = $(DOCKER_COMPOSE) -f docker-compose.yaml -f docker-compose.dev.yaml
PROD_COMPOSE = $(DOCKER_COMPOSE) -f docker-compose.yaml -f docker-compose.prod.yaml
TEST_COMPOSE = $(DOCKER_COMPOSE) -p sales-pulse-test -f docker-compose.test.yaml

help:
	@echo "Global Sales Pulse - Makefile Commands"
	@echo ""
	@echo "Development:"
	@echo "  make dev-up          Start development environment"
	@echo "  make dev-down        Stop development environment"
	@echo "  make dev-logs        View development logs"
	@echo "  make dev-build       Rebuild development containers"
	@echo ""
	@echo "Production:"
	@echo "  make prod-up         Start production environment"
	@echo "  make prod-down       Stop production environment"
	@echo "  make prod-logs       View production logs"
	@echo ""
	@echo "Testing:"
	@echo "  make test            Run all tests in container"
	@echo "  make test-unit       Run unit tests only"
	@echo "  make test-int        Run integration tests only"
	@echo "  make test-e2e        Run end-to-end tests only"
	@echo ""
	@echo "Database:"
	@echo "  make db-migrate      Run Alembic migrations"
	@echo "  make db-reset        Reset warehouse database"
	@echo ""
	@echo "Utilities:"
	@echo "  make lint            Run linter (ruff)"
	@echo "  make shell-etl       Shell into ETL container"
	@echo "  make shell-producer  Shell into producer container"
	@echo "  make clean           Remove all containers and volumes"

# Development
dev-up:
	$(DEV_COMPOSE) up -d
	@echo "Development environment started"
	@echo "Airflow UI: http://localhost:8080 (admin/admin)"
	@echo "Grafana UI: http://localhost:3000 (admin/admin)"
	@echo "Kafka UI:   http://localhost:8081"
	@echo "Postgres Warehouse: localhost:5433"

dev-down:
	$(DEV_COMPOSE) down

dev-logs:
	$(DEV_COMPOSE) logs -f

dev-build:
	$(DEV_COMPOSE) build --no-cache

dev-restart:
	$(DEV_COMPOSE) restart

# Production
prod-up:
	$(PROD_COMPOSE) up -d

prod-down:
	$(PROD_COMPOSE) down

prod-logs:
	$(PROD_COMPOSE) logs -f

prod-build:
	$(PROD_COMPOSE) build --no-cache

# Testing
test:
	$(TEST_COMPOSE) up --build --abort-on-container-exit test-runner
	$(TEST_COMPOSE) down -v

test-unit:
	$(TEST_COMPOSE) run --rm test-runner uv run pytest tests/unit -v --tb=short
	$(TEST_COMPOSE) down -v

test-int:
	$(TEST_COMPOSE) run --rm test-runner uv run pytest tests/integration -v --tb=short
	$(TEST_COMPOSE) down -v

test-e2e:
	$(TEST_COMPOSE) run --rm test-runner uv run pytest tests/e2e -v --tb=short
	$(TEST_COMPOSE) down -v

# Database
db-migrate:
	$(DEV_COMPOSE) exec airflow-scheduler uv run alembic upgrade head

db-reset:
	$(DEV_COMPOSE) exec postgres-warehouse psql -U warehouse -d warehouse -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"
	$(DEV_COMPOSE) restart db-init

db-shell:
	$(DEV_COMPOSE) exec postgres-warehouse psql -U warehouse -d warehouse

# Utilities
lint:
	$(DEV_COMPOSE) run --rm --no-deps airflow-scheduler uv run ruff check .

lint-fix:
	$(DEV_COMPOSE) run --rm --no-deps airflow-scheduler uv run ruff check --fix .

shell-etl:
	$(DEV_COMPOSE) exec airflow-scheduler /bin/bash

shell-producer:
	$(DEV_COMPOSE) exec producer /bin/bash

shell-kafka:
	$(DEV_COMPOSE) exec kafka /bin/bash

shell-grafana:
	$(DEV_COMPOSE) exec grafana /bin/sh

grafana-logs:
	$(DEV_COMPOSE) logs -f grafana

kafka-topics:
	$(DEV_COMPOSE) exec kafka kafka-topics --bootstrap-server localhost:9092 --list

kafka-consume:
	$(DEV_COMPOSE) exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic transactions --from-beginning --max-messages 10

# Cleanup
clean:
	$(DOCKER_COMPOSE) -f docker-compose.yaml -f docker-compose.dev.yaml down -v --remove-orphans
	$(TEST_COMPOSE) down -v --remove-orphans
	docker system prune -f

# Setup
setup:
	@if [ ! -f .env ]; then cp .env.example .env; echo ".env created from .env.example"; fi

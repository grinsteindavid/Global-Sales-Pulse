# Global Sales Pulse

Real-time financial transaction monitoring pipeline using **Kafka**, **Airflow**, and **PostgreSQL**.

## Overview

This project demonstrates a production-grade ETL pipeline that:
- Ingests high-volume transaction streams via Kafka
- Processes data in micro-batches using Airflow (30-second intervals)
- Loads transformed data into a PostgreSQL analytics warehouse
- Detects potential fraud and calculates tax in real-time

## Architecture

```
┌──────────────┐    ┌─────────┐    ┌─────────────────────────────────────┐    ┌────────────┐
│   Producer   │───▶│  Kafka  │───▶│         AIRFLOW DAG                 │───▶│  Postgres  │
│   (UV+Py)    │    │         │    │ Extract → Validate → Transform → Load│    │ (warehouse)│
└──────────────┘    └─────────┘    └─────────────────────────────────────┘    └────────────┘
```

See [ARCHITECTURE.md](docs/ARCHITECTURE.md) for detailed technical documentation.

## Tech Stack

| Component | Technology |
|-----------|------------|
| **Message Broker** | Apache Kafka + Zookeeper |
| **Orchestration** | Apache Airflow 2.x |
| **Database** | PostgreSQL 15 |
| **ORM** | SQLAlchemy 2.x + Alembic |
| **Package Manager** | UV (fast Python packaging) |
| **Validation** | Pydantic |
| **Testing** | Pytest (containerized) |
| **Infrastructure** | Docker Compose (dev/prod/test) |

## Quick Start

### Prerequisites
- Docker & Docker Compose
- Make

### Run in Development Mode

```bash
# Clone and start
git clone <repo-url>
cd Global-Sales-Pulse

# Copy environment template
cp .env.example .env

# Start all services
make dev-up

# View logs
make dev-logs
```

### Access Services

| Service | URL |
|---------|-----|
| Airflow UI | http://localhost:8080 (admin/admin) |
| Grafana UI | http://localhost:3000 (admin/admin) |
| Postgres Warehouse | localhost:5433 |

### Run Tests

```bash
make test          # All tests
make test-unit     # Unit tests only
make test-int      # Integration tests
```

## Project Structure

```
Global-Sales-Pulse/
├── docker-compose.yaml          # Base services
├── docker-compose.dev.yaml      # Dev overrides
├── docker-compose.prod.yaml     # Prod overrides
├── docker-compose.test.yaml     # Test runner
├── Makefile                     # All commands
├── .env.example                 # Environment template
│
├── producer/                    # Transaction simulator
│   ├── Dockerfile
│   ├── pyproject.toml
│   └── src/
│
├── etl/                         # Shared ETL code
│   ├── Dockerfile
│   ├── pyproject.toml
│   └── src/
│       ├── models/              # SQLAlchemy ORM
│       ├── consumers/           # Kafka consumer
│       └── transformations/     # Business logic
│
├── dags/                        # Airflow DAGs
│   └── etl_pipeline.py
│
├── migrations/                  # Alembic migrations
│
├── tests/                       # Containerized tests
│
├── grafana/                     # Grafana configuration
│   ├── provisioning/            # Auto-configured datasources
│   └── dashboards/              # Pre-built dashboards
│
└── docs/
    └── ARCHITECTURE.md
```

## Makefile Commands

```bash
# Development
make dev-up              # Start dev environment
make dev-down            # Stop dev environment
make dev-logs            # View logs

# Production
make prod-up             # Start prod environment
make prod-down           # Stop prod environment

# Testing
make test                # Run all tests in container
make test-unit           # Unit tests only
make test-int            # Integration tests

# Database
make db-migrate          # Run Alembic migrations
make db-reset            # Reset database

# Utilities
make lint                # Run linter (ruff)
make shell-etl           # Shell into ETL container
make shell-producer      # Shell into producer container
make grafana-logs        # View Grafana logs
```

## Key Concepts Demonstrated

### Kafka
- **Decoupling**: Producer and consumer operate independently
- **Backpressure**: If DB is slow, Kafka buffers messages
- **Offset Management**: Exactly-once semantics per batch

### Airflow
- **Idempotency**: Tasks can be retried without duplicating data
- **Observability**: Full DAG lineage and task history
- **Alerting**: Built-in failure notifications

### Corporate Standards
- **ORM Security**: SQLAlchemy prevents SQL injection
- **Environment Separation**: Dev/Prod/Test configs
- **Containerized Everything**: No local dependencies
- **Schema Migrations**: Version-controlled with Alembic

## License

MIT

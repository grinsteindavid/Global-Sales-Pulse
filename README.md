# Global Sales Pulse

Real-time financial transaction monitoring pipeline using **Kafka**, **Airflow**, and **PostgreSQL**.

<img width="1433" height="721" alt="image" src="https://github.com/user-attachments/assets/12f22d18-0ebb-4714-b881-707ba83d241c" />



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
| **Data Processing** | Pandas (vectorized operations) |
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
make test          # All tests (includes schema validation)
make test-unit     # Unit tests only
make test-int      # Integration tests
```

### Database Schema Validation

The system includes automatic schema validation that runs during database initialization:

- **🔍 Comprehensive Checks** - Tables, columns, indexes, constraints, integrity
- **🚫 Failure Prevention** - Blocks services if schema is invalid  
- **🔧 Generic Design** - Works with any database structure
- **📋 Detailed Logging** - Full validation report

```bash
# Manual validation
docker compose -f compose/docker-compose.test.yaml up postgres-test db-init-test

# Reset and re-validate
make db-reset
```

> **Note**: Services depend on successful schema validation. If validation fails, dependent services will not start to prevent data corruption.

## Project Structure

```
Global-Sales-Pulse/
├── compose/
│   ├── docker-compose.yaml          # Base services
│   ├── docker-compose.dev.yaml      # Dev overrides
│   ├── docker-compose.prod.yaml     # Prod overrides
│   └── docker-compose.test.yaml     # Test runner
├── Makefile                         # All commands
├── .env.example                     # Environment template
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

## Performance Metrics

### Live System Performance
- **Throughput**: ~20-25 records per batch
- **Processing Latency**: 30-second intervals
- **Fraud Detection**: 56% flag rate for high-value transactions
- **Batch Processing**: Vectorized pandas operations
- **Revenue Processing**: $43K+ per 2-batch cycle

### Test Performance
- **E2E Test Suite**: 14 tests in 113 seconds
- **Record Retrieval**: 22-45 seconds average
- **Success Rate**: 100% test pass rate

### Pandas Integration Benefits
- **Vectorized Operations**: 10-100x faster fraud detection for large batches
- **Memory Efficiency**: Optimized DataFrame operations
- **Fallback Safety**: Graceful degradation to sequential processing

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

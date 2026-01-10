# Architecture Documentation

## System Overview

Global Sales Pulse is a **micro-batch ETL pipeline** designed for real-time financial transaction monitoring. The system processes transaction streams with a 30-second latency SLA, providing fraud detection and analytics capabilities.

## Architecture Diagram

```
┌───────────────────────────────────────────────────────────────────────────────────────────────┐
│                                      DOCKER NETWORK                                            │
│                                                                                                │
│  ┌──────────────┐    ┌─────────────────┐    ┌─────────────────────────────────────────┐       │
│  │   Producer   │    │     Kafka       │    │              Airflow                     │       │
│  │              │    │   + Zookeeper   │    │  ┌───────────┐    ┌──────────────────┐  │       │
│  │ Generates    │───▶│                 │───▶│  │ Scheduler │───▶│   ETL DAG        │  │       │
│  │ transactions │    │ Topic:          │    │  └───────────┘    │                  │  │       │
│  │ every 1-2s   │    │ transactions    │    │                   │ 1. Extract       │  │       │
│  │              │    │                 │    │  ┌───────────┐    │ 2. Validate      │  │       │
│  └──────────────┘    │ Retention: 7d   │    │  │ Webserver │    │ 3. Transform     │  │       │
│                      │ Partitions: 3   │    │  │ :8080     │    │ 4. Load          │  │       │
│                      └─────────────────┘    │  └───────────┘    └────────┬─────────┘  │       │
│                                             └────────────────────────────┼────────────┘       │
│                                                                          │                    │
│                                                                          ▼                    │
│  ┌──────────────────────┐                              ┌─────────────────────────────┐       │
│  │  Postgres (Airflow)  │                              │   Postgres (Warehouse)      │       │
│  │  :5432               │                              │   :5433                     │       │
│  │                      │                              │                             │       │
│  │  - DAG metadata      │                              │   - transactions table      │◀──┐   │
│  │  - Task history      │                              │   - Indexed for analytics   │   │   │
│  │  - Connections       │                              │   - Partitioned by date     │   │   │
│  └──────────────────────┘                              └─────────────────────────────┘   │   │
│                                                                                          │   │
│                                                        ┌─────────────────────────────┐   │   │
│                                                        │        Grafana              │───┘   │
│                                                        │        :3000                │       │
│                                                        │                             │       │
│                                                        │  - Real-time dashboards     │       │
│                                                        │  - Transaction metrics      │       │
│                                                        │  - Fraud monitoring         │       │
│                                                        └─────────────────────────────┘       │
│                                                                                                │
└───────────────────────────────────────────────────────────────────────────────────────────────┘
```

## Component Details

### 1. Producer Service

**Purpose**: Simulates a high-volume e-commerce transaction stream.

**Technology**:
- Python 3.12 with UV package manager
- confluent-kafka for Kafka integration
- Pydantic for data validation

**Behavior**:
- Generates random transactions every 1-2 seconds
- Publishes to Kafka topic `transactions`
- Includes: order_id, amount, currency, timestamp, customer_id, product_category

**Message Schema**:
```json
{
  "order_id": "uuid-v4",
  "amount": 150.99,
  "currency": "USD",
  "timestamp": "2024-01-15T10:30:00Z",
  "customer_id": "cust_12345",
  "product_category": "electronics"
}
```

### 2. Kafka + Zookeeper

**Purpose**: Durable message buffer that decouples producer from consumer.

**Configuration**:
- Topic: `transactions`
- Partitions: 3 (allows parallel consumption)
- Retention: 7 days (enables replay)
- Replication: 1 (dev) / 3 (prod)

**Why Kafka**:
- **Durability**: Messages persist even if consumers are down
- **Backpressure**: If Airflow/Postgres is slow, no data loss
- **Replay**: Can reprocess from any offset for debugging

### 3. Airflow

**Purpose**: Orchestrates the ETL pipeline on a 30-second schedule.

**Components**:
- **Scheduler**: Triggers DAG runs
- **Webserver**: UI for monitoring and manual triggers
- **Metadata DB**: Postgres storing DAG state, task history

**DAG Structure**:
```
extract_from_kafka → validate_records → transform_data → load_to_warehouse
                                              ↓
                                    route_invalid_to_dlq
```

### 4. PostgreSQL Warehouse

**Purpose**: Analytics database for processed transactions.

**Schema**:
```sql
CREATE TABLE transactions (
    id SERIAL PRIMARY KEY,
    order_id UUID UNIQUE NOT NULL,
    amount DECIMAL(12, 2) NOT NULL,
    currency VARCHAR(3) NOT NULL,
    tax_amount DECIMAL(12, 2) NOT NULL,
    total_amount DECIMAL(12, 2) NOT NULL,
    is_fraud_flagged BOOLEAN DEFAULT FALSE,
    customer_id VARCHAR(50) NOT NULL,
    product_category VARCHAR(50) NOT NULL,
    transaction_timestamp TIMESTAMPTZ NOT NULL,
    processed_at TIMESTAMPTZ DEFAULT NOW(),
    batch_id VARCHAR(50) NOT NULL
);

CREATE INDEX idx_transactions_timestamp ON transactions(transaction_timestamp);
CREATE INDEX idx_transactions_fraud ON transactions(is_fraud_flagged);
CREATE INDEX idx_transactions_customer ON transactions(customer_id);
```

## Data Flow

### Extract Phase
1. Airflow task connects to Kafka consumer group `etl-consumer`
2. Polls all available messages (max 1000 per batch)
3. Deserializes JSON messages
4. Commits offset only after successful load

### Validate Phase
1. Pydantic validates each message against schema
2. Invalid records routed to dead-letter topic
3. Valid records passed to transform phase

### Transform Phase
1. Calculate tax (8.5% of amount)
2. Calculate total (amount + tax)
3. Flag fraud if amount > 1000
4. Normalize timestamps to UTC
5. Add batch_id for traceability

### Load Phase
1. SQLAlchemy ORM builds insert statements
2. Bulk insert with `insert().on_conflict_do_nothing()`
3. Ensures idempotency on order_id

## Environment Strategy

### Development (`compose/docker-compose.dev.yaml`)
- Hot reload for DAGs
- Debug logging
- Single replica services
- Exposed ports for local debugging

### Production (`compose/docker-compose.prod.yaml`)
- Resource limits
- Health checks with restart policies
- Multiple Airflow workers
- No exposed ports (reverse proxy only)

### Testing (`compose/docker-compose.test.yaml`)
- Isolated Postgres instance
- Kafka with auto-create topics
- Pytest runner container
- Tears down after tests complete

## Error Handling

### Kafka Consumer Failures
- Offsets not committed until load succeeds
- Retry from last committed offset
- Max retries: 3 with exponential backoff

### Transform Failures
- Invalid records → dead-letter topic
- Partial batch success: valid records proceed
- Full batch failure: retry entire batch

### Database Failures
- SQLAlchemy handles connection pooling
- Transaction rollback on failure
- Alembic migrations are idempotent

## Database Schema Validation

### Overview
The system includes a comprehensive schema validation framework that ensures database integrity before allowing services to start. This prevents deployment of broken schemas and guarantees data consistency.

### Validation Pipeline
```bash
alembic upgrade head && python scripts/validate_schema.py
```

### Validation Checks

#### 1. Database Connectivity
- Verifies database connection is established
- Tests basic query execution capability

#### 2. Schema Completeness
- **Dynamic table discovery** - Automatically finds all tables
- Validates each table has columns (no empty tables)
- **Future-proof** - Works with any new tables added later

#### 3. Table Structure
- **Primary key validation** - Every table must have a PK
- **Foreign key detection** - Reports relationships found
- **Column integrity** - Validates column names and types
- **Smart warnings** - Flags potential issues (e.g., nullable *_id columns)

#### 4. Performance Indexes
- **Index discovery** - Finds all indexes across all tables
- **Column mapping validation** - Ensures indexes reference valid columns
- **Performance optimization** - Confirms query optimization indexes exist

#### 5. Constraints
- **Primary key constraints** - Validates PK enforcement
- **Unique constraints** - Checks uniqueness rules
- **NOT NULL constraints** - Validates required field enforcement
- **Constraint counting** - Reports total constraints per table

#### 6. Referential Integrity
- **Table accessibility** - Ensures all tables are queryable
- **Data type validation** - Tests prepared statement creation
- **Zero data insertion** - Validates without polluting database
- **Row count reporting** - Shows current table sizes

### Failure Prevention

#### Exit Code Behavior
- **Exit 0** ✅ = Success → Dependent services start
- **Exit 1** ❌ = Failure → Dependent services blocked

#### Docker Compose Integration
```yaml
db-init-test:
  command: >
    sh -c "
      alembic upgrade head &&
      python scripts/validate_schema.py
    "
  # Exits with code 0 on success, 1 on failure

airflow-scheduler-test:
  depends_on:
    db-init-test:
      condition: service_completed_successfully  # Requires exit 0
```

### Generic Design Benefits

#### Future-Proof Architecture
- **Zero maintenance** - No hardcoded table expectations
- **Automatic adaptation** - Works with any schema evolution
- **Universal compatibility** - Validates any database structure

#### Production Safety
- **Gatekeeper pattern** - Blocks services on schema failure
- **Data integrity guarantee** - No partial initialization
- **Zero corruption risk** - Validation without data insertion

#### Corporate Standards Compliance
- **Comprehensive coverage** - Structure, constraints, performance, integrity
- **Audit trail** - Detailed validation logging
- **Failure documentation** - Clear error reporting

### Usage Examples

#### Development Environment
```bash
make dev-up      # Runs full validation pipeline
make db-reset    # Restarts validation after schema reset
```

#### Test Environment
```bash
make test        # Validates schema before running tests
make test-unit   # Uses validated schema for unit tests
```

#### Production Deployment
- Schema validation runs automatically during deployment
- Services blocked until validation passes
- Rollback triggered on validation failure

### Validation Output Example
```
🚀 Starting database schema validation...

--- Database Connectivity ---
✅ Database connectivity verified

--- Schema Completeness ---
✅ Found 2 table(s): ['alembic_version', 'transactions']
✅ Table 'alembic_version' has 1 column(s)
✅ Table 'transactions' has 12 column(s)

--- Table Structure ---
✅ All table structures validated

--- Performance Indexes ---
✅ Total indexes across all tables: 5

--- Constraints ---
✅ Summary: 2 PK, 1 unique, 13 NOT NULL constraints

--- Referential Integrity ---
✅ All tables referential integrity validated

🎉 Database schema validation PASSED!
✅ Database is ready for use
```

### File Structure
```
etl/
├── scripts/
│   └── validate_schema.py    # Generic validation script
├── migrations/
│   └── versions/             # Alembic migration files
└── Dockerfile                # Includes scripts/ in container
```

## Monitoring & Observability

### Airflow UI (http://localhost:8080)
- DAG run history
- Task duration metrics
- Failure alerts (email/Slack configurable)

### Grafana UI (http://localhost:3000)
- **Real-time dashboards** with 30-second auto-refresh
- Pre-built "Sales Pulse Dashboard" includes:
  - Transaction volume (24h count, per-minute time series)
  - Revenue metrics (total, by category pie chart)
  - Fraud rate monitoring with threshold alerts
  - Tax collection totals
  - Recent transactions table
  - Flagged fraud transactions table
- Auto-provisioned PostgreSQL Warehouse datasource
- Customizable dashboards via JSON

### Logging
- Structured JSON logs
- Correlation IDs per batch
- Log levels: DEBUG (dev), INFO (prod)

### Future Enhancements
- Prometheus metrics endpoint
- Kafka lag monitoring
- Grafana alerting rules

## Security Considerations

### SQL Injection Prevention
- SQLAlchemy ORM (no raw SQL)
- Parameterized queries only

### Secrets Management
- Environment variables via `.env`
- No secrets in code or Docker images
- Airflow connections stored encrypted

### Network Isolation
- Internal Docker network
- Only Airflow webserver exposed
- Postgres not accessible externally in prod

## Scaling Considerations

### Horizontal Scaling
- **Kafka**: Add partitions + consumers
- **Airflow**: Add workers (CeleryExecutor)
- **Postgres**: Read replicas for analytics

### Vertical Scaling
- Increase batch size (currently 1000)
- Reduce DAG interval (currently 30s)
- Add database connection pool size

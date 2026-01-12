"""
ETL Pipeline DAG

Consumes transactions from Kafka, transforms them, and loads to Postgres warehouse.
Schedule interval is configurable via ETL_SCHEDULE_SECONDS env var (default: 30s).
"""
import logging
import os
import uuid
from datetime import datetime, timedelta

ETL_SCHEDULE_SECONDS = int(os.environ.get("ETL_SCHEDULE_SECONDS", "30"))

from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
}


def extract_from_kafka(**context) -> list[dict]:
    from src.consumers import KafkaTransactionConsumer

    consumer = KafkaTransactionConsumer()
    try:
        messages = consumer.consume_batch(max_messages=1000, timeout=5.0)
        context["ti"].xcom_push(key="raw_messages", value=messages)
        context["ti"].xcom_push(key="message_count", value=len(messages))
        logger.info(f"Extracted {len(messages)} messages from Kafka")
        
        # Commit offsets BEFORE closing consumer
        consumer.commit()
        return messages
    except Exception as e:
        logger.error(f"Failed to extract from Kafka: {e}")
        raise
    finally:
        consumer.close()


def transform_data(**context) -> list[dict]:
    from src.transformations import TransactionTransformer

    raw_messages = context["ti"].xcom_pull(key="raw_messages", task_ids="extract")

    if not raw_messages:
        logger.info("No messages to transform")
        context["ti"].xcom_push(key="transformed", value=[])
        context["ti"].xcom_push(key="failed", value=[])
        return []

    batch_id = f"batch_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    transformer = TransactionTransformer(batch_id=batch_id)

    transformed, failed = transformer.transform_batch(raw_messages)

    transformed_dicts = [t.model_dump(mode="json") for t in transformed]
    context["ti"].xcom_push(key="transformed", value=transformed_dicts)
    context["ti"].xcom_push(key="failed", value=failed)
    context["ti"].xcom_push(key="batch_id", value=batch_id)

    logger.info(f"Transformed {len(transformed)} messages, {len(failed)} failed")
    return transformed_dicts


def load_to_warehouse(**context) -> int:
    from src.database import SessionLocal
    from src.loaders import PostgresLoader
    from src.transformations.transformer import TransformedTransaction

    transformed_dicts = context["ti"].xcom_pull(key="transformed", task_ids="transform")

    if not transformed_dicts:
        logger.info("No data to load")
        return 0

    transformed = [TransformedTransaction(**t) for t in transformed_dicts]

    session = SessionLocal()
    try:
        loader = PostgresLoader(session)
        loaded_count = loader.load_batch(transformed)
        logger.info(f"Loaded {loaded_count} records to warehouse")
        return loaded_count
    except Exception as e:
        session.rollback()
        logger.error(f"Failed to load to warehouse: {e}")
        raise
    finally:
        session.close()




with DAG(
    dag_id="etl_pipeline",
    default_args=default_args,
    description="ETL pipeline: Kafka -> Transform -> Postgres",
    schedule_interval=timedelta(seconds=ETL_SCHEDULE_SECONDS),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["etl", "kafka", "postgres"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract_from_kafka,
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform_data,
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=load_to_warehouse,
    )

    extract_task >> transform_task >> load_task

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    warehouse_database_url: str = (
        "postgresql+psycopg2://warehouse:warehouse@postgres-warehouse:5432/warehouse"
    )
    kafka_bootstrap_servers: str = "kafka:9092"
    kafka_topic_transactions: str = "transactions"
    kafka_consumer_group: str = "etl-consumer"
    log_level: str = "INFO"

    class Config:
        env_file = ".env"


settings = Settings()

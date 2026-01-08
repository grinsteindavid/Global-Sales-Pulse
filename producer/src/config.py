from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    kafka_bootstrap_servers: str = "kafka:9092"
    kafka_topic_transactions: str = "transactions"
    producer_interval_min: float = 1.0
    producer_interval_max: float = 2.0
    log_level: str = "INFO"

    class Config:
        env_file = ".env"


settings = Settings()

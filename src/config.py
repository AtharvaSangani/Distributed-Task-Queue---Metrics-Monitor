"""Application configuration settings."""

from typing import Optional
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Central configuration for Redis, API, and Worker."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

    # Redis Connection Settings
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0
    REDIS_PASSWORD: Optional[str] = None
    REDIS_URL: Optional[str] = None

    # Queue Names
    TASK_QUEUE: str = "task_queue"
    DEAD_LETTER_QUEUE: str = "dead_letter_queue"

    # Worker Settings
    WORKER_POP_TIMEOUT: int = 5
    MAX_RETRIES: int = 3
    BASE_BACKOFF_SECONDS: float = 2.0
    TASK_TTL_SECONDS: int = 3600  # 1 hour TTL for completed tasks

    # Metric Keys
    METRIC_COMPLETED_TASKS: str = "metrics:completed_tasks"
    METRIC_FAILED_TASKS: str = "metrics:failed_tasks"
    METRIC_RETRIED_TASKS: str = "metrics:retried_tasks"

    # API Settings
    API_HOST: str = "0.0.0.0"
    API_PORT: int = 8000

    @property
    def redis_connection_url(self) -> str:
        """Construct Redis connection URL if not provided directly."""
        if self.REDIS_URL:
            return self.REDIS_URL
        if self.REDIS_PASSWORD:
            return f"redis://:{self.REDIS_PASSWORD}@{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"
        return f"redis://{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"


settings = Settings()

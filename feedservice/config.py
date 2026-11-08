"""Runtime configuration, read once from environment variables."""

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass
class Settings:
    database_url: str = os.getenv(
        "DATABASE_URL", "postgresql+psycopg://feeds:feeds@localhost:5432/feeds"
    )
    # "stub" is used by the tests, anything else is treated as an AMQP URL.
    broker_url: str = os.getenv("BROKER_URL", "amqp://guest:guest@localhost:5672/")
    data_dir: Path = Path(os.getenv("DATA_DIR", "./data"))

    max_feed_bytes: int = int(os.getenv("MAX_FEED_BYTES", str(100 * 1024 * 1024)))
    max_image_bytes: int = int(os.getenv("MAX_IMAGE_BYTES", str(10 * 1024 * 1024)))
    image_timeout_s: float = float(os.getenv("IMAGE_TIMEOUT_S", "10"))

    max_retries: int = int(os.getenv("TASK_MAX_RETRIES", "5"))
    min_backoff_ms: int = int(os.getenv("TASK_MIN_BACKOFF_MS", "1000"))
    max_backoff_ms: int = int(os.getenv("TASK_MAX_BACKOFF_MS", "300000"))

    @property
    def feeds_dir(self) -> Path:
        return self.data_dir / "feeds"

    @property
    def images_dir(self) -> Path:
        return self.data_dir / "images"


settings = Settings()

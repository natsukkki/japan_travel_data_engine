"""Runtime configuration shared by ingestion jobs."""

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[1]
LOCAL_ENV_PATH = PROJECT_ROOT / "infrastructure" / "env" / ".env"


@dataclass(frozen=True)
class Settings:
    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    raw_bucket: str


def get_required_env(name: str) -> str:
    """Return a required environment variable or fail fast."""

    value = os.getenv(name)
    if value is None or not value.strip():
        raise RuntimeError(
            f"Не задана обязательная переменная окружения: {name}"
        )
    return value.strip()


def load_settings(env_path: Path = LOCAL_ENV_PATH) -> Settings:
    """Load and validate runtime settings.

    Docker/Airflow variables already present in the environment take priority
    because ``override`` is disabled.
    """

    load_dotenv(env_path, override=False)

    return Settings(
        minio_endpoint=get_required_env("MINIO_ENDPOINT"),
        minio_access_key=get_required_env("MINIO_ROOT_USER"),
        minio_secret_key=get_required_env("MINIO_ROOT_PASSWORD"),
        raw_bucket=get_required_env("MINIO_RAW_BUCKET"),
    )

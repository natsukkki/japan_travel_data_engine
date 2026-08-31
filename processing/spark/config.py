"""Runtime configuration for Spark processing jobs."""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class SparkSettings:
    """Connections and credentials required by Spark jobs."""

    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    raw_bucket: str
    postgres_jdbc_url: str
    postgres_user: str
    postgres_password: str


def get_required_env(name: str) -> str:
    """Return a required environment variable or fail fast."""

    value = os.getenv(name)
    if value is None or not value.strip():
        raise RuntimeError(
            f"Не задана обязательная переменная окружения: {name}"
        )

    return value.strip()


def load_spark_settings() -> SparkSettings:
    """Load and validate settings passed to the Spark container."""

    return SparkSettings(
        minio_endpoint=get_required_env("MINIO_SPARK_ENDPOINT"),
        minio_access_key=get_required_env("MINIO_ROOT_USER"),
        minio_secret_key=get_required_env("MINIO_ROOT_PASSWORD"),
        raw_bucket=get_required_env("MINIO_RAW_BUCKET"),
        postgres_jdbc_url=get_required_env("POSTGRES_JDBC_URL"),
        postgres_user=get_required_env("POSTGRES_USER"),
        postgres_password=get_required_env("POSTGRES_PASSWORD"),
    )

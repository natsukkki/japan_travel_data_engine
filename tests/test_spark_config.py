"""Tests for Spark runtime configuration."""

import os
import unittest
from unittest.mock import patch

from processing.spark.config import get_required_env, load_spark_settings


class SparkConfigTests(unittest.TestCase):
    def test_get_required_env_strips_whitespace(self) -> None:
        with patch.dict(os.environ, {"REQUIRED_SETTING": "  value  "}):
            self.assertEqual(get_required_env("REQUIRED_SETTING"), "value")

    def test_get_required_env_rejects_blank_value(self) -> None:
        with patch.dict(os.environ, {"REQUIRED_SETTING": "   "}):
            with self.assertRaisesRegex(
                RuntimeError,
                "REQUIRED_SETTING",
            ):
                get_required_env("REQUIRED_SETTING")

    def test_load_spark_settings_maps_environment_variables(self) -> None:
        environment = {
            "MINIO_SPARK_ENDPOINT": "http://minio:9000",
            "MINIO_ROOT_USER": "minio-user",
            "MINIO_ROOT_PASSWORD": "minio-password",
            "MINIO_RAW_BUCKET": "raw-batch",
            "POSTGRES_JDBC_URL": (
                "jdbc:postgresql://postgres:5432/japan_travel"
            ),
            "POSTGRES_USER": "postgres-user",
            "POSTGRES_PASSWORD": "postgres-password",
        }

        with patch.dict(os.environ, environment, clear=True):
            settings = load_spark_settings()

        self.assertEqual(settings.minio_endpoint, "http://minio:9000")
        self.assertEqual(settings.minio_access_key, "minio-user")
        self.assertEqual(settings.minio_secret_key, "minio-password")
        self.assertEqual(settings.raw_bucket, "raw-batch")
        self.assertEqual(
            settings.postgres_jdbc_url,
            "jdbc:postgresql://postgres:5432/japan_travel",
        )
        self.assertEqual(settings.postgres_user, "postgres-user")
        self.assertEqual(settings.postgres_password, "postgres-password")


if __name__ == "__main__":
    unittest.main()

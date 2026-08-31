"""Tests for the PostgreSQL JDBC smoke-test reader."""

import unittest
from unittest.mock import MagicMock, call

from processing.spark.config import SparkSettings
from processing.spark.jdbc import read_jdbc


class PostgresSmokeTests(unittest.TestCase):
    def test_read_jdbc_configures_postgres_reader(self) -> None:
        spark = MagicMock()
        reader = spark.read.format.return_value
        expected_df = reader.option.return_value.load.return_value
        reader.option.return_value = reader
        reader.load.return_value = expected_df
        settings = SparkSettings(
            minio_endpoint="http://minio:9000",
            minio_access_key="minio-user",
            minio_secret_key="minio-password",
            raw_bucket="raw-batch",
            postgres_jdbc_url=(
                "jdbc:postgresql://postgres:5432/japan_travel"
            ),
            postgres_user="postgres-user",
            postgres_password="postgres-password",
        )

        result = read_jdbc(
            spark=spark,
            settings=settings,
            dbtable="dds_schema.dds_regions",
        )

        spark.read.format.assert_called_once_with("jdbc")
        reader.option.assert_has_calls(
            [
                call("url", settings.postgres_jdbc_url),
                call("dbtable", "dds_schema.dds_regions"),
                call("user", settings.postgres_user),
                call("password", settings.postgres_password),
                call("driver", "org.postgresql.Driver"),
            ]
        )
        reader.load.assert_called_once_with()
        self.assertIs(result, expected_df)


if __name__ == "__main__":
    unittest.main()

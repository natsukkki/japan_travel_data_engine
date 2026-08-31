"""Tests for shared Spark JDBC operations."""

import unittest
from unittest.mock import MagicMock, call

from processing.spark.config import SparkSettings
from processing.spark.jdbc import JDBC_DRIVER, write_jdbc


class JdbcWriterTests(unittest.TestCase):
    def test_write_jdbc_configures_postgres_writer(self) -> None:
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
        df = MagicMock()
        writer = df.write
        writer.format.return_value = writer
        writer.option.return_value = writer
        writer.mode.return_value = writer

        write_jdbc(
            df=df,
            settings=settings,
            dbtable="dds_schema.dds_regions",
        )

        writer.format.assert_called_once_with("jdbc")
        writer.option.assert_has_calls(
            [
                call("url", settings.postgres_jdbc_url),
                call("dbtable", "dds_schema.dds_regions"),
                call("user", settings.postgres_user),
                call("password", settings.postgres_password),
                call("driver", JDBC_DRIVER),
            ]
        )
        writer.mode.assert_called_once_with("append")
        writer.save.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()

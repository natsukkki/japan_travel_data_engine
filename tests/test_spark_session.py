"""Tests for shared Spark session configuration."""

import unittest
from unittest.mock import MagicMock, call, patch

from processing.spark.config import SparkSettings
from processing.spark.session import configure_s3a, create_spark_session


class SparkSessionConfigTests(unittest.TestCase):
    def setUp(self) -> None:
        self.settings = SparkSettings(
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

    def test_configure_s3a_sets_minio_hadoop_properties(self) -> None:
        spark = MagicMock()
        hadoop_config = spark.sparkContext._jsc.hadoopConfiguration.return_value

        configure_s3a(spark, self.settings)

        hadoop_config.set.assert_has_calls(
            [
                call(
                    "fs.s3a.impl",
                    "org.apache.hadoop.fs.s3a.S3AFileSystem",
                ),
                call("fs.s3a.endpoint", "http://minio:9000"),
                call("fs.s3a.endpoint.region", "us-east-1"),
                call("fs.s3a.path.style.access", "true"),
                call("fs.s3a.connection.ssl.enabled", "false"),
                call(
                    "fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
                ),
                call("fs.s3a.access.key", "minio-user"),
                call("fs.s3a.secret.key", "minio-password"),
            ]
        )
        self.assertEqual(hadoop_config.set.call_count, 8)

    @patch("processing.spark.session.configure_s3a")
    @patch("processing.spark.session.SparkSession")
    def test_create_spark_session_configures_created_session(
        self,
        spark_session_class: MagicMock,
        configure_s3a_mock: MagicMock,
    ) -> None:
        builder_with_name = (
            spark_session_class.builder.appName.return_value
        )
        spark = builder_with_name.getOrCreate.return_value

        result = create_spark_session("weather-etl", self.settings)

        spark_session_class.builder.appName.assert_called_once_with(
            "weather-etl"
        )
        builder_with_name.getOrCreate.assert_called_once_with()
        configure_s3a_mock.assert_called_once_with(
            spark=spark,
            settings=self.settings,
        )
        self.assertIs(result, spark)


if __name__ == "__main__":
    unittest.main()

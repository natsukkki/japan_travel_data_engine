"""Tests for shared Spark JDBC operations."""

import unittest
from unittest.mock import MagicMock, call, patch

from processing.spark.config import SparkSettings
from processing.spark.jdbc import (
    JDBC_DRIVER,
    execute_postgres_transaction,
    truncate_postgres_table,
    write_jdbc,
)


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


class PostgresTransactionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.settings = SparkSettings(
            minio_endpoint="http://minio:9000",
            minio_access_key="minio-user",
            minio_secret_key="minio-password",
            raw_bucket="raw-batch",
            postgres_jdbc_url="jdbc:postgresql://postgres:5432/japan_travel",
            postgres_user="postgres-user",
            postgres_password="postgres-password",
        )

    @patch("processing.spark.jdbc.psycopg.connect")
    def test_statements_share_one_connection_in_order(self, connect: MagicMock) -> None:
        connection = connect.return_value.__enter__.return_value
        cursor = connection.cursor.return_value.__enter__.return_value
        statements = ("INSERT INTO weather", "INSERT INTO log", "TRUNCATE staging")

        execute_postgres_transaction(self.settings, statements)

        connect.assert_called_once_with(
            "postgresql://postgres:5432/japan_travel",
            user=self.settings.postgres_user,
            password=self.settings.postgres_password,
        )
        cursor.execute.assert_has_calls([call(sql) for sql in statements])
        self.assertEqual(cursor.execute.call_count, 3)
        connect.return_value.__exit__.assert_called_once_with(None, None, None)

    @patch("processing.spark.jdbc.psycopg.connect")
    def test_sql_error_propagates_and_aborts_remaining_statements(
        self, connect: MagicMock
    ) -> None:
        connection = connect.return_value.__enter__.return_value
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.execute.side_effect = [None, RuntimeError("journal insert failed")]

        with self.assertRaisesRegex(RuntimeError, "journal insert failed"):
            execute_postgres_transaction(
                self.settings,
                ("INSERT INTO weather", "INSERT INTO log", "TRUNCATE staging"),
            )

        self.assertEqual(cursor.execute.call_count, 2)
        exit_args = connect.return_value.__exit__.call_args.args
        self.assertIs(exit_args[0], RuntimeError)
        self.assertIn("journal insert failed", str(exit_args[1]))

    @patch("processing.spark.jdbc.psycopg.connect")
    def test_truncate_quotes_schema_and_table_as_identifiers(
        self, connect: MagicMock
    ) -> None:
        cursor = connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value

        truncate_postgres_table(self.settings, "dds_schema.dds_staging_weather_daily")

        query = cursor.execute.call_args.args[0]
        self.assertEqual(
            query.as_string(),
            'TRUNCATE TABLE "dds_schema"."dds_staging_weather_daily"',
        )


if __name__ == "__main__":
    unittest.main()

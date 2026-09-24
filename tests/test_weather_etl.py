"""Behavioral tests for weather file selection, validation, and loading."""

import json
import re
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from processing.spark.config import SparkSettings
from processing.spark import weather_etl as weather


def make_settings() -> SparkSettings:
    return SparkSettings(
        minio_endpoint="http://minio:9000",
        minio_access_key="minio-user",
        minio_secret_key="minio-password",
        raw_bucket="raw-batch",
        postgres_jdbc_url="jdbc:postgresql://postgres:5432/japan_travel",
        postgres_user="postgres-user",
        postgres_password="postgres-password",
    )


def supports_local_spark() -> bool:
    """Spark 4 needs Java 17+; skip local Spark tests on older hosts."""
    try:
        result = subprocess.run(
            ["java", "-version"], capture_output=True, text=True, timeout=5
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    match = re.search(r'version "(\d+)', result.stderr + result.stdout)
    return bool(match and int(match.group(1)) >= 17)


def weather_json(dates: list[str], **overrides: object) -> dict:
    daily = {
        "time": dates,
        "temperature_2m_mean": [12.5] * len(dates),
        "precipitation_sum": [1.2] * len(dates),
        "relative_humidity_2m_mean": [70.0] * len(dates),
        "wind_speed_10m_mean": [3.5] * len(dates),
        "sunshine_duration": [3600.0] * len(dates),
    }
    daily.update(overrides)
    return {"daily": daily}


class WeatherWorkflowTests(unittest.TestCase):
    @patch.object(weather, "read_raw_weather")
    @patch.object(weather, "find_unprocessed_weather_keys", return_value=[])
    @patch.object(weather, "read_jdbc")
    @patch.object(weather, "read_raw_weather_keys")
    @patch.object(weather, "create_spark_session")
    def test_second_run_with_all_keys_logged_skips_every_write(
        self, create_session, read_keys, read_jdbc, find_keys, read_raw
    ) -> None:
        settings = make_settings()
        with patch.object(weather, "truncate_postgres_table") as truncate, \
             patch.object(weather, "write_jdbc") as write, \
             patch.object(weather, "execute_postgres_transaction") as transaction:
            result = weather.run_weather_etl(settings)

        self.assertEqual(result, {"processed_files": 0, "weather_rows": 0})
        read_keys.assert_called_once_with(
            spark=create_session.return_value, bucket=settings.raw_bucket
        )
        read_jdbc.assert_called_once_with(
            spark=create_session.return_value,
            settings=settings,
            dbtable=weather.WEATHER_PROCESSED_FILES,
        )
        find_keys.assert_called_once_with(
            minio_keys_df=read_keys.return_value,
            processed_files_df=read_jdbc.return_value.select.return_value,
        )
        read_raw.assert_not_called()
        truncate.assert_not_called()
        write.assert_not_called()
        transaction.assert_not_called()
        create_session.return_value.stop.assert_called_once_with()

    @patch.object(weather, "find_unprocessed_weather_keys")
    @patch.object(weather, "read_jdbc")
    @patch.object(weather, "read_raw_weather_keys")
    @patch.object(weather, "create_spark_session")
    def test_failed_validation_does_not_touch_staging_or_journal(
        self, create_session, read_keys, read_jdbc, find_keys
    ) -> None:
        find_keys.return_value = ["weather/JP-TOKYO/2024/01/2024-01-01.json"]
        with patch.object(weather, "read_raw_weather") as read_raw, \
             patch.object(weather, "validate_raw_weather", side_effect=ValueError("bad raw")), \
             patch.object(weather, "truncate_postgres_table") as truncate, \
             patch.object(weather, "write_jdbc") as write, \
             patch.object(weather, "execute_postgres_transaction") as transaction:
            with self.assertRaisesRegex(ValueError, "bad raw"):
                weather.run_weather_etl(make_settings())

        read_raw.assert_called_once()
        truncate.assert_not_called()
        write.assert_not_called()
        transaction.assert_not_called()
        create_session.return_value.stop.assert_called_once_with()

    @patch.object(weather, "create_spark_session")
    def test_success_loads_staging_then_executes_one_dds_transaction(
        self, create_session
    ) -> None:
        settings = make_settings()
        keys = ["weather/JP-TOKYO/2024/01/2024-01-01.json"]
        events = []
        joined = MagicMock()
        joined.count.return_value = 3
        with patch.object(weather, "read_raw_weather_keys"), \
             patch.object(weather, "read_jdbc"), \
             patch.object(weather, "find_unprocessed_weather_keys", return_value=keys), \
             patch.object(weather, "read_raw_weather"), \
             patch.object(weather, "validate_raw_weather"), \
             patch.object(weather, "flatten_daily_weather"), \
             patch.object(weather, "validate_weather_df"), \
             patch.object(weather, "cast_weather_types"), \
             patch.object(weather, "validate_required_weather_fields"), \
             patch.object(weather, "validate_unique_weather_dates"), \
             patch.object(weather, "attach_region_ids", return_value=joined), \
             patch.object(weather, "truncate_postgres_table", side_effect=lambda **_: events.append("truncate")), \
             patch.object(weather, "write_jdbc", side_effect=lambda **_: events.append("write")), \
             patch.object(weather, "validate_staging_row_count", side_effect=lambda **_: events.append("check_rows")), \
             patch.object(weather, "validate_staging_object_keys", side_effect=lambda **_: events.append("check_keys")), \
             patch.object(weather, "execute_postgres_transaction", side_effect=lambda **_: events.append("transaction")) as transaction:
            result = weather.run_weather_etl(settings)

        self.assertEqual(result, {"processed_files": 1, "weather_rows": 3})
        self.assertEqual(events, ["truncate", "write", "check_rows", "check_keys", "transaction"])
        statements = transaction.call_args.kwargs["statements"]
        self.assertEqual(len(statements), 3)
        self.assertIn("INSERT INTO dds_schema.dds_weather_daily", statements[0])
        self.assertIn("FROM dds_schema.dds_staging_weather_daily", statements[0])
        self.assertIn("precipitation_sum", statements[0])
        self.assertIn("INSERT INTO dds_schema.dds_weather_processed_files", statements[1])
        self.assertRegex(statements[1], r"SELECT\s+DISTINCT\s+object_key")
        self.assertIn("TRUNCATE TABLE dds_schema.dds_staging_weather_daily", statements[2])
        create_session.return_value.stop.assert_called_once_with()

    @patch.object(weather, "create_spark_session")
    def test_failed_staging_verification_does_not_mark_file_processed(
        self, create_session
    ) -> None:
        joined = MagicMock()
        joined.count.return_value = 2
        with patch.object(weather, "read_raw_weather_keys"), \
             patch.object(weather, "read_jdbc"), \
             patch.object(weather, "find_unprocessed_weather_keys", return_value=["weather/JP-TOKYO/2024/01/2024-01-01.json"]), \
             patch.object(weather, "read_raw_weather"), \
             patch.object(weather, "validate_raw_weather"), \
             patch.object(weather, "flatten_daily_weather"), \
             patch.object(weather, "validate_weather_df"), \
             patch.object(weather, "cast_weather_types"), \
             patch.object(weather, "validate_required_weather_fields"), \
             patch.object(weather, "validate_unique_weather_dates"), \
             patch.object(weather, "attach_region_ids", return_value=joined), \
             patch.object(weather, "truncate_postgres_table"), \
             patch.object(weather, "write_jdbc"), \
             patch.object(weather, "validate_staging_row_count", side_effect=ValueError("one row missing")), \
             patch.object(weather, "execute_postgres_transaction") as transaction:
            with self.assertRaisesRegex(ValueError, "one row missing"):
                weather.run_weather_etl(make_settings())

        transaction.assert_not_called()
        create_session.return_value.stop.assert_called_once_with()


@unittest.skipUnless(supports_local_spark(), "Spark 4 requires Java 17+")
class WeatherSparkDataTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("weather-etl-tests")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def write_raw(self, region: str, filename: str, data: dict) -> tuple[str, str]:
        key = f"weather/{region}/2024/01/{filename}"
        path = Path(self.tempdir.name) / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data), encoding="utf-8")
        return key, str(path)

    def read_raw(self, region="JP-TOKYO", filename="2024-01-01.json", data=None):
        key, path = self.write_raw(
            region, filename, data or weather_json(["2024-01-01"])
        )
        return key, weather.read_raw_weather(self.spark, [path])

    def test_old_and_new_file_names_expand_to_unique_region_dates(self) -> None:
        old_key, old_path = self.write_raw(
            "JP-TOKYO", "2024-01-01-2024-01-02.json",
            weather_json(["2024-01-01", "2024-01-02"]),
        )
        new_key, new_path = self.write_raw(
            "JP-TOKYO", "2024-01-03.json",
            weather_json(["2024-01-03"]),
        )
        raw = weather.read_raw_weather(self.spark, [old_path, new_path])
        weather.validate_raw_weather(raw)
        daily = weather.flatten_daily_weather(raw)
        weather.validate_weather_df(daily)
        typed = weather.cast_weather_types(daily)
        weather.validate_required_weather_fields(typed)
        weather.validate_unique_weather_dates(typed)
        regions = self.spark.createDataFrame([(1, "JP-TOKYO")], ["region_id", "region_code"])
        joined = weather.attach_region_ids(typed, regions)
        actual = {(row["object_key"], str(row["weather_date"])) for row in joined.collect()}
        self.assertEqual(actual, {
            (old_key, "2024-01-01"), (old_key, "2024-01-02"),
            (new_key, "2024-01-03"),
        })
        weather.validate_staging_row_count(joined, expected_rows=3)
        weather.validate_staging_object_keys(joined, expected_keys=[old_key, new_key])

    def test_left_anti_selects_only_unlogged_files(self) -> None:
        keys = ["weather/JP-TOKYO/2024/01/2024-01-01.json", "weather/JP-TOKYO/2024/01/2024-01-03.json"]
        schema = StructType([StructField("object_key", StringType(), False)])
        raw = self.spark.createDataFrame([(key,) for key in keys], schema)
        processed = self.spark.createDataFrame([(keys[0],)], schema)
        self.assertEqual(weather.find_unprocessed_weather_keys(raw, processed), [keys[1]])
        self.assertEqual(weather.find_unprocessed_weather_keys(raw, raw), [])

    def test_s3a_listing_extracts_old_and_new_object_keys(self) -> None:
        keys = [
            "weather/JP-TOKYO/2024/01/2024-01-01-2024-01-02.json",
            "weather/JP-TOKYO/2024/01/2024-01-03.json",
        ]
        paths = self.spark.createDataFrame(
            [(f"s3a://raw-batch/{key}",) for key in keys], ["path"]
        )
        spark = MagicMock()
        reader = spark.read.format.return_value
        reader.option.return_value = reader
        reader.load.return_value = paths

        actual = weather.read_raw_weather_keys(spark, "raw-batch")

        self.assertEqual({row["object_key"] for row in actual.collect()}, set(keys))
        spark.read.format.assert_called_once_with("binaryFile")
        reader.load.assert_called_once_with("s3a://raw-batch/weather/")

    def test_missing_or_null_metric_array_is_rejected(self) -> None:
        for value in (None, [None]):
            with self.subTest(value=value):
                _, raw = self.read_raw(data=weather_json(["2024-01-01"], precipitation_sum=value))
                with self.assertRaisesRegex(ValueError, "daily.precipitation_sum"):
                    weather.validate_raw_weather(raw)

    def test_empty_raw_or_missing_daily_object_is_rejected(self) -> None:
        _, raw = self.read_raw(data={"daily": None})
        with self.assertRaisesRegex(ValueError, "отсутствует объект daily"):
            weather.validate_raw_weather(raw)
        with self.assertRaisesRegex(ValueError, "ни одной записи"):
            weather.validate_raw_weather(raw.limit(0))

    def test_mismatched_daily_array_lengths_are_rejected(self) -> None:
        _, raw = self.read_raw(data=weather_json(["2024-01-01"], precipitation_sum=[1.0, 2.0]))
        with self.assertRaisesRegex(ValueError, "разную длину"):
            weather.validate_raw_weather(raw)

    def test_overlapping_files_are_rejected_before_write(self) -> None:
        _, first = self.write_raw("JP-TOKYO", "2024-01-01-2024-01-02.json", weather_json(["2024-01-01", "2024-01-02"]))
        _, second = self.write_raw("JP-TOKYO", "2024-01-02.json", weather_json(["2024-01-02"]))
        daily = weather.flatten_daily_weather(weather.read_raw_weather(self.spark, [first, second]))
        typed = weather.cast_weather_types(daily)
        with self.assertRaisesRegex(ValueError, "повторные наблюдения"):
            weather.validate_unique_weather_dates(typed)

    def test_invalid_date_or_numeric_value_is_rejected(self) -> None:
        _, raw = self.read_raw(data=weather_json(["not-a-date"], relative_humidity_2m_mean=[101.0]))
        daily = weather.flatten_daily_weather(raw)
        with self.assertRaisesRegex(ValueError, "вне допустимых границ"):
            weather.validate_weather_df(daily)
        typed = weather.cast_weather_types(daily)
        with self.assertRaisesRegex(ValueError, "содержат null"):
            weather.validate_required_weather_fields(typed)

    def test_unknown_region_is_rejected(self) -> None:
        _, raw = self.read_raw(region="JP-UNKNOWN")
        typed = weather.cast_weather_types(weather.flatten_daily_weather(raw))
        regions = self.spark.createDataFrame([(1, "JP-TOKYO")], ["region_id", "region_code"])
        with self.assertRaisesRegex(ValueError, "не найден region_id"):
            weather.attach_region_ids(typed, regions)

    def test_staging_rejects_missing_rows_or_wrong_file_keys(self) -> None:
        key, raw = self.read_raw()
        typed = weather.cast_weather_types(weather.flatten_daily_weather(raw))
        regions = self.spark.createDataFrame([(1, "JP-TOKYO")], ["region_id", "region_code"])
        joined = weather.attach_region_ids(typed, regions)
        with self.assertRaisesRegex(ValueError, "ожидалось 2"):
            weather.validate_staging_row_count(joined, expected_rows=2)
        with self.assertRaisesRegex(ValueError, "не совпадают"):
            weather.validate_staging_object_keys(joined, expected_keys=["weather/other.json"])
        with self.assertRaisesRegex(ValueError, "строки без object_key"):
            weather.validate_staging_object_keys(
                joined.withColumn("object_key", F.lit("")), expected_keys=[key]
            )
        self.assertEqual(joined.select(F.col("object_key")).first()[0], key)


if __name__ == "__main__":
    unittest.main()

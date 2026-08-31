"""Tests for the regional DDS reference loader."""

import unittest
from unittest.mock import MagicMock, call, patch

from pyspark.sql.types import DecimalType, StringType

from processing.spark.config import SparkSettings
from processing.spark.load_regions import (
    REGIONS_PATH,
    TARGET_TABLE,
    get_regions_schema,
    run_regions_load,
)


def make_settings() -> SparkSettings:
    """Create deterministic Spark settings for unit tests."""

    return SparkSettings(
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


class RegionsSchemaTests(unittest.TestCase):
    def test_get_regions_schema_matches_postgres_columns(self) -> None:
        schema = get_regions_schema()

        self.assertEqual(
            schema.fieldNames(),
            [
                "region_code",
                "prefecture_name",
                "city_name",
                "latitude",
                "longitude",
            ],
        )
        self.assertIsInstance(schema["region_code"].dataType, StringType)
        self.assertEqual(schema["latitude"].dataType, DecimalType(8, 5))
        self.assertEqual(schema["longitude"].dataType, DecimalType(9, 5))


class RegionsWorkflowTests(unittest.TestCase):
    @patch("processing.spark.load_regions.write_jdbc")
    @patch("processing.spark.load_regions.find_new_regions")
    @patch("processing.spark.load_regions.validate_region_conflicts")
    @patch("processing.spark.load_regions.validate_loaded_regions")
    @patch("processing.spark.load_regions.read_jdbc")
    @patch("processing.spark.load_regions.validate_regions")
    @patch("processing.spark.load_regions.normalize_regions")
    @patch("processing.spark.load_regions.read_source_regions")
    @patch("processing.spark.load_regions.create_spark_session")
    def test_run_regions_load_writes_and_verifies_new_regions(
        self,
        create_spark_session: MagicMock,
        read_source_regions: MagicMock,
        normalize_regions: MagicMock,
        validate_regions: MagicMock,
        read_jdbc: MagicMock,
        validate_loaded_regions: MagicMock,
        validate_region_conflicts: MagicMock,
        find_new_regions: MagicMock,
        write_jdbc: MagicMock,
    ) -> None:
        settings = make_settings()
        spark = create_spark_session.return_value
        raw_df = MagicMock()
        source_df = MagicMock()
        existing_df = MagicMock()
        new_df = MagicMock()
        loaded_df = MagicMock()
        read_source_regions.return_value = raw_df
        normalize_regions.return_value = source_df
        source_df.count.return_value = 8
        read_jdbc.side_effect = [existing_df, loaded_df]
        find_new_regions.return_value = new_df
        new_df.count.return_value = 8

        result = run_regions_load(settings=settings)

        read_source_regions.assert_called_once_with(
            spark=spark,
            path=REGIONS_PATH,
        )
        normalize_regions.assert_called_once_with(regions_df=raw_df)
        validate_regions.assert_called_once_with(regions_df=source_df)
        read_jdbc.assert_has_calls(
            [
                call(
                    spark=spark,
                    settings=settings,
                    dbtable=TARGET_TABLE,
                ),
                call(
                    spark=spark,
                    settings=settings,
                    dbtable=TARGET_TABLE,
                ),
            ]
        )
        validate_region_conflicts.assert_called_once_with(
            source_df=source_df,
            target_df=existing_df,
        )
        write_jdbc.assert_called_once_with(
            settings=settings,
            dbtable=TARGET_TABLE,
            df=new_df,
            mode="append",
        )
        validate_loaded_regions.assert_called_once_with(
            source_df=source_df,
            target_df=loaded_df,
        )
        self.assertEqual(
            result,
            {"source_regions": 8, "inserted_regions": 8},
        )
        spark.stop.assert_called_once_with()

    @patch("processing.spark.load_regions.write_jdbc")
    @patch("processing.spark.load_regions.find_new_regions")
    @patch("processing.spark.load_regions.validate_region_conflicts")
    @patch("processing.spark.load_regions.validate_loaded_regions")
    @patch("processing.spark.load_regions.read_jdbc")
    @patch("processing.spark.load_regions.validate_regions")
    @patch("processing.spark.load_regions.normalize_regions")
    @patch("processing.spark.load_regions.read_source_regions")
    @patch("processing.spark.load_regions.create_spark_session")
    def test_run_regions_load_skips_write_when_nothing_is_new(
        self,
        create_spark_session: MagicMock,
        read_source_regions: MagicMock,
        normalize_regions: MagicMock,
        validate_regions: MagicMock,
        read_jdbc: MagicMock,
        validate_loaded_regions: MagicMock,
        validate_region_conflicts: MagicMock,
        find_new_regions: MagicMock,
        write_jdbc: MagicMock,
    ) -> None:
        settings = make_settings()
        source_df = MagicMock()
        source_df.count.return_value = 8
        normalize_regions.return_value = source_df
        new_df = MagicMock()
        new_df.count.return_value = 0
        find_new_regions.return_value = new_df
        read_jdbc.side_effect = [MagicMock(), MagicMock()]

        result = run_regions_load(settings=settings)

        write_jdbc.assert_not_called()
        self.assertEqual(
            result,
            {"source_regions": 8, "inserted_regions": 0},
        )
        create_spark_session.return_value.stop.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()

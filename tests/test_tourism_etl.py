"""Tests for tourism file selection and state validation without external I/O."""

import re
import subprocess
import unittest
from unittest.mock import DEFAULT, Mock, patch

from pyspark.sql import SparkSession

from ingestion.batch.tourism_statistics import STATE_SOURCE, STATE_VERSION
from processing.spark import tourism_etl as tourism
from processing.spark.config import SparkSettings


def make_state(object_key: str = "tourism/jta_accommodation/final/2025/current.xlsx") -> dict:
    return {
        "version": STATE_VERSION,
        "source": STATE_SOURCE,
        "last_checked_at": None,
        "artifacts": {"final:2025": {"object_key": object_key}},
    }


def make_settings() -> SparkSettings:
    return SparkSettings(
        minio_endpoint="http://minio:9000",
        minio_access_key="test-user",
        minio_secret_key="test-password",
        raw_bucket="raw-batch",
        postgres_jdbc_url="jdbc:postgresql://postgres:5432/japan_travel",
        postgres_user="test-user",
        postgres_password="test-password",
    )


def supports_local_spark() -> bool:
    """Check whether Java 17+ is available for local Spark tests."""

    try:
        result = subprocess.run(
            ["java", "-version"], capture_output=True, text=True, timeout=5
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    match = re.search(r'version "(\d+)', result.stderr + result.stdout)
    return bool(match and int(match.group(1)) >= 17)


def make_tourism_row(**overrides) -> dict:
    return {
        "prefecture_code": 13,
        "region_code": "JP-TOKYO",
        "year": 2025,
        "month": 1,
        "total_guest_nights": 1000,
        "foreign_guest_nights": 100,
        "logical_key": "final:2025",
        "release_type": "final",
        "object_key": "tourism/jta_accommodation/final/2025/final.xlsx",
        **overrides,
    }


class TourismEtlTests(unittest.TestCase):
    @patch.object(tourism, "parse_jta_workbook")
    @patch.object(tourism, "load_bytes_from_minio")
    def test_artifact_rows_are_flat_and_keep_each_files_metadata(
        self, load_bytes, parse_workbook,
    ) -> None:
        final_key = "tourism/jta_accommodation/final/2025/final.xlsx"
        monthly_key = "tourism/jta_accommodation/second_preliminary/2026/06/monthly.xlsx"
        final_rows = [
            {"prefecture_code": 13, "year": 2025, "month": 1},
            {"prefecture_code": 13, "year": 2025, "month": 2},
        ]
        monthly_rows = [{"prefecture_code": 13, "year": 2026, "month": 6}]
        load_bytes.side_effect = [b"final-content", b"monthly-content"]
        parse_workbook.side_effect = [final_rows, monthly_rows]

        rows = tourism.read_tourism_artifacts(
            unprocessed_artifacts=[
                {"logical_key": "final:2025", "object_key": final_key},
                {"logical_key": "second_preliminary:2026-06", "object_key": monthly_key},
            ],
            client=Mock(),
            bucket="raw-batch",
        )

        self.assertEqual(len(rows), 3)
        self.assertEqual([row["month"] for row in rows], [1, 2, 6])
        self.assertEqual([row["object_key"] for row in rows], [final_key, final_key, monthly_key])
        self.assertEqual(
            [row["logical_key"] for row in rows],
            ["final:2025", "final:2025", "second_preliminary:2026-06"],
        )
        self.assertEqual(
            [row["release_type"] for row in rows],
            ["final", "final", "second_preliminary"],
        )
        self.assertEqual(parse_workbook.call_args_list[0].kwargs, {
            "content": b"final-content", "release_type": "final", "year": 2025, "month": None,
        })
        self.assertEqual(parse_workbook.call_args_list[1].kwargs, {
            "content": b"monthly-content", "release_type": "second_preliminary", "year": 2026, "month": 6,
        })

    @patch.object(tourism, "parse_jta_workbook")
    @patch.object(tourism, "load_bytes_from_minio")
    def test_empty_artifact_list_returns_no_rows_without_reading_files(
        self, load_bytes, parse_workbook,
    ) -> None:
        self.assertEqual(tourism.read_tourism_artifacts([], Mock(), "raw-batch"), [])
        load_bytes.assert_not_called()
        parse_workbook.assert_not_called()

    def test_final_artifact_period_uses_integer_year_and_no_month(self) -> None:
        result = tourism.define_object_type({"logical_key": "final:2025"})

        self.assertEqual(result, ("final", 2025, None))

    def test_preliminary_artifact_period_uses_integer_year_and_month(self) -> None:
        result = tourism.define_object_type({
            "logical_key": "second_preliminary:2026-08",
        })

        self.assertEqual(result, ("second_preliminary", 2026, 8))

    def test_artifact_period_rejects_unsupported_release_type(self) -> None:
        with self.assertRaises(ValueError):
            tourism.define_object_type({"logical_key": "unknown:2025"})

    def test_state_requires_nonempty_string_object_keys(self) -> None:
        for value in (None, 123, "", "   "):
            with self.subTest(value=value):
                state = make_state()
                state["artifacts"]["final:2025"]["object_key"] = value

                with self.assertRaises(ValueError):
                    tourism.validate_tourism_etl_state(state)

    def test_state_accepts_valid_artifacts_and_rejects_invalid_containers(self) -> None:
        tourism.validate_tourism_etl_state(make_state())
        empty_state = make_state()
        empty_state["artifacts"] = {}
        tourism.validate_tourism_etl_state(empty_state)

        for value in (None, [], "object-key"):
            with self.subTest(value=value):
                state = make_state()
                state["artifacts"]["final:2025"] = value

                with self.assertRaises(ValueError):
                    tourism.validate_tourism_etl_state(state)

    def test_file_selection_uses_current_state_and_keeps_unprocessed_revision(self) -> None:
        current_key = "tourism/jta_accommodation/final/2025/current.xlsx"
        processed_key = "tourism/jta_accommodation/final/2024/processed.xlsx"
        state = make_state(current_key)
        state["artifacts"]["final:2024"] = {"object_key": processed_key}
        processed_df = Mock()
        processed_df.select.return_value.collect.return_value = [
            {"object_key": "tourism/jta_accommodation/final/2025/older.xlsx"},
            {"object_key": processed_key},
        ]

        result = tourism.find_unprocessed_tourism_artifacts(state, processed_df)

        self.assertEqual(result, [
            {"logical_key": "final:2025", "object_key": current_key},
        ])

    def test_file_selection_follows_current_revision_a_to_b_and_back_to_a(self) -> None:
        key_a = "tourism/jta_accommodation/final/2025/a.xlsx"
        key_b = "tourism/jta_accommodation/final/2025/b.xlsx"
        processed_df = Mock()

        for current_key, saved_key, expected in (
            (key_a, key_a, []),
            (key_b, key_a, [{"logical_key": "final:2025", "object_key": key_b}]),
            (key_a, key_b, [{"logical_key": "final:2025", "object_key": key_a}]),
        ):
            with self.subTest(current_key=current_key, saved_key=saved_key):
                processed_df.select.return_value.collect.return_value = [
                    {"object_key": saved_key},
                ]

                self.assertEqual(
                    tourism.find_unprocessed_tourism_artifacts(
                        make_state(current_key), processed_df,
                    ),
                    expected,
                )

    @patch.object(tourism, "read_jdbc")
    @patch.object(tourism, "load_json_from_minio", return_value=None)
    @patch.object(tourism, "create_minio_client")
    @patch.object(tourism, "create_spark_session")
    def test_missing_state_stops_before_postgres_and_closes_spark(
        self, create_session, _create_client, _load_state, read_jdbc,
    ) -> None:
        with self.assertRaises(ValueError):
            tourism.run_tourism_etl(make_settings())

        read_jdbc.assert_not_called()
        create_session.return_value.stop.assert_called_once_with()

    @patch.object(tourism, "create_minio_client", side_effect=RuntimeError("client failed"))
    @patch.object(tourism, "create_spark_session")
    def test_client_creation_failure_still_closes_spark(
        self, create_session, _create_client,
    ) -> None:
        with self.assertRaises(RuntimeError):
            tourism.run_tourism_etl(make_settings())

        create_session.return_value.stop.assert_called_once_with()


class TourismWorkflowTests(unittest.TestCase):
    def setUp(self) -> None:
        patcher = patch.multiple(
            tourism,
            create_spark_session=DEFAULT,
            create_minio_client=DEFAULT,
            load_json_from_minio=DEFAULT,
            read_jdbc=DEFAULT,
            read_tourism_artifacts=DEFAULT,
            attach_regions_ids=DEFAULT,
            truncate_postgres_table=DEFAULT,
            write_jdbc=DEFAULT,
            execute_postgres_transaction=DEFAULT,
        )
        self.mocks = patcher.start()
        self.addCleanup(patcher.stop)
        self.state = make_state()
        self.state["artifacts"]["second_preliminary:2026-06"] = {
            "object_key": "tourism/jta_accommodation/second_preliminary/2026/06/monthly.xlsx",
        }
        self.mocks["load_json_from_minio"].return_value = self.state
        self.processed_df = Mock()
        self.processed_df.select.return_value.collect.return_value = []
        self.staging_df = Mock()
        self.staging_df.count.return_value = 104
        self.mocks["read_jdbc"].side_effect = [self.processed_df, Mock(), self.staging_df]
        self.mocks["attach_regions_ids"].return_value.count.return_value = 104
        self.mocks["read_tourism_artifacts"].return_value = [make_tourism_row()]

    def test_success_reports_input_rows_after_committing_transaction(self) -> None:
        result = tourism.run_tourism_etl(make_settings())

        self.assertEqual(result, {"processed_files": 2, "tourism_rows": 104})
        self.mocks["execute_postgres_transaction"].assert_called_once()
        statements = self.mocks["execute_postgres_transaction"].call_args.kwargs["statements"]
        self.assertEqual(len(statements), 3)
        self.assertEqual(
            statements[-1].strip(),
            f"TRUNCATE TABLE {tourism.STAGING_TOURISM_TABLE}",
        )
        self.mocks["create_spark_session"].return_value.stop.assert_called_once_with()

    def test_no_new_files_skips_staging_and_reports_zero_counts(self) -> None:
        self.processed_df.select.return_value.collect.return_value = [
            {"object_key": artifact["object_key"]}
            for artifact in self.state["artifacts"].values()
        ]

        result = tourism.run_tourism_etl(make_settings())

        self.assertEqual(result, {"processed_files": 0, "tourism_rows": 0})
        self.mocks["read_tourism_artifacts"].assert_not_called()
        self.mocks["truncate_postgres_table"].assert_not_called()
        self.mocks["write_jdbc"].assert_not_called()
        self.mocks["execute_postgres_transaction"].assert_not_called()

    def test_incomplete_staging_stops_before_publishing(self) -> None:
        self.staging_df.count.return_value = 103

        with self.assertRaises(ValueError):
            tourism.run_tourism_etl(make_settings())

        self.mocks["execute_postgres_transaction"].assert_not_called()
        self.mocks["create_spark_session"].return_value.stop.assert_called_once_with()

    def test_transaction_failure_propagates_without_success_summary(self) -> None:
        self.mocks["execute_postgres_transaction"].side_effect = RuntimeError("commit failed")

        with self.assertRaises(RuntimeError):
            tourism.run_tourism_etl(make_settings())

        self.mocks["create_spark_session"].return_value.stop.assert_called_once_with()


@unittest.skipUnless(supports_local_spark(), "Spark 4 requires Java 17+")
class TourismRegionJoinTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("tourism-region-join-tests")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def test_join_attaches_region_ids_and_preserves_staging_values(self) -> None:
        rows = [
            make_tourism_row(),
            make_tourism_row(prefecture_code=26, region_code="JP-KYOTO"),
        ]
        source_df = self.spark.createDataFrame(rows, tourism.get_tourism_schema())
        regions_df = self.spark.createDataFrame(
            [(1, "JP-TOKYO", "Tokyo"), (2, "JP-KYOTO", "Kyoto")],
            ["region_id", "region_code", "prefecture_name"],
        )

        result = tourism.attach_regions_ids(source_df, regions_df)

        self.assertEqual(result.columns, [
            "region_id", "year", "month", "total_guest_nights",
            "foreign_guest_nights", "logical_key", "release_type", "object_key",
        ])
        self.assertEqual(
            [row.asDict() for row in result.orderBy("region_id").collect()],
            [
                {
                    "region_id": region_id, "year": 2025, "month": 1,
                    "total_guest_nights": 1000, "foreign_guest_nights": 100,
                    "logical_key": "final:2025",
                    "release_type": "final", "object_key": rows[0]["object_key"],
                }
                for region_id in (1, 2)
            ],
        )

    def test_join_rejects_a_region_missing_from_dds(self) -> None:
        source_df = self.spark.createDataFrame(
            [make_tourism_row(region_code="JP-KYOTO", prefecture_code=26)],
            tourism.get_tourism_schema(),
        )
        regions_df = self.spark.createDataFrame(
            [(1, "JP-TOKYO")], ["region_id", "region_code"]
        )

        with self.assertRaises(ValueError):
            tourism.attach_regions_ids(source_df, regions_df)

    def test_join_keeps_overlapping_publications_as_distinct_source_rows(self) -> None:
        preliminary_key = "tourism/jta_accommodation/second_preliminary/2025/01/preliminary.xlsx"
        source_df = self.spark.createDataFrame(
            [
                make_tourism_row(),
                make_tourism_row(
                    logical_key="second_preliminary:2025-01",
                    release_type="second_preliminary", object_key=preliminary_key,
                    total_guest_nights=900,
                ),
            ],
            tourism.get_tourism_schema(),
        )
        regions_df = self.spark.createDataFrame(
            [(1, "JP-TOKYO")], ["region_id", "region_code"]
        )

        rows = tourism.attach_regions_ids(source_df, regions_df).collect()

        self.assertEqual(len(rows), 2)
        self.assertEqual(
            {(row["region_id"], row["year"], row["month"]) for row in rows},
            {(1, 2025, 1)},
        )
        self.assertEqual(
            {
                (row["logical_key"], row["release_type"], row["object_key"], row["total_guest_nights"])
                for row in rows
            },
            {
                ("final:2025", "final", make_tourism_row()["object_key"], 1000),
                ("second_preliminary:2025-01", "second_preliminary", preliminary_key, 900),
            },
        )


if __name__ == "__main__":
    unittest.main()

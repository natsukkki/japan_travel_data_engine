from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructType,
    StructField,
)

from ingestion.batch.tourism_statistics import STATE_KEY, validate_state
from processing.spark.config import load_spark_settings, SparkSettings
from processing.spark.jdbc import (
    execute_postgres_transaction,
    read_jdbc,
    truncate_postgres_table,
    write_jdbc,
)
from processing.spark.jta_excel_parser import parse_jta_workbook
from processing.spark.session import create_spark_session
from storage.minio import create_minio_client, load_bytes_from_minio, load_json_from_minio


REGIONS_TABLE = "dds_schema.dds_regions"
TOURISM_MONTHLY_TABLE = "dds_schema.dds_tourism_monthly"
TOURISM_PROCESSED_TABLE = "dds_schema.dds_tourism_processed_files"
STAGING_TOURISM_TABLE = "dds_schema.dds_staging_tourism_monthly"


def get_tourism_schema() -> StructType:
    """Return the schema of parsed tourism rows with source metadata."""

    return StructType(
        [
            StructField(
                name="prefecture_code",
                dataType=IntegerType(),
                nullable=False,
            ),
            StructField(
                name="region_code",
                dataType=StringType(),
                nullable=False,
            ),
            StructField(
                name="year",
                dataType=IntegerType(),
                nullable=False,
            ),
            StructField(
                name="month",
                dataType=IntegerType(),
                nullable=False,
            ),
            StructField(
                name="total_guest_nights",
                dataType=IntegerType(),
                nullable=False,
            ),
            StructField(
                name="foreign_guest_nights",
                dataType=IntegerType(),
                nullable=False,
            ),
            StructField(
                name="logical_key",
                dataType=StringType(),
                nullable=False,
            ),
            StructField(
                name="release_type",
                dataType=StringType(),
                nullable=False,
            ),
            StructField(
                name="object_key",
                dataType=StringType(),
                nullable=False,
            ),
        ]
    )


def validate_tourism_etl_state(state: Any) -> None:
    """Validate tourism state and the current artifact object keys."""

    if state is None:
        raise ValueError(
            "State туризма не найден. Сначала выполни ingestion."
        )

    validate_state(state=state)

    for logical_key, artifact in state["artifacts"].items():
        if not isinstance(artifact, dict):
            raise ValueError(
                f"Некорректная запись state: {logical_key}"
            )

        object_key = artifact.get("object_key")

        if not isinstance(object_key, str) or not object_key.strip():
            raise ValueError(
                f"Отсутствует или некорректен object_key "
                f"для публикации {logical_key}"
            )


def find_unprocessed_tourism_artifacts(
    state: dict[str, Any],
    processed_files_df: DataFrame,
) -> list[dict[str, str]]:
    """Return current tourism artifacts absent from the processed-files log."""

    processed_keys = {
        row["object_key"]
        for row in processed_files_df.select("object_key").collect()
    }

    return [
        {
            "logical_key": logical_key,
            "object_key": artifact["object_key"],
        }
        for logical_key, artifact in state["artifacts"].items()
        if artifact["object_key"] not in processed_keys
    ]


def define_object_type(
    artifact: dict[str, str],
) -> tuple[str, int, int | None]:
    """Parse the release type, year, and month from a tourism artifact."""

    logical_key = artifact.get("logical_key")
    if not isinstance(logical_key, str) or not logical_key.strip():
        raise ValueError(
            "Поле logical_key публикации должно быть непустой строкой"
        )

    release_type, period = logical_key.split(":", maxsplit=1)

    if release_type == "second_preliminary":
        year, month = period.split("-", maxsplit=1)
        return release_type, int(year), int(month)
    elif release_type == "final":
        return release_type, int(period), None
    else:
        raise ValueError(
            f"Неизвестный тип публикации {release_type!r} "
            f"в logical_key {logical_key!r}"
        )


def read_tourism_artifacts(
    unprocessed_artifacts: list[dict[str, str]],
    client,
    bucket: str,
) -> list[dict[str, Any]]:
    """Download and parse tourism artifacts into rows with source metadata."""

    all_rows = []

    for artifact in unprocessed_artifacts:
        object_key = artifact.get("object_key")

        artifact_bytes = load_bytes_from_minio(
            client=client,
            bucket=bucket,
            object_key=object_key,
        )

        release_type, year, month = define_object_type(artifact)

        result = parse_jta_workbook(
            content=artifact_bytes,
            release_type=release_type,
            year=year,
            month=month,
        )
        for row in result:
            row["logical_key"] = artifact["logical_key"]
            row["release_type"] = release_type
            row["object_key"] = object_key
        all_rows.extend(result)

    return all_rows


def attach_regions_ids(
    tourism_df: DataFrame,
    dds_regions_df: DataFrame,
) -> DataFrame:
    """Attach DDS region IDs, reject unmatched rows, and select staging columns."""

    joined_df = tourism_df.join(
        other=dds_regions_df,
        on="region_code",
        how="left",
    )

    unmatched_regions_df = joined_df.filter(
        F.col("region_id").isNull()
    )
    if unmatched_regions_df.head(1):
        raise ValueError(
            "Для части туристических данных не найден region_id "
            "в справочнике регионов"
        )

    return joined_df.select(
        "region_id",
        "year",
        "month",
        "total_guest_nights",
        "foreign_guest_nights",
        "logical_key",
        "release_type",
        "object_key",
    )


def run_tourism_etl(
    settings: SparkSettings | None = None,
) -> dict[str, int]:
    """Process tourism files and report file and staged-input row counts.

    tourism_rows counts input rows before publication version selection,
    not the number of rows inserted or updated in DDS.
    """

    runtime_settings = settings or load_spark_settings()
    spark = create_spark_session(
        app_name="tourism_etl",
        settings=runtime_settings,
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        client = create_minio_client(
            endpoint=runtime_settings.minio_endpoint,
            access_key=runtime_settings.minio_access_key,
            secret_key=runtime_settings.minio_secret_key,
        )
        tourism_state = load_json_from_minio(
            client=client,
            bucket=runtime_settings.raw_bucket,
            object_key=STATE_KEY,
        )

        validate_tourism_etl_state(state=tourism_state)

        processed_files_df = read_jdbc(
            spark=spark,
            settings=runtime_settings,
            dbtable=TOURISM_PROCESSED_TABLE,
        )

        unprocessed_artifacts = find_unprocessed_tourism_artifacts(
            state=tourism_state,
            processed_files_df=processed_files_df,
        )
        if not unprocessed_artifacts:
            return {
                "processed_files": 0,
                "tourism_rows": 0,
            }

        tourism_rows = read_tourism_artifacts(
            unprocessed_artifacts=unprocessed_artifacts,
            client=client,
            bucket=runtime_settings.raw_bucket,
        )

        tourism_df = spark.createDataFrame(
            data=tourism_rows,
            schema=get_tourism_schema(),
        )

        dds_regions_df = read_jdbc(
            spark=spark,
            settings=runtime_settings,
            dbtable=REGIONS_TABLE,
        )

        tourism_monthly_df = attach_regions_ids(
            tourism_df=tourism_df,
            dds_regions_df=dds_regions_df,
        )

        expected_rows = tourism_monthly_df.count()

        truncate_postgres_table(
            settings=runtime_settings,
            table_name=STAGING_TOURISM_TABLE,
        )

        write_jdbc(
            settings=runtime_settings,
            dbtable=STAGING_TOURISM_TABLE,
            df=tourism_monthly_df,
        )

        actual_rows = read_jdbc(
            spark=spark,
            settings=runtime_settings,
            dbtable=STAGING_TOURISM_TABLE,
        ).count()

        if expected_rows != actual_rows:
            raise ValueError(
                f"Неполная загрузка staging: ожидалось {expected_rows} строк, "
                f"получено {actual_rows}"
            )

        execute_postgres_transaction(
            settings=runtime_settings,
            statements=(
                f'''
                    INSERT INTO {TOURISM_MONTHLY_TABLE} AS t(
                        region_id,
                        year,
                        month,
                        total_guest_nights,
                        foreign_guest_nights,
                        release_type
                    )
                    SELECT DISTINCT ON (region_id, year, month)
                        region_id,
                        year,
                        month,
                        total_guest_nights,
                        foreign_guest_nights,
                        release_type
                    FROM {STAGING_TOURISM_TABLE}
                    ORDER BY region_id ASC, year ASC, month ASC, release_type ASC
                    ON CONFLICT (region_id, year, month)
                    DO UPDATE
                    SET
                        total_guest_nights = excluded.total_guest_nights,
                        foreign_guest_nights = excluded.foreign_guest_nights,
                        release_type = excluded.release_type
                    WHERE (
                        (
                            t.release_type = excluded.release_type
                            AND (
                                t.total_guest_nights != excluded.total_guest_nights
                                OR t.foreign_guest_nights != excluded.foreign_guest_nights
                            )
                        )
                        OR (
                            t.release_type = 'second_preliminary'
                            AND excluded.release_type = 'final'
                        )
                    )
                ''',
                f'''
                    INSERT INTO {TOURISM_PROCESSED_TABLE} (
                        logical_key,
                        object_key,
                        processed_at
                    )
                    SELECT DISTINCT
                        logical_key,
                        object_key,
                        CURRENT_TIMESTAMP AS processed_at
                    FROM {STAGING_TOURISM_TABLE}
                    ON CONFLICT (logical_key)
                    DO UPDATE
                    SET
                        object_key = excluded.object_key,
                        processed_at = excluded.processed_at
                ''',
                f'''
                    TRUNCATE TABLE {STAGING_TOURISM_TABLE}
                ''',
            )
        )

        return {
            "processed_files": len(unprocessed_artifacts),
            "tourism_rows": expected_rows,
        }
    finally:
        spark.stop()


def main():
    results = run_tourism_etl()
    print(results)


if __name__ == "__main__":
    main()

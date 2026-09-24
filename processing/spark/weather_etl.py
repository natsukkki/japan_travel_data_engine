"""Transform raw Open-Meteo files from MinIO for the weather DDS table."""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


from processing.spark.jdbc import read_jdbc, write_jdbc, truncate_postgres_table, execute_postgres_transaction
from processing.spark.config import load_spark_settings, SparkSettings
from processing.spark.session import create_spark_session

REGIONS_TABLE = "dds_schema.dds_regions"
WEATHER_TABLE = "dds_schema.dds_weather_daily"
WEATHER_PROCESSED_FILES = "dds_schema.dds_weather_processed_files"
WEATHER_STAGING_TABLE = "dds_schema.dds_staging_weather_daily"

DAILY_ARRAY_FIELDS = (
    "time",
    "temperature_2m_mean",
    "precipitation_sum",
    "relative_humidity_2m_mean",
    "wind_speed_10m_mean",
    "sunshine_duration",
)

METRIC_COLUMNS = (
    "temperature_mean",
    "precipitation_sum",
    "humidity_mean",
    "wind_speed_mean",
    "sunshine_duration_seconds",
)


def read_raw_weather_keys(
    spark: SparkSession,
    bucket: str
) -> DataFrame:
    """List weather JSON object keys in MinIO without loading their contents."""

    return (
        spark.read
        .format("binaryFile")
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "*.json")
        .load(f"s3a://{bucket}/weather/")
        .select(
            F.regexp_extract(
                F.col("path"),
                r"/(weather/.+\.json)$",
                1,
            ).alias("object_key")
        )
    )


def get_raw_weather_schema() -> StructType:
    """Return the expected schema of raw Open-Meteo JSON files."""

    daily_schema = StructType( 
        [
            StructField(
                "time",
                ArrayType(StringType(), containsNull=True),
                nullable=True,
            ),
            StructField(
                "temperature_2m_mean",
                ArrayType(DoubleType(), containsNull=True),
                nullable=True,
            ),
            StructField(
                "precipitation_sum",
                ArrayType(DoubleType(), containsNull=True),
                nullable=True,
            ),
            StructField(
                "relative_humidity_2m_mean",
                ArrayType(DoubleType(), containsNull=True),
                nullable=True,
            ),
            StructField(
                "wind_speed_10m_mean",
                ArrayType(DoubleType(), containsNull=True),
                nullable=True,
            ),
            StructField(
                "sunshine_duration",
                ArrayType(DoubleType(), containsNull=True),
                nullable=True,
            ),
        ]
    )

    return StructType(
        [
            StructField("daily", daily_schema, nullable=True),
        ]
    )


def read_raw_weather(
    spark: SparkSession,
    object_paths: list[str],
) -> DataFrame:
    """Read selected raw Open-Meteo JSON objects from MinIO."""

    return (
        spark.read
        .schema(get_raw_weather_schema())
        .json(object_paths)
        .withColumn("source_file", F.input_file_name()) 
        .withColumn(
            "region_code",
            F.regexp_extract(
                F.col("source_file"), 
                r"/weather/([^/]+)/",
                1
            ), 
        )
    )


def validate_daily_fields(weather_df: DataFrame) -> None:
    """Reject missing, empty, or partially null daily metric arrays."""

    for field_name in DAILY_ARRAY_FIELDS:
        field = F.col(f"daily.{field_name}")

        invalid_array_rows = weather_df.filter(
            field.isNull() 
            | (F.size(field) == 0)
            | F.exists(field, lambda value: value.isNull())
        )
        if invalid_array_rows.head(1):
            raise ValueError(
                f"Массив daily.{field_name} отсутствует, пуст "
                "или содержит null"
            )
    

def validate_daily_array_lengths(weather_df: DataFrame) -> None:
    """Reject records whose daily arrays have different lengths."""

    dates_length = F.size(F.col("daily.time"))
    different_lengths_condition = F.lit(False)
    
    for field_name in DAILY_ARRAY_FIELDS[1:]:
        field_length = F.size(F.col(f"daily.{field_name}"))
    
        different_lengths_condition = (
            different_lengths_condition
            | (field_length != dates_length)
        )
    
    rows_with_different_lengths = weather_df.filter(
        different_lengths_condition
    )
    
    if rows_with_different_lengths.head(1):
        raise ValueError(
            "Массивы внутри daily имеют разную длину"
        )


def validate_raw_weather(weather_df: DataFrame) -> None:
    """Validate the structure of raw Open-Meteo weather records."""

    if not weather_df.head(1):
        raise ValueError("Не найдено ни одной записи с погодными данными")

    missing_daily_rows = weather_df.filter(
        F.col("daily").isNull()
    )
    if missing_daily_rows.head(1):
        raise ValueError("В исходных погодных данных отсутствует объект daily")

    validate_daily_fields(weather_df=weather_df)

    validate_daily_array_lengths(weather_df=weather_df)


def flatten_daily_weather(weather_df: DataFrame) -> DataFrame:
    """Expand daily arrays into one row per region and date."""

    daily_df = weather_df.select(
         "source_file",
         "region_code",
        F.explode(F.arrays_zip(
            F.col("daily.time"), 
            F.col("daily.temperature_2m_mean"),
            F.col("daily.precipitation_sum"),
            F.col("daily.relative_humidity_2m_mean"),
            F.col("daily.wind_speed_10m_mean"),
            F.col("daily.sunshine_duration"),
            )
        ).alias("day")
    )

    return daily_df.select(
        "source_file",
        "region_code",
        F.col("day.time").alias("weather_date"),
        F.col("day.temperature_2m_mean").alias("temperature_mean"),
        F.col("day.precipitation_sum").alias("precipitation_sum"),
        F.col("day.relative_humidity_2m_mean").alias("humidity_mean"),
        F.col("day.wind_speed_10m_mean").alias("wind_speed_mean"),
        F.col("day.sunshine_duration").alias("sunshine_duration_seconds")
    )   


def cast_weather_types(weather_df: DataFrame) -> DataFrame:
    """Convert daily weather columns to the target DDS types."""
    
    return weather_df.select(
        "source_file",
        "region_code",
        F.col("weather_date").try_cast(DateType()).alias("weather_date"),
        F.col("temperature_mean").try_cast(DecimalType(5,2)).alias("temperature_mean"),
        F.col("precipitation_sum").try_cast(DecimalType(7,2)).alias("precipitation_sum"),
        F.col("humidity_mean").try_cast(DecimalType(5,2)).alias("humidity_mean"),
        F.col("wind_speed_mean").try_cast(DecimalType(6,2)).alias("wind_speed_mean"),
        F.round(F.col("sunshine_duration_seconds")).try_cast(IntegerType()).alias("sunshine_duration_seconds")
    )


def validate_weather_df(weather_df: DataFrame) -> None:
    """Reject non-finite metrics and out-of-range values."""

    invalid_values_condition = (
        (F.col("humidity_mean") < 0)
        | (F.col("humidity_mean") > 100)
        | (F.col("precipitation_sum") < 0)
        | (F.col("wind_speed_mean") < 0)
        | (F.col("sunshine_duration_seconds") < 0)
        | (F.col("sunshine_duration_seconds") > 86400)
    )

    for column in METRIC_COLUMNS:
        col = F.col(column)

        invalid_values_condition = (
            invalid_values_condition
            | col.isNaN()
            | col.isin(float("inf"), float("-inf"))
        )

    invalid_df = weather_df.filter(invalid_values_condition)

    if invalid_df.head():
        raise ValueError(
            "Погодные данные содержат NaN, бесконечность "
            "или значения вне допустимых границ: "
            "влажность должна быть от 0 до 100 %, осадки и скорость ветра — "
            "неотрицательными, продолжительность солнечного сияния — "
            "от 0 до 86400 секунд включительно"
        )


def validate_required_weather_fields(weather_df: DataFrame) -> None:
    """Reject null dates or weather metrics after conversion to DDS types."""

    null_values_condition = F.col("weather_date").isNull()

    for column_name in METRIC_COLUMNS:
        metric_column = F.col(column_name)

        null_values_condition = (
            null_values_condition
            | metric_column.isNull()
        )

    rows_with_null_values = weather_df.filter(null_values_condition)

    if rows_with_null_values.head():
        raise ValueError(
            "После приведения типов дата наблюдения или погодные показатели содержат null. "
            "Проверьте наличие исходных значений и возможность их "
            "преобразования в целевые типы DDS"
        )


def validate_unique_weather_dates(weather_df: DataFrame) -> None:
    """Reject duplicate observations for the same region and date."""

    observation_counts_df = (
        weather_df
        .groupBy("region_code", "weather_date")
        .agg(F.count("*").alias("row_count"))
    )

    duplicate_observations_df = observation_counts_df.filter(F.col("row_count") >  1)

    if duplicate_observations_df.head():
        raise ValueError(
            "Погодные данные содержат повторные наблюдения "
            "по паре region_code + weather_date: "
            "для одного региона допускается только одна запись за день"
        )


def attach_region_ids(
    weather_df: DataFrame, 
    regions_df: DataFrame
) -> DataFrame:
    """Attach region IDs, reject unmatched regions, and select weather DDS columns."""
    
    joined_df = weather_df.join(
        other=regions_df.select("region_id", "region_code"),
        on="region_code", 
        how="left",
    )

    unmatched_regions_df = joined_df.filter(F.col("region_id").isNull())

    if unmatched_regions_df.head():
        raise ValueError(
            "Для части погодных наблюдений не найден region_id "
            f"в {REGIONS_TABLE}. Проверьте region_code "
            "в путях исходных файлов и наличие регионов в справочнике"
        )

    return joined_df.select(
        "region_id",
        "weather_date",
        "temperature_mean",
        "precipitation_sum",
        "humidity_mean",
        "wind_speed_mean",
        "sunshine_duration_seconds",
        F.regexp_extract(
            F.col("source_file"), 
            r"/(weather/.+\.json)$",
            1,
        ).alias("object_key")
    )


def find_unprocessed_weather_keys(
    minio_keys_df: DataFrame,
    processed_files_df: DataFrame,
) -> list[str]:
    """Return MinIO object keys absent from the processed-files log."""

    
    new_keys_df = minio_keys_df.join(
        other=processed_files_df,
        on="object_key",
        how="left_anti"
    )

    new_object_keys = [
        row["object_key"] 
        for row in new_keys_df.orderBy("object_key").collect()
    ]

    return new_object_keys


def validate_staging_row_count(
    staging_df: DataFrame,
    expected_rows: int,
) -> None:
    """Reject empty or incomplete staging loads."""

    actual_rows = staging_df.count()

    if actual_rows == 0:
        raise ValueError(
            f"Таблица {WEATHER_STAGING_TABLE} пуста после загрузки; "
            f"ожидалось {expected_rows} погодных наблюдений"
        )

    if expected_rows != actual_rows:
        raise ValueError(
            f"В таблице {WEATHER_STAGING_TABLE} найдено {actual_rows} строк "
            f"после загрузки; ожидалось {expected_rows}"
        )

    
def validate_staging_object_keys(
    staging_df: DataFrame,
    expected_keys: list[str],
) -> None:
    """Reject missing, extra, or empty MinIO object keys in staging."""

    missing_key_rows = staging_df.filter(
        F.col("object_key").isNull() | (F.col("object_key") == "")
    )
        
    if missing_key_rows.head(1):
        raise ValueError(
            f"В таблице {WEATHER_STAGING_TABLE} есть строки без object_key: "
            "невозможно сопоставить погодные наблюдения с файлами MinIO"
        )
    
    staging_keys = {
        row["object_key"]
        for row in staging_df.select("object_key").distinct().collect()
    }
    expected_key_set = set(expected_keys)
    
    if staging_keys != expected_key_set:
        raise ValueError(
            f"Ключи в {WEATHER_STAGING_TABLE} не совпадают с выбранными "
            f"файлами MinIO: отсутствуют {sorted(expected_key_set - staging_keys)}; "
            f"лишние {sorted(staging_keys - expected_key_set)}"
        )

   
def run_weather_etl(
    settings: SparkSettings | None =  None,
) -> dict[str, int]:
    """Run the Spark workflow for raw daily weather data."""

    runtime_settings = settings or load_spark_settings()
    spark = create_spark_session(
        app_name="load_dds_weather_daily",
        settings=runtime_settings
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        minio_keys_df = read_raw_weather_keys(
            spark=spark,
            bucket=runtime_settings.raw_bucket,
        )

        processed_keys_df = read_jdbc(
            spark=spark,
            settings=runtime_settings, 
            dbtable=WEATHER_PROCESSED_FILES,
        ).select("object_key")

        new_object_keys = find_unprocessed_weather_keys(
            minio_keys_df=minio_keys_df,
            processed_files_df=processed_keys_df
        )

        if not new_object_keys:
            return {"processed_files": 0, "weather_rows": 0,}

        new_object_paths = [
                f"s3a://{runtime_settings.raw_bucket}/{key}"
                for key in new_object_keys
            ]
        
        weather_df = read_raw_weather(
            spark=spark,
            object_paths=new_object_paths,
        )
        
        validate_raw_weather(weather_df=weather_df)

        daily_df = flatten_daily_weather(weather_df=weather_df)

        validate_weather_df(weather_df=daily_df)

        typed_df = cast_weather_types(weather_df=daily_df)

        validate_required_weather_fields(weather_df=typed_df)
        validate_unique_weather_dates(weather_df=typed_df)

        regions_df = read_jdbc(
            spark=spark,
            settings=runtime_settings,
            dbtable=REGIONS_TABLE
        )

        joined_df = attach_region_ids(
            weather_df=typed_df, 
            regions_df=regions_df
        )

        expected_rows = joined_df.count()

        truncate_postgres_table(
            settings=runtime_settings, 
            table_name=WEATHER_STAGING_TABLE
        )

        write_jdbc(
            settings=runtime_settings,
            dbtable=WEATHER_STAGING_TABLE,
            df=joined_df,
        )

        staging_df = read_jdbc(
            spark=spark,
            settings=runtime_settings,
            dbtable=WEATHER_STAGING_TABLE,
        )

        validate_staging_row_count(
            staging_df=staging_df,
            expected_rows=expected_rows,
        )

        validate_staging_object_keys(
            staging_df=staging_df, 
            expected_keys=new_object_keys
        )

        execute_postgres_transaction(
            settings=runtime_settings,
            statements=('''
                INSERT INTO dds_schema.dds_weather_daily (
                    region_id,
                    weather_date,
                    temperature_mean,
                    precipitation_sum,
                    humidity_mean,
                    wind_speed_mean,
                    sunshine_duration_seconds
                )
                SELECT 
                    region_id,
                    weather_date,
                    temperature_mean,
                    precipitation_sum,
                    humidity_mean,
                    wind_speed_mean,
                    sunshine_duration_seconds
                FROM dds_schema.dds_staging_weather_daily
                ''',
                '''
                INSERT INTO dds_schema.dds_weather_processed_files (
                    object_key, 
                    processed_at
                )
                SELECT DISTINCT 
                    object_key,
                    CURRENT_TIMESTAMP
                FROM dds_schema.dds_staging_weather_daily
                ''',
                '''
                TRUNCATE TABLE dds_schema.dds_staging_weather_daily
                ''',
            ),
        )

        return {
            "processed_files": len(new_object_keys),
            "weather_rows": expected_rows,
        }
    finally:
        spark.stop()


def main():
    """Run the weather ETL as a standalone Spark job."""

    results = run_weather_etl()
    print(results)


if __name__ == "__main__":
    main()
"""Load the regional reference data into the DDS layer."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    StringType,
    StructField,
    StructType,
)

from processing.spark.config import load_spark_settings, SparkSettings
from processing.spark.jdbc import read_jdbc, write_jdbc
from processing.spark.session import create_spark_session

REGIONS_PATH = "/opt/project/ingestion/batch/config/regions.json"
TARGET_TABLE = "dds_schema.dds_regions"


def get_regions_schema() -> StructType:
    """Return the explicit schema of the regions JSON source."""

    return StructType(
        [
            StructField("region_code", StringType(), nullable=True),
            StructField("prefecture_name", StringType(), nullable=True),
            StructField("city_name", StringType(), nullable=True),
            StructField("latitude", DecimalType(precision=8, scale=5), nullable=True),
            StructField("longitude", DecimalType(precision=9, scale=5), nullable=True) #ставим тру чтобы спрак некорреткные строки смог прочитать как Null
        ]
    )


def read_source_regions(
    spark: SparkSession,
    path: str = REGIONS_PATH,
) -> DataFrame:
    """Read regional reference data from JSON."""

    return (
        spark.read
        .option("multiLine", "true")
        .schema(get_regions_schema())
        .json(path)
    )


def normalize_regions(regions_df: DataFrame) -> DataFrame:
    """Trim string values in the regional reference data."""

    return regions_df.select(
        F.trim(F.col("region_code")).alias("region_code"),
        F.trim(F.col("prefecture_name")).alias("prefecture_name"),
        F.trim(F.col("city_name")).alias("city_name"),
        F.col("latitude"),
        F.col("longitude"),
    ) 


def validate_required_fields(regions_df: DataFrame) -> None:
    """Validate required string fields."""

    invalid_condition = (
        F.col("region_code").isNull()
        | (F.col("region_code") == "")
        | F.col("prefecture_name").isNull()
        | (F.col("prefecture_name") == "")
        | F.col("city_name").isNull()
        | (F.col("city_name") == "")
    )

    invalid_rows = regions_df.filter(invalid_condition) #проверяем есть ли побитые данные, а не просто оставляем качественные, так как если будут побитые, то нужно оишбку выбрасывать
    #пересобирает в скл выражение наш код на питоне, будет буквально where region_code IS NULL OR ..

    if invalid_rows.head(1):
        raise ValueError("Справочник регионов содержит пустые обязательные поля")


def validate_coordinates(regions_df: DataFrame) -> None:
    """Validate latitude and longitude values."""

    invalid_conditions = (
        F.col("latitude").isNull()
        | (F.col("latitude") > 90)
        | (F.col("latitude") < -90)
        | F.col("longitude").isNull()
        | (F.col("longitude") > 180)
        | (F.col("longitude") < -180)
    )

    invalid_rows = regions_df.filter(invalid_conditions)

    if invalid_rows.head(1):
        raise ValueError("Справочник регионов содержит некорректные координаты")


def validate_unique_region_codes(regions_df: DataFrame) -> None:
    """Validate that every region code is unique."""

    regions_count = (
        regions_df
        .groupBy(F.col("region_code"))
        .agg(F.count("*").alias("row_count")) #считаем сколько строк в каждой группе, лучше юзать agg так как точечный контроль, за раз несколько агрегаций + алиас
    )

    invalid_df = regions_count.filter(F.col("row_count") > 1)

    if invalid_df.head(1):
        raise ValueError("Справочник регионов содержит повторяющиеся region_code")


def validate_regions(regions_df: DataFrame) -> None:
    """Validate the complete regional reference dataset."""


    if not regions_df.head(1):
        raise ValueError("Исходный JSON не содержит регионов")

    validate_required_fields(regions_df=regions_df)

    validate_coordinates(regions_df=regions_df)

    validate_unique_region_codes(regions_df=regions_df)


def validate_region_conflicts(
    source_df: DataFrame, 
    target_df: DataFrame,
) -> None:
    """Fail when stored region attributes differ from the JSON source."""

    source = source_df.alias("source")
    target = target_df.alias("target")

    matched_regions = source.join(
        other=target,
        on=F.col("source.region_code") == F.col("target.region_code"), 
        how="inner"
    )

    attributes_differ = (
        ~F.col("source.prefecture_name").eqNullSafe(
            F.col("target.prefecture_name")    #аналог IS DISTINCT, ~ заменяет not, можно было бы != если бы не нул значения (ну в проде так не делают)
        )
        | ~F.col("source.city_name").eqNullSafe(
            F.col("target.city_name")
        )
        | ~F.col("source.latitude").eqNullSafe(
            F.col("target.latitude")
        )
        | ~F.col("source.longitude").eqNullSafe(
            F.col("target.longitude")
        )
    )

    conflicts_df = matched_regions.filter(attributes_differ)

    if conflicts_df.head(1):
        raise ValueError(
            "Данные существующих регионов не совпадают с regions.json"
        )
    

def find_new_regions(
    source_df: DataFrame,
    target_df: DataFrame,
) -> DataFrame:
    """Find source regions that are not yet stored in PostgreSQL."""      

    target_codes = target_df.select("region_code")

    return source_df.join(
        other=target_codes,
        on="region_code",
        how="leftanti"
    )


def validate_loaded_regions(
    source_df: DataFrame,
    target_df: DataFrame,
) -> None:
    """Validate that every source region was loaded without changes."""

    
    validate_region_conflicts(
        source_df=source_df,
        target_df=target_df
    )

    missing_regions_df = find_new_regions(
        source_df=source_df,
        target_df=target_df
    )

    if missing_regions_df.head(1):
        raise ValueError(
            "Не все регионы из regions.json были загружены в "
            "dds_schema.dds_regions"
        )
    


def run_regions_load(
    settings: SparkSettings | None = None,
) -> dict[str, int]:
    """Run the complete regional reference loading workflow."""

    runtime_settings = settings or load_spark_settings()
    spark = create_spark_session(
        app_name="load-dds-regions",
        settings=runtime_settings,
    )

    try:
        regions_df = read_source_regions(spark=spark, path=REGIONS_PATH)

        regions_df = normalize_regions(regions_df=regions_df)
        
        validate_regions(regions_df=regions_df)

        existing_regions_df = read_jdbc(
            spark=spark,
            settings=runtime_settings,
            dbtable=TARGET_TABLE,
        )

        validate_region_conflicts(
            source_df=regions_df,
            target_df=existing_regions_df
        )

        new_regions_df = find_new_regions(
            source_df=regions_df, 
            target_df=existing_regions_df
        )

        new_regions_count = new_regions_df.count()

        if new_regions_count:
            write_jdbc(
                settings=runtime_settings,
                dbtable=TARGET_TABLE,
                df=new_regions_df,
                mode="append"
            )
        loaded_regions_df = read_jdbc(
            spark=spark,
            settings=runtime_settings,
            dbtable=TARGET_TABLE
        )

        validate_loaded_regions(
            source_df=regions_df, 
            target_df=loaded_regions_df
        )

        return {
            "source_regions": regions_df.count(),
            "inserted_regions": new_regions_count,
        }
    finally:
        spark.stop()


def main() -> None:
    """Run the regional reference loader as a standalone Spark job."""

    summary = run_regions_load()
    print(f"Загрузка dds_regions завершена: {summary}")


if __name__ == "__main__":
    main()

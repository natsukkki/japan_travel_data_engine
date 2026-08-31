"""Smoke test for reading PostgreSQL through Spark JDBC."""

from processing.spark.config import load_spark_settings
from processing.spark.jdbc import read_jdbc
from processing.spark.session import create_spark_session


def main() -> None:
    """Run PostgreSQL connection and table-reading smoke checks."""

    settings = load_spark_settings()
    spark = create_spark_session(
        app_name="postgres-jdbc-smoke-test",
        settings=settings,
    )

    try:
        connection_df = read_jdbc(
            spark=spark,
            settings=settings,
            dbtable="(SELECT 1 AS connection_ok) AS connection_test",
        )
        connection_df.show()

        regions_df = read_jdbc(
            spark=spark,
            settings=settings,
            dbtable="dds_schema.dds_regions",
        )
        regions_df.printSchema()
        print(f"DDS regions rows: {regions_df.count()}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

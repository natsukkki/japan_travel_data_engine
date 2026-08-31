"""Shared PostgreSQL JDBC operations for Spark jobs."""

from pyspark.sql import DataFrame, SparkSession

from processing.spark.config import SparkSettings


JDBC_DRIVER = "org.postgresql.Driver"


def read_jdbc(
    spark: SparkSession,
    settings: SparkSettings,
    dbtable: str,
) -> DataFrame:
    """Read a PostgreSQL table or subquery through JDBC."""

    return (
        spark.read
        .format("jdbc")
        .option("url", settings.postgres_jdbc_url)
        .option("dbtable", dbtable)
        .option("user", settings.postgres_user)
        .option("password", settings.postgres_password)
        .option("driver", JDBC_DRIVER)
        .load()
    )


def write_jdbc(
    settings: SparkSettings,
    dbtable: str,
    df: DataFrame,
    mode: str = "append"
) -> None:
    """Write a Spark DataFrame to PostgreSQL through JDBC."""

    (
        df.write
        .format("jdbc")
        .option("url", settings.postgres_jdbc_url)
        .option("dbtable", dbtable)
        .option("user", settings.postgres_user)
        .option("password", settings.postgres_password)
        .option("driver", JDBC_DRIVER)
        .mode(mode)
        .save()
    )
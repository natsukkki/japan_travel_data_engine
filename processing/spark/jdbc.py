"""Shared PostgreSQL JDBC operations for Spark jobs."""
import psycopg
from psycopg import sql
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


def truncate_postgres_table(
    settings: SparkSettings,
    table_name: str,
) -> None:
    """Remove all rows from a PostgreSQL table in one transaction."""

    try:
        schema_name, relation_name = table_name.split(".", maxsplit=1)
    except ValueError as error:
        raise ValueError(
            "Имя таблицы должно иметь формат schema.table"
        ) from error
    
    postgres_url = settings.postgres_jdbc_url.removeprefix("jdbc:")

    with psycopg.connect(
        postgres_url,
        user=settings.postgres_user, 
        password=settings.postgres_password
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                sql.SQL("TRUNCATE TABLE {}").format(
                    sql.Identifier(schema_name, relation_name)
                )
            )


def execute_postgres_transaction(
        settings: SparkSettings, 
        statements: tuple[str, ...]
) -> None: #чисто шаблонная вещь для исполнения несколькиих транзакций, логика погоды в скрипте etl погоды

    postgres_url = settings.postgres_jdbc_url.removeprefix("jdbc:")

    with psycopg.connect(
        postgres_url, 
        user=settings.postgres_user, 
        password=settings.postgres_password
    ) as connection:
        with connection.cursor() as cursor:
            for statement in statements:
                cursor.execute(statement)

        
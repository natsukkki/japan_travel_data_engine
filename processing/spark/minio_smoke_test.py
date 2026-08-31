"""Smoke test for reading one raw weather object from MinIO."""

from processing.spark.config import load_spark_settings
from processing.spark.session import create_spark_session


WEATHER_OBJECT_KEY = (
    "weather/JP-FUKUOKA/2024/01/"
    "2024-01-01-2024-01-31.json"
)


def main() -> None:
    settings = load_spark_settings()
    spark = create_spark_session(
        app_name="minio-weather-smoke-test",
        settings=settings,
    )

    try:
        object_uri = (
            f"s3a://{settings.raw_bucket}/{WEATHER_OBJECT_KEY}"
        )
        weather_df = (
            spark.read
            .option("multiLine", "true")
            .json(object_uri)
        )

        weather_df.printSchema()
        weather_df.select(
            "latitude",
            "longitude",
            "timezone",
        ).show(truncate=False)

        print(f"Raw objects read: {weather_df.count()}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

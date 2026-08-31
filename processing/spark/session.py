"""Shared Spark session configuration."""

from pyspark.sql import SparkSession

from processing.spark.config import SparkSettings


def configure_s3a(
    spark: SparkSession,
    settings: SparkSettings,
) -> None:
    """Configure Hadoop S3A to access the local MinIO service."""

    hadoop_config = spark.sparkContext._jsc.hadoopConfiguration()

    s3a_settings = {
        "fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "fs.s3a.endpoint": settings.minio_endpoint,
        "fs.s3a.endpoint.region": "us-east-1",
        "fs.s3a.path.style.access": "true",
        "fs.s3a.connection.ssl.enabled": str(
            settings.minio_endpoint.startswith("https://")
        ).lower(),
        "fs.s3a.aws.credentials.provider": (
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        ),
        "fs.s3a.access.key": settings.minio_access_key,
        "fs.s3a.secret.key": settings.minio_secret_key,
    }

    for key, value in s3a_settings.items():
        hadoop_config.set(key, value)


def create_spark_session(
    app_name: str,
    settings: SparkSettings,
) -> SparkSession:
    """Create a Spark session configured for MinIO access."""

    spark = (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )

    configure_s3a(spark=spark, settings=settings)

    return spark

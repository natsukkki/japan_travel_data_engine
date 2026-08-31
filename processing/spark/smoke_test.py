from pyspark.sql import SparkSession

def main() -> None:
    spark = (
        SparkSession.builder
        .appName('japan-travel-smoke-test')
        .getOrCreate()
    )

    try:
        regions_df = spark.createDataFrame(
            [
                ("tokyo", "Tokyo"),
                ("osaka", "Osaka"),
            ],
            schema=["region_code", "region_name"],
        )
        regions_df.show()
        regions_df.printSchema()

        print(f"Spark version: {spark.version}")
        print(f"Number of rows: {regions_df.count()}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
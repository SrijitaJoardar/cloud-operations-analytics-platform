from pyspark.sql import SparkSession


def create_spark_session() -> SparkSession:
    """
    Create and return a SparkSession.
    """

    spark = (
        SparkSession.builder
        .appName("Cloud Operations Analytics Platform")
        .master("local[*]")
        .getOrCreate()
    )

    return spark
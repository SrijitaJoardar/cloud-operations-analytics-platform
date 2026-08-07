from pyspark.sql import DataFrame
from pyspark.sql import SparkSession


def extract_cloud_usage_data(
    spark: SparkSession,
    file_path: str,
) -> DataFrame:
    """
    Read cloud telemetry data from CSV.
    """

    dataframe = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(file_path)
    )

    return dataframe
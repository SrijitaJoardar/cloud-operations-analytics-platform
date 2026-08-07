from pyspark.sql import DataFrame


def save_as_parquet(
    dataframe: DataFrame,
    output_path: str,
) -> None:
    """
    Save DataFrame as a Parquet file.
    """

    (
        dataframe.write
        .mode("overwrite")
        .parquet(output_path)
    )
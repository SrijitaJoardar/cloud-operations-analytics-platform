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

def save_rejected_records(
            dataframe: DataFrame,
            output_path: str,
    ) -> None:
        """
        Save rejected data-quality records as Parquet.
        """

        (
            dataframe
            .write
            .mode("overwrite")
            .parquet(output_path)
        )



def save_partitioned_parquet(
    dataframe: DataFrame,
    output_path: str,
    partition_column: str,
) -> None:
    """
    Save DataFrame as partitioned Parquet.
    """

    (
        dataframe.write
        .mode("overwrite")
        .partitionBy(partition_column)
        .parquet(output_path)
    )


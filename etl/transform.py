from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, col, round
from config.constants import RUNNING_STATUS


def select_required_columns(
    dataframe: DataFrame,
) -> DataFrame:
    """
    Select only the required columns for analysis.
    """

    return dataframe.select(
        "date",
        "instance_id",
        "region",
        "status",
        "cpu_usage",
        "memory_usage",
        "daily_cost_usd",
    )


def filter_running_instances(
    dataframe: DataFrame,
) -> DataFrame:
    """
    Filter only running cloud instances.
    """

    return dataframe.filter(
        dataframe.status == RUNNING_STATUS
    )


def count_instances_by_region(
    dataframe: DataFrame,
) -> DataFrame:
    """
    Count running instances by region.
    """

    return (
        dataframe
        .groupBy("region")
        .count()
        .withColumnRenamed(
            "count",
            "instance_count",
        )
        .orderBy(
            col("instance_count").desc()
        )
    )


def average_by_region(
    dataframe: DataFrame,
    column_name: str,
) -> DataFrame:
    """
    Calculate the average of a numeric column by region.
    """

    return (
        dataframe
        .groupBy("region")
        .agg(
            round(
                avg(column_name),
                2,
            ).alias(
                f"average_{column_name}"
            )
        )
        .orderBy(
            col(
                f"average_{column_name}"
            ).desc()
        )
    )

def demonstrate_partitioning(
    dataframe: DataFrame,
) -> None:
    """
    Demonstrate Spark partition management.
    """

    print(
        "\n========== PARTITION INFORMATION =========="
    )

    print(
        "Original partitions:",
        dataframe.rdd.getNumPartitions(),
    )

    repartitioned_dataframe = (
        dataframe.repartition(10)
    )

    print(
        "After repartition(10):",
        repartitioned_dataframe
        .rdd
        .getNumPartitions(),
    )

    coalesced_dataframe = (
        repartitioned_dataframe.coalesce(3)
    )

    print(
        "After coalesce(3):",
        coalesced_dataframe
        .rdd
        .getNumPartitions(),
    )
from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, col, round


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
        dataframe.status == "Running"
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
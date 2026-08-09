from pyspark.sql import DataFrame

from pyspark.sql.functions import (
    col,
    lit,
    when,
)

from config.constants import (
    REGIONS,
    STATUS,
    CPU_USAGE_MIN,
    CPU_USAGE_MAX,
    MEMORY_USAGE_MIN,
    MEMORY_USAGE_MAX,
    RUNNING_HOURS_MIN,
    RUNNING_HOURS_MAX,
    DAILY_COST_MIN,
)

REQUIRED_COLUMNS = [
    "date",
    "instance_id",
    "region",
    "status",
    "cpu_usage",
    "memory_usage",
    "daily_cost_usd",
]





def build_null_condition():
    """
    Build condition for NULL values
    in required columns.
    """

    null_condition = None

    for column_name in REQUIRED_COLUMNS:

        condition = (
            col(column_name).isNull()
        )

        if null_condition is None:
            null_condition = condition

        else:
            null_condition = (
                null_condition | condition
            )

    return null_condition


def build_numeric_invalid_condition():
    """
    Build condition for invalid numeric values.
    """

    return (
            (col("cpu_usage") <= CPU_USAGE_MIN)
            | (col("cpu_usage") >= CPU_USAGE_MAX)
            | (col("memory_usage") <= MEMORY_USAGE_MIN)
            | (col("memory_usage") >= MEMORY_USAGE_MAX)
            | (col("running_hours") < RUNNING_HOURS_MIN)
            | (col("running_hours") > RUNNING_HOURS_MAX)
            | (col("daily_cost_usd") < DAILY_COST_MIN)
    )


def build_categorical_invalid_condition():
    """
    Build condition for invalid categorical values.
    """

    return (
            ~col("status").isin(
                STATUS
            )
            | ~col("region").isin(
        REGIONS
    )
    )


def build_invalid_condition():
    """
    Build the complete data-quality
    invalid-record condition.
    """

    return (
        build_null_condition()
        | build_numeric_invalid_condition()
        | build_categorical_invalid_condition()
    )


def check_null_values(
    dataframe: DataFrame,
) -> DataFrame:
    """
    Identify records containing NULL values
    in required columns.
    """

    return dataframe.filter(
        build_null_condition()
    )


def check_range_values(
    dataframe: DataFrame,
) -> DataFrame:
    """
    Identify records containing invalid
    numeric values based on business rules.
    """

    return dataframe.filter(
        build_numeric_invalid_condition()
    )


def check_allowed_values(
    dataframe: DataFrame,
) -> DataFrame:
    """
    Identify records containing invalid
    categorical values.
    """

    return dataframe.filter(
        build_categorical_invalid_condition()
    )


def check_duplicate_records(
    dataframe: DataFrame,
) -> DataFrame:
    """
    Identify duplicate telemetry records
    based on instance_id and date.
    """

    duplicate_keys = (
        dataframe
        .groupBy(
            "instance_id",
            "date",
        )
        .count()
        .filter(
            col("count") > 1
        )
        .select(
            "instance_id",
            "date",
        )
    )

    return (
        dataframe
        .join(
            duplicate_keys,
            on=[
                "instance_id",
                "date",
            ],
            how="inner",
        )
    )


def split_valid_invalid_records(
    dataframe: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """
    Separate valid and invalid records based on
    all configured data-quality rules.
    """

    invalid_condition = (
        build_invalid_condition()
    )

    invalid_dataframe = (
        dataframe.filter(
            invalid_condition
        )
    )

    valid_dataframe = (
        dataframe.filter(
            ~invalid_condition
        )
    )

    return (
        valid_dataframe,
        invalid_dataframe,
    )


def add_rejection_reason(
    dataframe: DataFrame,
) -> DataFrame:
    """
    Add a rejection_reason column
    to invalid records.
    """

    return (
        dataframe
        .withColumn(
            "rejection_reason",
            when(
                build_numeric_invalid_condition(),
                lit(
                    "INVALID_NUMERIC_VALUE"
                ),
            )
            .when(
                build_categorical_invalid_condition(),
                lit(
                    "INVALID_CATEGORICAL_VALUE"
                ),
            )
            .otherwise(
                lit("UNKNOWN")
            ),
        )
    )
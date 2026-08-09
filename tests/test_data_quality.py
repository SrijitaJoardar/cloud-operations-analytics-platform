
from pyspark.sql import SparkSession

from etl.data_quality import (
    check_null_values,
    check_range_values,
    check_allowed_values,
    check_duplicate_records,
    split_valid_invalid_records,
)


def create_test_spark():
    """
    Create a local Spark session for tests.
    """

    return (
        SparkSession.builder
        .master("local[2]")
        .appName("DataQualityTests")
        .getOrCreate()
    )


def test_check_null_values():
    """
    Verify that NULL values are detected.
    """

    spark = create_test_spark()

    dataframe = spark.createDataFrame(
        [
            (
                "2026-08-01",
                "i-001",
                "us-east-1",
                "Running",
                50.0,
                60.0,
                2.0,
            ),
            (
                None,
                "i-002",
                "us-east-1",
                "Running",
                50.0,
                60.0,
                2.0,
            ),
        ],
        [
            "date",
            "instance_id",
            "region",
            "status",
            "cpu_usage",
            "memory_usage",
            "daily_cost_usd",
        ],
    )

    result = check_null_values(
        dataframe
    ).count()

    assert result == 1

    spark.stop()


def test_check_range_values():
    """
    Verify that invalid numeric values are detected.
    """

    spark = create_test_spark()

    dataframe = spark.createDataFrame(
        [
            (
                "2026-08-01",
                "i-001",
                "us-east-1",
                "Running",
                50.0,
                60.0,
                2.0,
                10.0,
            ),
            (
                "2026-08-01",
                "i-002",
                "us-east-1",
                "Running",
                0.0,
                60.0,
                2.0,
                10.0,
            ),
        ],
        [
            "date",
            "instance_id",
            "region",
            "status",
            "cpu_usage",
            "memory_usage",
            "daily_cost_usd",
            "running_hours",
        ],
    )

    result = check_range_values(
        dataframe
    ).count()

    assert result == 1

    spark.stop()


def test_check_allowed_values():
    """
    Verify that invalid categorical values are detected.
    """

    spark = create_test_spark()

    dataframe = spark.createDataFrame(
        [
            (
                "2026-08-01",
                "i-001",
                "us-east-1",
                "Running",
                50.0,
                60.0,
                2.0,
                10.0,
            ),
            (
                "2026-08-01",
                "i-002",
                "invalid-region",
                "Running",
                50.0,
                60.0,
                2.0,
                10.0,
            ),
        ],
        [
            "date",
            "instance_id",
            "region",
            "status",
            "cpu_usage",
            "memory_usage",
            "daily_cost_usd",
            "running_hours",
        ],
    )

    result = check_allowed_values(
        dataframe
    ).count()

    assert result == 1

    spark.stop()


def test_check_duplicate_records():
    """
    Verify that duplicate instance/date records are detected.
    """

    spark = create_test_spark()

    dataframe = spark.createDataFrame(
        [
            (
                "2026-08-01",
                "i-001",
                "us-east-1",
                "Running",
                50.0,
                60.0,
                2.0,
                10.0,
            ),
            (
                "2026-08-01",
                "i-001",
                "us-east-1",
                "Running",
                55.0,
                65.0,
                2.5,
                11.0,
            ),
        ],
        [
            "date",
            "instance_id",
            "region",
            "status",
            "cpu_usage",
            "memory_usage",
            "daily_cost_usd",
            "running_hours",
        ],
    )

    result = check_duplicate_records(
        dataframe
    ).count()

    assert result == 2

    spark.stop()


def test_split_valid_invalid_records():
    """
    Verify that valid and invalid records
    are separated correctly.
    """

    spark = create_test_spark()

    dataframe = spark.createDataFrame(
        [
            (
                "2026-08-01",
                "i-001",
                "us-east-1",
                "Running",
                50.0,
                60.0,
                2.0,
                10.0,
            ),
            (
                "2026-08-01",
                "i-002",
                "us-east-1",
                "Running",
                0.0,
                60.0,
                2.0,
                10.0,
            ),
        ],
        [
            "date",
            "instance_id",
            "region",
            "status",
            "cpu_usage",
            "memory_usage",
            "daily_cost_usd",
            "running_hours",
        ],
    )

    valid_dataframe, invalid_dataframe = (
        split_valid_invalid_records(
            dataframe
        )
    )

    assert valid_dataframe.count() == 1
    assert invalid_dataframe.count() == 1

    spark.stop()
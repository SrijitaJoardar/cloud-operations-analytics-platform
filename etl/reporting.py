from pyspark.sql import DataFrame


def print_data_quality_report(
    null_count: int,
    invalid_numeric_count: int,
    invalid_categorical_count: int,
    duplicate_count: int,
    valid_count: int,
    invalid_count: int,
) -> None:
    """
    Print the data quality summary.
    """

    print(
        "\n========== DATA QUALITY =========="
    )

    print(
        f"Records with NULL values: "
        f"{null_count}"
    )

    print(
        f"Records with invalid numeric values: "
        f"{invalid_numeric_count}"
    )

    print(
        f"Records with invalid categorical values: "
        f"{invalid_categorical_count}"
    )

    print(
        f"Duplicate records: "
        f"{duplicate_count}"
    )

    print(
        f"Valid records: "
        f"{valid_count}"
    )

    print(
        f"Invalid records: "
        f"{invalid_count}"
    )


def print_spark_sql_report(
    total_records,
    cost_by_project,
    top_expensive_instances,
    cost_by_region,
    infrastructure_summary,
    running_by_region,
    top_instances_by_region,
    top_instances_per_region: int,
) -> None:
    """
    Print Spark SQL analytics reports.
    """

    print("\nTotal Records")

    total_records.show(
        truncate=False,
    )

    print(
        "\n========== SPARK SQL REPORTS =========="
    )

    print(
        "\nDaily Cloud Cost by Project"
    )

    cost_by_project.show(
        truncate=False,
    )

    print(
        "\nTop Expensive Running Instances"
    )

    top_expensive_instances.show(
        truncate=False,
    )

    print(
        "\nDaily Cloud Cost by Region"
    )

    cost_by_region.show(
        truncate=False,
    )

    print(
        "\nRunning Infrastructure Summary"
    )

    infrastructure_summary.show(
        truncate=False,
    )

    print(
        "\nRunning Instances by Region - Spark SQL"
    )

    running_by_region.show(
        truncate=False,
    )

    print(
        f"\nTop {top_instances_per_region} "
        "Most Expensive Running "
        "Instances by Region"
    )

    top_instances_by_region.select(
        "region",
        "instance_id",
        "project_name",
        "instance_type",
        "daily_cost_usd",
        "rank",
    ).show(
        truncate=False,
    )


def print_cloud_operations_report(
    cpu_report,
    memory_report,
    region_counts,
    running_dataframe,
) -> None:
    """
    Print cloud operations reports.
    """

    print(
        "\n========== CLOUD OPERATIONS REPORT =========="
    )

    print(
        "\nAverage CPU Usage by Region"
    )

    cpu_report.show(
        truncate=False,
    )

    print(
        "\nAverage Memory Usage by Region"
    )

    memory_report.show(
        truncate=False,
    )

    print(
        "\nRunning Instances by Region"
    )

    region_counts.show(
        truncate=False,
    )

    print(
        "\nRunning Instances Sample"
    )

    running_dataframe.show(
        5,
        truncate=False,
    )

def print_pipeline_summary(
    total_count: int,
    valid_count: int,
    invalid_count: int,
    duplicate_count: int,
    null_count: int,
) -> None:
    """
    Print the final pipeline summary.
    """

    print(
        "\n========== PIPELINE SUMMARY =========="
    )

    print(
        f"Total records: {total_count}"
    )

    print(
        f"Valid records: {valid_count}"
    )

    print(
        f"Rejected records: {invalid_count}"
    )

    print(
        f"Duplicate records: {duplicate_count}"
    )

    print(
        f"NULL records: {null_count}"
    )

    print(
        "Pipeline status: SUCCESS"
    )

def print_pipeline_performance(
    extract_duration: float,
    quality_duration: float,
    analytics_duration: float,
    transform_duration: float,
    load_duration: float,
    pipeline_duration: float,
) -> None:
    """
    Print pipeline performance metrics.
    """

    print(
        "\n========== PIPELINE PERFORMANCE =========="
    )

    print(
        f"Extract: "
        f"{extract_duration:.2f} seconds"
    )

    print(
        f"Data Quality: "
        f"{quality_duration:.2f} seconds"
    )

    print(
        f"Analytics: "
        f"{analytics_duration:.2f} seconds"
    )

    print(
        f"Transformation: "
        f"{transform_duration:.2f} seconds"
    )

    print(
        f"Load: "
        f"{load_duration:.2f} seconds"
    )

    print(
        f"Total Pipeline: "
        f"{pipeline_duration:.2f} seconds"
    )
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

from etl.extract import extract_cloud_usage_data

from etl.data_quality import (
    check_null_values,
    check_range_values,
    check_allowed_values,
    check_duplicate_records,
    split_valid_invalid_records,
    add_rejection_reason,
)

from etl.transform import (
    select_required_columns,
    filter_running_instances,
    count_instances_by_region,
    average_by_region,
)

from etl.load import (
    save_as_parquet,
    save_rejected_records,
)

from config.settings import (
    TOP_EXPENSIVE_INSTANCES,
    TOP_INSTANCES_PER_REGION,
    PROCESSED_DATA_FILE,
    REJECTED_DATA_FILE,
)


def extract_data(
    spark: SparkSession,
    file_path: str,
) -> DataFrame:
    """
    Extract cloud usage data from the raw CSV file.
    """

    return extract_cloud_usage_data(
        spark,
        file_path,
    )


def validate_data(
    dataframe: DataFrame,
) -> tuple[DataFrame, DataFrame, dict]:
    """
    Perform data quality checks and split the
    dataset into valid and invalid records.
    """

    null_records = check_null_values(
        dataframe,
    )

    range_invalid_records = (
        check_range_values(
            dataframe,
        )
    )

    allowed_value_invalid_records = (
        check_allowed_values(
            dataframe,
        )
    )

    duplicate_records = (
        check_duplicate_records(
            dataframe,
        )
    )

    valid_dataframe, invalid_dataframe = (
        split_valid_invalid_records(
            dataframe,
        )
    )

    valid_dataframe = (
        valid_dataframe.cache()
    )

    invalid_dataframe = (
        add_rejection_reason(
            invalid_dataframe,
        )
    )

    quality_metrics = {
        "null_records": null_records,
        "range_invalid_records":
            range_invalid_records,
        "allowed_value_invalid_records":
            allowed_value_invalid_records,
        "duplicate_records":
            duplicate_records,
    }

    return (
        valid_dataframe,
        invalid_dataframe,
        quality_metrics,
    )


def transform_data(
    valid_dataframe: DataFrame,
) -> tuple[
    DataFrame,
    DataFrame,
    DataFrame,
    DataFrame,
]:
    """
    Transform valid records and generate
    operational reports.
    """

    transformed_dataframe = (
        select_required_columns(
            valid_dataframe,
        )
    )

    running_dataframe = (
        filter_running_instances(
            transformed_dataframe,
        )
    )

    region_counts = (
        count_instances_by_region(
            running_dataframe,
        )
    )

    cpu_report = average_by_region(
        running_dataframe,
        "cpu_usage",
    )

    memory_report = average_by_region(
        running_dataframe,
        "memory_usage",
    )

    return (
        running_dataframe,
        region_counts,
        cpu_report,
        memory_report,
    )


def run_analytics(
    spark: SparkSession,
    dataframe: DataFrame,
) -> dict:
    """
    Run Spark SQL analytics and window-function
    analytics on the cloud usage dataset.
    """

    dataframe.createOrReplaceTempView(
        "cloud_usage"
    )

    # ---------------------------------
    # Total Records
    # ---------------------------------

    total_records = spark.sql(
        """
        SELECT
            COUNT(*) AS total_records
        FROM cloud_usage
        """
    )

    # ---------------------------------
    # Running Instances by Region
    # ---------------------------------

    running_by_region = spark.sql(
        """
        SELECT
            region,
            COUNT(*) AS instance_count
        FROM cloud_usage
        WHERE status = 'Running'
        GROUP BY region
        ORDER BY instance_count DESC
        """
    )

    # ---------------------------------
    # Infrastructure Summary
    # ---------------------------------

    infrastructure_summary = spark.sql(
        """
        SELECT
            ROUND(AVG(cpu_usage), 2)
                AS average_cpu_usage,
            ROUND(AVG(memory_usage), 2)
                AS average_memory_usage,
            ROUND(SUM(daily_cost_usd), 2)
                AS total_daily_cost_usd,
            ROUND(MAX(cpu_usage), 2)
                AS maximum_cpu_usage,
            ROUND(MIN(cpu_usage), 2)
                AS minimum_cpu_usage
        FROM cloud_usage
        WHERE status = 'Running'
        """
    )

    # ---------------------------------
    # Cost by Region
    # ---------------------------------

    cost_by_region = spark.sql(
        """
        SELECT
            region,
            ROUND(SUM(daily_cost_usd), 2)
                AS total_daily_cost_usd
        FROM cloud_usage
        WHERE status = 'Running'
        GROUP BY region
        ORDER BY total_daily_cost_usd DESC
        """
    )

    # ---------------------------------
    # Cost by Project
    # ---------------------------------

    cost_by_project = spark.sql(
        """
        SELECT
            project_name,
            COUNT(*) AS running_instances,
            ROUND(SUM(daily_cost_usd), 2)
                AS total_daily_cost_usd,
            ROUND(AVG(daily_cost_usd), 2)
                AS average_instance_cost_usd
        FROM cloud_usage
        WHERE status = 'Running'
        GROUP BY project_name
        ORDER BY total_daily_cost_usd DESC
        """
    )

    # ---------------------------------
    # Top Expensive Instances
    # ---------------------------------

    top_expensive_instances = spark.sql(
        f"""
        SELECT
            instance_id,
            region,
            project_name,
            instance_type,
            ROUND(daily_cost_usd, 2)
                AS daily_cost_usd
        FROM cloud_usage
        WHERE status = 'Running'
        ORDER BY daily_cost_usd DESC
        LIMIT {TOP_EXPENSIVE_INSTANCES}
        """
    )

    # ---------------------------------
    # Window Function
    # ---------------------------------

    window_spec = Window.partitionBy(
        "region"
    ).orderBy(
        col("daily_cost_usd").desc()
    )

    ranked_instances = (
        dataframe
        .filter(
            col("status") == "Running"
        )
        .withColumn(
            "rank",
            row_number().over(
                window_spec
            ),
        )
    )

    top_instances_by_region = (
        ranked_instances
        .filter(
            col("rank")
            <= TOP_INSTANCES_PER_REGION
        )
        .orderBy(
            col("region"),
            col("rank"),
        )
    )

    return {
        "total_records": total_records,
        "running_by_region": running_by_region,
        "infrastructure_summary":
            infrastructure_summary,
        "cost_by_region":
            cost_by_region,
        "cost_by_project":
            cost_by_project,
        "top_expensive_instances":
            top_expensive_instances,
        "top_instances_by_region":
            top_instances_by_region,
    }


def load_data(
    running_dataframe: DataFrame,
    invalid_dataframe: DataFrame,
) -> None:
    """
    Save valid transformed data and rejected
    records to their configured output locations.
    """

    save_as_parquet(
        running_dataframe,
        str(PROCESSED_DATA_FILE),
    )

    save_rejected_records(
        invalid_dataframe,
        str(REJECTED_DATA_FILE),
    )
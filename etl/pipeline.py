from etl.spark_session import create_spark_session
from etl.extract import extract_cloud_usage_data
from etl.transform import select_required_columns
from etl.transform import filter_running_instances
from etl.transform import count_instances_by_region
from etl.transform import average_by_region
from utils.logger import get_logger
from config.settings import (
    RAW_DATA_FILE,
    PROCESSED_DATA_FILE,
    REJECTED_DATA_FILE,
)

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

from etl.load import (
    save_as_parquet,
    save_rejected_records,
)

from etl.transform import demonstrate_partitioning

from etl.data_quality import (
    check_null_values,
    check_range_values,
    check_allowed_values,
    check_duplicate_records,
    split_valid_invalid_records,
    add_rejection_reason,
)


def main() -> None:
    """
    Main ETL pipeline.
    """

    logger = get_logger(
        __name__
    )

    spark = None

    try:

        # ---------------------------------
        # Create Spark Session
        # ---------------------------------

        spark = create_spark_session()

        logger.info(
            "Spark Session Created Successfully!"
        )

        logger.info(
            "Spark Version: %s",
            spark.version,
        )

        # ---------------------------------
        # Extract
        # ---------------------------------

        logger.info(
            "Starting data extraction."
        )

        dataframe = extract_cloud_usage_data(
            spark,
            str(RAW_DATA_FILE),
        )

        logger.info(
            "Data extraction completed successfully."
        )

        # ---------------------------------
        # Data Quality
        # ---------------------------------

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

        valid_dataframe = valid_dataframe.cache()

        invalid_dataframe = add_rejection_reason(
            invalid_dataframe,
        )

        # ---------------------------------
        # Data Quality Report
        # ---------------------------------

        print(
            "\n========== DATA QUALITY =========="
        )

        null_count = null_records.count()

        invalid_numeric_count = (
            range_invalid_records.count()
        )

        invalid_categorical_count = (
            allowed_value_invalid_records.count()
        )

        duplicate_count = (
            duplicate_records.count()
        )

        valid_count = (
            valid_dataframe.count()
        )

        invalid_count = (
            invalid_dataframe.count()
        )

        total_count = (
                valid_count + invalid_count
        )

        logger.info(
            "Records with NULL values: %s",
            null_count,
        )

        logger.info(
            "Records with invalid numeric values: %s",
            invalid_numeric_count,
        )

        logger.info(
            "Records with invalid categorical values: %s",
            invalid_categorical_count,
        )

        logger.info(
            "Duplicate records: %s",
            duplicate_count,
        )

        logger.info(
            "Valid records: %s",
            valid_count,
        )

        logger.info(
            "Invalid records: %s",
            invalid_count,
        )
        # ---------------------------------
        # Invalid Numeric Records
        # ---------------------------------

        print(
            "\n========== INVALID NUMERIC RECORDS =========="
        )

        range_invalid_records.select(
            "instance_id",
            "region",
            "cpu_usage",
            "memory_usage",
            "running_hours",
            "daily_cost_usd",
        ).show(
            20,
            truncate=False,
        )

        # ---------------------------------
        # Spark SQL Analytics
        # ---------------------------------

        dataframe.createOrReplaceTempView(
            "cloud_usage"
        )

        total_records = spark.sql(
            """
            SELECT
                COUNT(*) AS total_records
            FROM cloud_usage
            """
        )

        print(
            "\nTotal Records"
        )

        total_records.show(
            truncate=False,
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
        # Top 10 Most Expensive Instances
        # ---------------------------------

        top_expensive_instances = spark.sql(
            """
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
            LIMIT 10
            """
        )

        # ---------------------------------
        # Window Function
        # Top 3 Expensive Instances
        # Per Region
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

        top_3_by_region = (
            ranked_instances
            .filter(
                col("rank") <= 3
            )
            .orderBy(
                col("region"),
                col("rank"),
            )
        )

        # ---------------------------------
        # Spark SQL Reports
        # ---------------------------------

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
            "\nTop 10 Most Expensive Running Instances"
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

        # ---------------------------------
        # Window Function Report
        # ---------------------------------

        print(
            "\nTop 3 Most Expensive Running "
            "Instances by Region"
        )

        top_3_by_region.select(
            "region",
            "instance_id",
            "project_name",
            "instance_type",
            "daily_cost_usd",
            "rank",
        ).show(
            truncate=False,
        )

        # ---------------------------------
        # DataFrame Transformations
        # ---------------------------------

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

        # ---------------------------------
        # DataFrame Reports
        # ---------------------------------

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

        demonstrate_partitioning(
            running_dataframe,
        )

        # ---------------------------------
        # Load
        # ---------------------------------

        logger.info(
            "Saving valid data."
        )

        save_as_parquet(
            running_dataframe,
            str(PROCESSED_DATA_FILE),
        )

        logger.info(
            "Valid data saved successfully."
        )

        logger.info(
            "Saving rejected records."
        )



        save_rejected_records(
            invalid_dataframe,
            str(REJECTED_DATA_FILE),
        )
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

        logger.info(
            "Rejected data saved successfully."
        )

        logger.info(
            "Pipeline completed successfully."
        )

    except Exception:
        logger.exception(
            "Pipeline execution failed."
        )

        raise

    finally:

        if spark is not None:

            valid_dataframe.unpersist()

            spark.stop()

            logger.info(
                "Spark session stopped."
            )


if __name__ == "__main__":
    main()
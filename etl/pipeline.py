from etl.spark_session import create_spark_session
from etl.pipeline_context import PipelineContext

from etl.stages import (
    extract_data,
    validate_data,
    run_analytics,
    transform_data,
    load_data,
)

from etl.transform import (
    demonstrate_partitioning,
)

from utils.logger import get_logger

import time

from config.settings import (
    RAW_DATA_FILE,
    TOP_INSTANCES_PER_REGION,
)


def main() -> None:
    """
    Main ETL pipeline orchestrator.
    """

    logger = get_logger(
        __name__
    )

    pipeline_start_time = (
        time.perf_counter()
    )

    spark = None
    context = PipelineContext()

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

        extract_start_time = (
            time.perf_counter()
        )

        context.dataframe = extract_data(
            spark,
            str(RAW_DATA_FILE),
        )


        extract_duration = (
            time.perf_counter()
            - extract_start_time
        )

        logger.info(
            "Extract stage completed in %.2f seconds.",
            extract_duration,
        )

        # ---------------------------------
        # Data Quality
        # ---------------------------------

        quality_start_time = (
            time.perf_counter()
        )

        (
            context.valid_dataframe,
            context.invalid_dataframe,
            context.quality_metrics,
        ) = validate_data(
            context.dataframe,
        )






        quality_duration = (
            time.perf_counter()
            - quality_start_time
        )

        # ---------------------------------
        # Data Quality Metrics
        # ---------------------------------

        null_records = context.quality_metrics[
            "null_records"
        ]

        range_invalid_records = (
            context.quality_metrics[
                "range_invalid_records"
            ]
        )

        allowed_value_invalid_records = (
            context.quality_metrics[
                "allowed_value_invalid_records"
            ]
        )

        duplicate_records = (
            context.quality_metrics[
                "duplicate_records"
            ]
        )

        # ---------------------------------
        # Data Quality Report
        # ---------------------------------

        print(
            "\n========== DATA QUALITY =========="
        )

        null_count = (
            null_records.count()
        )

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
            context.valid_dataframe.count()
        )

        invalid_count = (
            context.invalid_dataframe.count()
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

        logger.info(
            "Data quality stage completed in %.2f seconds.",
            quality_duration,
        )

        # ---------------------------------
        # Analytics
        # ---------------------------------

        analytics_start_time = (
            time.perf_counter()
        )

        context.analytics_results = run_analytics(
            spark,
            context.dataframe,
        )



        analytics_duration = (
            time.perf_counter()
            - analytics_start_time
        )

        logger.info(
            "Analytics stage completed in %.2f seconds.",
            analytics_duration,
        )

        # ---------------------------------
        # Analytics Results
        # ---------------------------------

        total_records = context.analytics_results[
            "total_records"
        ]

        running_by_region = (
            context.analytics_results[
                "running_by_region"
            ]
        )

        infrastructure_summary = (
            context.analytics_results[
                "infrastructure_summary"
            ]
        )

        cost_by_region = (
            context.analytics_results[
                "cost_by_region"
            ]
        )

        cost_by_project = (
            context.analytics_results[
                "cost_by_project"
            ]
        )

        top_expensive_instances = (
            context.analytics_results[
                "top_expensive_instances"
            ]
        )

        top_instances_by_region = (
            context.analytics_results[
                "top_instances_by_region"
            ]
        )

        # ---------------------------------
        # Spark SQL Reports
        # ---------------------------------

        print(
            "\nTotal Records"
        )

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

        # ---------------------------------
        # Window Function Report
        # ---------------------------------

        print(
            f"\nTop {TOP_INSTANCES_PER_REGION} "
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

        # ---------------------------------
        # Transformation
        # ---------------------------------

        transform_start_time = (
            time.perf_counter()
        )

        (
            context.running_dataframe,
            region_counts,
            cpu_report,
            memory_report,
        ) = transform_data(
            context.valid_dataframe,
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

        context.running_dataframe.show(
            5,
            truncate=False,
        )

        demonstrate_partitioning(
            context.running_dataframe,
        )

        transform_duration = (
            time.perf_counter()
            - transform_start_time
        )

        logger.info(
            "Transformation stage completed in %.2f seconds.",
            transform_duration,
        )

        # ---------------------------------
        # Load
        # ---------------------------------

        load_start_time = (
            time.perf_counter()
        )

        load_data(
            context.running_dataframe,
            context.invalid_dataframe,
        )

        load_duration = (
            time.perf_counter()
            - load_start_time
        )

        logger.info(
            "Load stage completed in %.2f seconds.",
            load_duration,
        )

        # ---------------------------------
        # Pipeline Summary
        # ---------------------------------

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

        # ---------------------------------
        # Pipeline Performance
        # ---------------------------------

        pipeline_duration = (
            time.perf_counter()
            - pipeline_start_time
        )

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

        # ---------------------------------
        # Cleanup
        # ---------------------------------

        if context.valid_dataframe is not None:
            context.valid_dataframe.unpersist()

        if spark is not None:
            spark.stop()

            logger.info(
                "Spark session stopped."
            )


if __name__ == "__main__":
    main()
from etl.spark_session import create_spark_session
from etl.pipeline_context import PipelineContext
from etl.reporting import (
    print_data_quality_report,
    print_spark_sql_report,
    print_cloud_operations_report,
    print_pipeline_summary,
    print_pipeline_performance,
)

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

        print_data_quality_report(
            null_count=null_count,
            invalid_numeric_count=invalid_numeric_count,
            invalid_categorical_count=invalid_categorical_count,
            duplicate_count=duplicate_count,
            valid_count=valid_count,
            invalid_count=invalid_count,
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


        print_spark_sql_report(
            total_records=total_records,
            cost_by_project=cost_by_project,
            top_expensive_instances=top_expensive_instances,
            cost_by_region=cost_by_region,
            infrastructure_summary=infrastructure_summary,
            running_by_region=running_by_region,
            top_instances_by_region=top_instances_by_region,
            top_instances_per_region=TOP_INSTANCES_PER_REGION,
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

        print_cloud_operations_report(
            cpu_report=cpu_report,
            memory_report=memory_report,
            region_counts=region_counts,
            running_dataframe=context.running_dataframe,
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

        print_pipeline_summary(
            total_count=total_count,
            valid_count=valid_count,
            invalid_count=invalid_count,
            duplicate_count=duplicate_count,
            null_count=null_count,
        )


        # ---------------------------------
        # Pipeline Performance
        # ---------------------------------

        pipeline_duration = (
            time.perf_counter()
            - pipeline_start_time
        )

        print_pipeline_performance(
            extract_duration=extract_duration,
            quality_duration=quality_duration,
            analytics_duration=analytics_duration,
            transform_duration=transform_duration,
            load_duration=load_duration,
            pipeline_duration=pipeline_duration,
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
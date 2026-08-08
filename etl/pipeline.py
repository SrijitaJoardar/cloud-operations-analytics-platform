from etl.spark_session import create_spark_session
from etl.pipeline_context import PipelineContext
from etl.quality import calculate_quality_counts

from etl.reporting import (
    print_data_quality_report,
    print_spark_sql_report,
    print_cloud_operations_report,
    print_pipeline_summary,
    print_pipeline_performance,
)

from etl.stage_runner import run_stage

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

        context.dataframe, _ = run_stage(
            "Extract",
            extract_data,
            spark,
            str(RAW_DATA_FILE),
            stage_durations=context.stage_durations,
        )

        # ---------------------------------
        # Data Quality
        # ---------------------------------

        (
            (
                context.valid_dataframe,
                context.invalid_dataframe,
                context.quality_metrics,
            ),
            _,
        ) = run_stage(
            "Data Quality",
            validate_data,
            context.dataframe,
            stage_durations=context.stage_durations,
        )

        # ---------------------------------
        # Data Quality Metrics
        # ---------------------------------

        quality_counts = calculate_quality_counts(
            quality_metrics=context.quality_metrics,
            valid_dataframe=context.valid_dataframe,
            invalid_dataframe=context.invalid_dataframe,
        )

        range_invalid_records = (
            context.quality_metrics[
                "range_invalid_records"
            ]
        )

        null_count = quality_counts[
            "null_count"
        ]

        invalid_numeric_count = quality_counts[
            "invalid_numeric_count"
        ]

        invalid_categorical_count = quality_counts[
            "invalid_categorical_count"
        ]

        duplicate_count = quality_counts[
            "duplicate_count"
        ]

        valid_count = quality_counts[
            "valid_count"
        ]

        invalid_count = quality_counts[
            "invalid_count"
        ]

        total_count = quality_counts[
            "total_count"
        ]

        # ---------------------------------
        # Data Quality Report
        # ---------------------------------

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

        # ---------------------------------
        # Analytics
        # ---------------------------------

        (
            context.analytics_results,
            _,
        ) = run_stage(
            "Analytics",
            run_analytics,
            spark,
            context.dataframe,
            stage_durations=context.stage_durations,
        )

        # ---------------------------------
        # Spark SQL Reports
        # ---------------------------------

        print_spark_sql_report(
            total_records=context.analytics_results[
                "total_records"
            ],
            cost_by_project=context.analytics_results[
                "cost_by_project"
            ],
            top_expensive_instances=context.analytics_results[
                "top_expensive_instances"
            ],
            cost_by_region=context.analytics_results[
                "cost_by_region"
            ],
            infrastructure_summary=context.analytics_results[
                "infrastructure_summary"
            ],
            running_by_region=context.analytics_results[
                "running_by_region"
            ],
            top_instances_by_region=context.analytics_results[
                "top_instances_by_region"
            ],
            top_instances_per_region=TOP_INSTANCES_PER_REGION,
        )

        # ---------------------------------
        # Transformation
        # ---------------------------------

        (
            (
                context.running_dataframe,
                region_counts,
                cpu_report,
                memory_report,
            ),
            _,
        ) = run_stage(
            "Transformation",
            transform_data,
            context.valid_dataframe,
            stage_durations=context.stage_durations,
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

        # ---------------------------------
        # Load
        # ---------------------------------

        (
            _,
            _,
        ) = run_stage(
            "Load",
            load_data,
            context.running_dataframe,
            context.invalid_dataframe,
            stage_durations=context.stage_durations,
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
            stage_durations=context.stage_durations,
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
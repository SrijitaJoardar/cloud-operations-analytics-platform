from etl.spark_session import create_spark_session
from etl.extract import extract_cloud_usage_data
from etl.transform import select_required_columns
from etl.transform import filter_running_instances
from etl.transform import count_instances_by_region
from etl.transform import average_by_region
from etl.load import save_as_parquet
from config.settings import RAW_DATA_FILE
from config.settings import PROCESSED_DATA_FILE









def main() -> None:
    """
    Main ETL pipeline.
    """

    # ---------------------------------
    # Create Spark Session
    # ---------------------------------

    spark = create_spark_session()

    print("Spark Session Created Successfully!")
    print(f"Spark Version: {spark.version}")

    # ---------------------------------
    # Extract
    # ---------------------------------

    dataframe = extract_cloud_usage_data(
        spark,
        str(RAW_DATA_FILE),
    )

    # ---------------------------------
    # Transform
    # ---------------------------------

    transformed_dataframe = select_required_columns(
        dataframe,
    )

    running_dataframe = filter_running_instances(
        transformed_dataframe,
    )

    region_counts = count_instances_by_region(
        running_dataframe,
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
    # Reports
    # ---------------------------------

    print("\n========== CLOUD OPERATIONS REPORT ==========")

    print("\nAverage CPU Usage by Region")

    cpu_report.show(
        truncate=False,
    )

    print("\nAverage Memory Usage by Region")

    memory_report.show(
        truncate=False,
    )

    print("\nRunning Instances by Region")

    region_counts.show(
        truncate=False,
    )

    print("\nRunning Instances Sample")

    running_dataframe.show(
        5,
        truncate=False,
    )

    # ---------------------------------
    # Load
    # ---------------------------------

    save_as_parquet(
        running_dataframe,
        str(PROCESSED_DATA_FILE),
    )

    print("\nParquet file saved successfully!")

    spark.stop()


if __name__ == "__main__":
    main()
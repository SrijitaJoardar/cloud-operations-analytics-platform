import pandas as pd


from config.constants import (
    CPU_USAGE_MIN,
    CPU_USAGE_MAX,
    MEMORY_USAGE_MIN,
    MEMORY_USAGE_MAX,
    RUNNING_HOURS_MIN,
    RUNNING_HOURS_MAX,
    DAILY_COST_MIN,
    DAILY_COST_MAX,
)


class DataValidator:
    """
    Validate generated cloud telemetry data.
    """

    def __init__(self, dataframe: pd.DataFrame):
        self.dataframe = dataframe

    def check_null_values(self) -> None:
        """
        Check for missing values in the dataset.
        """

        null_counts = self.dataframe.isnull().sum()

        print("\nNull Value Report")

        print(null_counts)

    def check_duplicate_records(self) -> None:
        """
        Check for duplicate records based on
        date and instance ID.
        """

        duplicate_count = self.dataframe.duplicated(
            subset=["date", "instance_id"]
        ).sum()

        print("\nDuplicate Record Report")

        print(
            f"Duplicate records: {duplicate_count}"
        )

    def validate_range(
            self,
            column_name: str,
            minimum: float,
            maximum: float,
    ) -> None:
        """
        Validate that all values in a column fall
        within the specified range.
        """

        invalid_records = self.dataframe[
            (self.dataframe[column_name] < minimum)
            | (self.dataframe[column_name] > maximum)
            ]

        print(f"\n{column_name} Validation")

        print(
            f"Invalid records: {len(invalid_records)}"
        )

    def validate_cpu_usage(self) -> None:
        """
        Validate CPU usage.
        """

        self.validate_range(
            "cpu_usage",
            CPU_USAGE_MIN,
            CPU_USAGE_MAX,
        )

    def validate_all_ranges(self) -> None:
        """
        Validate all numeric columns.
        """

        self.validate_range(
            "cpu_usage",
            0,
            100,
        )

        self.validate_range(
            "memory_usage",
            0,
            100,
        )

        self.validate_range(
            "running_hours",
            0,
            24,
        )

        self.validate_range(
            "daily_cost_usd",
            0,
            1000,
        )
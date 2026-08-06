import pandas as pd


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
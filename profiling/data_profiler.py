import pandas as pd


class DataProfiler:
    """
   Generate summary statistics for cloud telemetry data.
    """

    def __init__(
        self,
        dataframe: pd.DataFrame,
    ):
        self.dataframe = dataframe

    def get_total_records(self) -> int:
        """
        Return the total number of records.
        """
        return len(self.dataframe)

    def get_unique_count(
        self,
        column_name: str,
    ) -> int:
        """
        Return the number of unique values
        in a column.
        """
        return self.dataframe[
            column_name
        ].nunique()

    def get_average(
            self,
            column_name: str,
    ) -> float:
        """
        Return the average value
        of a numeric column.
        """

        return round(
            self.dataframe[column_name].mean(),
            2,
        )

    def get_value_counts(
            self,
            column_name: str,
    ) -> pd.Series:
        """
        Return the frequency of each value
        in a column.
        """

        return self.dataframe[
            column_name
        ].value_counts()

    def generate_report(self) -> None:
        """
        Display the profiling report.
        """

        print("\n========== DATA PROFILE ==========")

        print(
            f"Total Records : {self.get_total_records()}"
        )

        print(
            f"Unique Regions : {self.get_unique_count('region')}"
        )

        print(
            f"Unique Services : {self.get_unique_count('service_name')}"
        )

        print(
            f"Unique Projects : {self.get_unique_count('project_name')}"
        )

        print(
            f"Unique Teams : {self.get_unique_count('team_name')}"
        )
        print()

        print(
            f"Average CPU Usage : {self.get_average('cpu_usage')}%"
        )

        print(
            f"Average Memory Usage : {self.get_average('memory_usage')}%"
        )
        print()

        print("Status Distribution")
        print("-------------------")

        status_counts = self.get_value_counts(
            "status"
        )

        for status, count in status_counts.items():
            print(
                f"{status:<15}: {count}"
            )

        print("\n=================================")
from pyspark.sql import DataFrame


def calculate_quality_counts(
    quality_metrics: dict[str, DataFrame],
    valid_dataframe: DataFrame,
    invalid_dataframe: DataFrame,
) -> dict[str, int]:
    """
    Calculate data quality record counts.
    """

    null_count = quality_metrics[
        "null_records"
    ].count()

    invalid_numeric_count = quality_metrics[
        "range_invalid_records"
    ].count()

    invalid_categorical_count = quality_metrics[
        "allowed_value_invalid_records"
    ].count()

    duplicate_count = quality_metrics[
        "duplicate_records"
    ].count()

    valid_count = valid_dataframe.count()

    invalid_count = invalid_dataframe.count()

    total_count = (
        valid_count + invalid_count
    )

    return {
        "null_count": null_count,
        "invalid_numeric_count": invalid_numeric_count,
        "invalid_categorical_count": invalid_categorical_count,
        "duplicate_count": duplicate_count,
        "valid_count": valid_count,
        "invalid_count": invalid_count,
        "total_count": total_count,
    }
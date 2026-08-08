from dataclasses import dataclass
from typing import Any

from pyspark.sql import DataFrame


@dataclass
class PipelineContext:
    """
    Stores data produced by different pipeline stages.
    """

    dataframe: DataFrame | None = None

    valid_dataframe: DataFrame | None = None

    invalid_dataframe: DataFrame | None = None

    quality_metrics: dict[str, Any] | None = None

    analytics_results: dict[str, DataFrame] | None = None

    running_dataframe: DataFrame | None = None
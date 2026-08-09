from pathlib import Path


# ---------------------------------
# Project Paths
# ---------------------------------

PROJECT_ROOT = (
    Path(__file__)
    .resolve()
    .parent
    .parent
)

DATA_DIR = (
    PROJECT_ROOT
    / "data"
)

RAW_DATA_DIR = (
    DATA_DIR
    / "raw"
)

PROCESSED_DATA_DIR = (
    DATA_DIR
    / "processed"
)


# ---------------------------------
# Input / Output Files
# ---------------------------------

RAW_DATA_FILE = (
    RAW_DATA_DIR
    / "cloud_usage.csv"
)

PROCESSED_DATA_FILE = (
    PROCESSED_DATA_DIR
    / "running_instances.parquet"
)

REJECTED_DATA_FILE = (
    PROCESSED_DATA_DIR
    / "rejected_records.parquet"
)


# ---------------------------------
# Data Generation Configuration
# ---------------------------------

NUMBER_OF_RECORDS = 10_000

NUMBER_OF_INSTANCES = 500


# ---------------------------------
# Analytics Configuration
# ---------------------------------

TOP_EXPENSIVE_INSTANCES = 10

TOP_INSTANCES_PER_REGION = 3


# ---------------------------------
# Transformation Configuration
# ---------------------------------

TARGET_PARTITIONS = 10


# ---------------------------------
# Configuration Validation
# ---------------------------------


def validate_configuration() -> None:
    """
    Validate pipeline configuration values.
    """

    if NUMBER_OF_RECORDS <= 0:
        raise ValueError(
            "NUMBER_OF_RECORDS must be greater than 0."
        )

    if NUMBER_OF_INSTANCES <= 0:
        raise ValueError(
            "NUMBER_OF_INSTANCES must be greater than 0."
        )

    if TOP_EXPENSIVE_INSTANCES <= 0:
        raise ValueError(
            "TOP_EXPENSIVE_INSTANCES must be greater than 0."
        )

    if TOP_INSTANCES_PER_REGION <= 0:
        raise ValueError(
            "TOP_INSTANCES_PER_REGION must be greater than 0."
        )

    if TARGET_PARTITIONS <= 0:
        raise ValueError(
            "TARGET_PARTITIONS must be greater than 0."
        )


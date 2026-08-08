from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

DATA_DIR = PROJECT_ROOT / "data"

RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"

RAW_DATA_FILE = (
    RAW_DATA_DIR
    / "cloud_usage.csv"
)
PROCESSED_DATA_FILE = (
    PROCESSED_DATA_DIR
    / "running_instances.parquet"
)

REJECTED_DATA_FILE = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "rejected_records.parquet"
)

NUMBER_OF_RECORDS = 10_000
NUMBER_OF_INSTANCES = 500
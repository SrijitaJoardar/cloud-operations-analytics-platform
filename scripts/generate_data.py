from pathlib import Path
from validation.data_validator import DataValidator
import logging


from datetime import datetime, timedelta


from random import choice, randint, uniform,seed

import pandas as pd
from faker import Faker

from config.settings import (
    NUMBER_OF_INSTANCES,
    NUMBER_OF_RECORDS,
    RAW_DATA_FILE,
)
from config.constants import (
    ENVIRONMENTS,
    INSTANCE_TYPES,
    PROJECT_NAMES,
    REGIONS,
    SERVICES,
    STATUS,
    TEAM_NAMES,
    CLOUD_PROVIDERS,
    ACCOUNT_IDS,
    REGION_TO_AVAILABILITY_ZONES,
    INSTANCE_TYPE_HOURLY_COST,
)



faker = Faker()
Faker.seed(42)
seed(42)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)

def generate_dates(number_of_records: int) -> list[str]:
    """
    Generate random dates within the last 365 days.
    """
    validate_record_count(number_of_records)
    dates = []
    today = datetime.now()

    for _ in range(number_of_records):
        days_ago = randint(0, 364)
        random_date = today - timedelta(days=days_ago)
        dates.append(random_date.strftime("%Y-%m-%d"))

    return dates

def generate_random_values(
    values: list[str],
    number_of_records: int,
) -> list[str]:
    """
    Generate random values from a predefined list.
    """
    validate_record_count(number_of_records)
    return [
        choice(values)
        for _ in range(number_of_records)
    ]
def validate_record_count(number_of_records: int) -> None:
    """
    Validate the requested number of records.
    """

    if number_of_records <= 0:
        raise ValueError(
            "number_of_records must be greater than zero."
        )

def generate_instance_ids(number_of_instances: int) -> list[str]:
    """
    Generate unique AWS-style EC2 instance IDs.
    """
    validate_record_count(number_of_instances)
    instance_ids = []

    for _ in range(number_of_instances):
        random_hex = faker.hexify(text="^^^^^^^^^^^^^^^^^", upper=False)
        instance_ids.append(f"i-{random_hex}")

    return instance_ids


def generate_unique_record_keys(
    number_of_records: int,
    instance_pool: list[str],
) -> list[tuple[str, str]]:
    """
    Generate unique (date, instance_id) pairs.
    """

    used_pairs = set()

    record_keys = []

    while len(record_keys) < number_of_records:

        days_ago = randint(0, 364)

        random_date = (
            datetime.now() - timedelta(days=days_ago)
        ).strftime("%Y-%m-%d")

        instance_id = choice(instance_pool)

        record_key = (
            random_date,
            instance_id,
        )

        if record_key not in used_pairs:
            used_pairs.add(record_key)
            record_keys.append(record_key)

    return record_keys

def assign_instance_ids(
    instance_ids: list[str],
    number_of_records: int,
) -> list[str]:
    """
    Assign existing instance IDs to telemetry records.
    """

    assigned_instances = []

    for _ in range(number_of_records):
        assigned_instances.append(choice(instance_ids))

    return assigned_instances

def generate_services(number_of_records: int) -> list[str]:
    """
    Generate random AWS services.
    """

    return generate_random_values(
        SERVICES,
        number_of_records,
    )

# def generate_regions(number_of_records: int) -> list[str]:
#     """
#     Generate random AWS regions.
#     """
#
#     regions = []
#
#     for _ in range(number_of_records):
#         regions.append(choice(REGIONS))
#
#     return regions

def generate_regions(number_of_records: int) -> list[str]:
    """
    Generate random AWS regions.
    """

    return generate_random_values(
        REGIONS,
        number_of_records,
    )

def generate_environments(number_of_records: int) -> list[str]:
    """
    Generate random deployment environments.
    """

    return generate_random_values(
        ENVIRONMENTS,
        number_of_records,
    )

def generate_project_names(number_of_records: int) -> list[str]:
    """
    Generate random project names.
    """

    return generate_random_values(
        PROJECT_NAMES,
        number_of_records,
    )

def generate_team_names(number_of_records: int) -> list[str]:
    """
    Generate random team names.
    """

    return generate_random_values(
        TEAM_NAMES,
        number_of_records,
    )
def generate_cloud_providers(number_of_records: int) -> list[str]:
    """
    Generate cloud providers.
    """

    return generate_random_values(
        CLOUD_PROVIDERS,
        number_of_records,
    )

def generate_account_ids(number_of_records: int) -> list[str]:
    """
    Generate AWS account IDs.
    """

    return generate_random_values(
        ACCOUNT_IDS,
        number_of_records,
    )
def generate_instance_types(number_of_records: int) -> list[str]:
    """
    Generate AWS EC2 instance types.
    """

    return generate_random_values(
        INSTANCE_TYPES,
        number_of_records,
    )

def generate_availability_zones(
    regions: list[str],
) -> list[str]:
    """
    Generate availability zones based on AWS regions.
    """

    availability_zones = []

    for region in regions:
        zones = REGION_TO_AVAILABILITY_ZONES[region]
        availability_zones.append(choice(zones))

    return availability_zones


def generate_status(number_of_records: int) -> list[str]:
    """
    Generate EC2 instance status.
    """

    return generate_random_values(
        STATUS,
        number_of_records,
    )

def generate_cpu_usage(
    statuses: list[str],
) -> list[float]:
    """
    Generate CPU usage based on instance status.
    """

    cpu_usage = []

    for status in statuses:
        if status == "Running":
            cpu_usage.append(round(uniform(10, 95), 2))
        else:
            cpu_usage.append(round(uniform(0, 2), 2))

    return cpu_usage


def generate_memory_usage(
    statuses: list[str],
) -> list[float]:
    """
    Generate memory usage based on instance status.
    """

    memory_usage = []

    for status in statuses:
        if status == "Running":
            memory_usage.append(round(uniform(20, 90), 2))
        else:
            memory_usage.append(round(uniform(0, 2), 2))

    return memory_usage


def generate_storage_usage(
    number_of_records: int,
) -> list[float]:
    """
    Generate storage usage in GB.
    """

    validate_record_count(number_of_records)

    storage_usage = []

    for _ in range(number_of_records):
        storage_usage.append(round(uniform(20, 900), 2))

    return storage_usage

def generate_network_in(
    statuses: list[str],
) -> list[float]:
    """
    Generate inbound network traffic in MB.
    """

    network_in = []

    for status in statuses:
        if status == "Running":
            network_in.append(round(uniform(100, 5000), 2))
        else:
            network_in.append(round(uniform(0, 5), 2))

    return network_in

def generate_network_out(
    statuses: list[str],
) -> list[float]:
    """
    Generate outbound network traffic in MB.
    """

    network_out = []

    for status in statuses:
        if status == "Running":
            network_out.append(round(uniform(100, 5000), 2))
        else:
            network_out.append(round(uniform(0, 5), 2))

    return network_out


def generate_running_hours(
    statuses: list[str],
) -> list[float]:
    """
    Generate daily running hours based on instance status.
    """

    running_hours = []

    for status in statuses:
        if status == "Running":
            running_hours.append(round(uniform(8, 24), 2))
        else:
            running_hours.append(round(uniform(0, 2), 2))

    return running_hours


def generate_daily_cost(
    instance_types: list[str],
    running_hours: list[float],
    statuses: list[str],
) -> list[float]:
    """
    Generate daily infrastructure cost in USD.
    """

    daily_cost = []

    for instance_type, hours, status in zip(
        instance_types,
        running_hours,
        statuses,
    ):
        hourly_rate = INSTANCE_TYPE_HOURLY_COST[instance_type]

        if status == "Running":
            cost = hourly_rate * hours
        else:
            cost = 0.10

        daily_cost.append(round(cost, 2))

    return daily_cost

def main() -> None:
    """
    Main function to generate synthetic cloud telemetry data.
    """
    logger.info("Starting synthetic cloud telemetry generation...")

    try:
        instance_pool = generate_instance_ids(
            NUMBER_OF_INSTANCES,
        )

        record_keys = generate_unique_record_keys(
            NUMBER_OF_RECORDS,
            instance_pool,
        )

        dates = []
        instance_ids = []

        for date, instance_id in record_keys:
            dates.append(date)
            instance_ids.append(instance_id)

        cloud_providers = generate_cloud_providers(
            NUMBER_OF_RECORDS,
        )

        account_ids = generate_account_ids(
            NUMBER_OF_RECORDS,
        )

        instance_types = generate_instance_types(
            NUMBER_OF_RECORDS,
        )

        services = generate_services(
            NUMBER_OF_RECORDS,
        )

        regions = generate_regions(
            NUMBER_OF_RECORDS,
        )

        availability_zones = generate_availability_zones(
            regions,
        )

        statuses = generate_status(
            NUMBER_OF_RECORDS,
        )

        cpu_usage = generate_cpu_usage(
            statuses,
        )

        memory_usage = generate_memory_usage(
            statuses,
        )

        storage_usage = generate_storage_usage(
            NUMBER_OF_RECORDS,
        )

        network_in = generate_network_in(
            statuses,
        )

        network_out = generate_network_out(
            statuses,
        )

        running_hours = generate_running_hours(
            statuses,
        )

        daily_cost = generate_daily_cost(
            instance_types,
            running_hours,
            statuses,
        )

        environments = generate_environments(
            NUMBER_OF_RECORDS,
        )

        projects = generate_project_names(
            NUMBER_OF_RECORDS,
        )

        teams = generate_team_names(
            NUMBER_OF_RECORDS,
        )

        df = pd.DataFrame(
            {
                "date": dates,
                "cloud_provider": cloud_providers,
                "account_id": account_ids,
                "instance_id": instance_ids,
                "instance_type": instance_types,
                "service_name": services,
                "region": regions,
                "availability_zone": availability_zones,
                "status": statuses,
                "cpu_usage": cpu_usage,
                "memory_usage": memory_usage,
                "storage_usage_gb": storage_usage,
                "network_in_mb": network_in,
                "network_out_mb": network_out,
                "running_hours": running_hours,
                "daily_cost_usd": daily_cost,
                "environment": environments,
                "project_name": projects,
                "team_name": teams,
            }
        )

        logger.info(
            "Successfully created DataFrame with %d records.",
            len(df),
        )

        Path(RAW_DATA_FILE).parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        df.to_csv(
            RAW_DATA_FILE,
            index=False,
        )

        logger.info(
            "Dataset saved successfully to %s",
            RAW_DATA_FILE,
        )

        validator = DataValidator(df)
        validator.check_null_values()
        validator.check_duplicate_records()

        print(df.head())

    except Exception as error:
        logger.exception(
            "Synthetic data generation failed: %s",
            error,
        )
if __name__ == "__main__":
    main()

from datetime import datetime, timedelta
from random import choice, randint, uniform

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
)



faker = Faker()

def generate_dates(number_of_records: int) -> list[str]:
    """
    Generate random dates within the last 365 days.
    """

    dates = []
    today = datetime.now()

    for _ in range(number_of_records):
        days_ago = randint(0, 364)
        random_date = today - timedelta(days=days_ago)
        dates.append(random_date.strftime("%Y-%m-%d"))

    return dates

def generate_instance_ids(number_of_instances: int) -> list[str]:
    """
    Generate unique AWS-style EC2 instance IDs.
    """

    instance_ids = []

    for _ in range(number_of_instances):
        random_hex = faker.hexify(text="^^^^^^^^^^^^^^^^^", upper=False)
        instance_ids.append(f"i-{random_hex}")

    return instance_ids

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

    services = []

    for _ in range(number_of_records):
        services.append(choice(SERVICES))

    return services

def generate_regions(number_of_records: int) -> list[str]:
    """
    Generate random AWS regions.
    """

    regions = []

    for _ in range(number_of_records):
        regions.append(choice(REGIONS))

    return regions

def generate_environments(number_of_records: int) -> list[str]:
    """
    Generate random deployment environments.
    """

    environments = []

    for _ in range(number_of_records):
        environments.append(choice(ENVIRONMENTS))

    return environments

def generate_project_names(number_of_records: int) -> list[str]:
    """
    Generate random project names.
    """

    projects = []

    for _ in range(number_of_records):
        projects.append(choice(PROJECT_NAMES))

    return projects

def generate_team_names(number_of_records: int) -> list[str]:
    """
    Generate random team names.
    """

    teams = []

    for _ in range(number_of_records):
        teams.append(choice(TEAM_NAMES))

    return teams


def main() -> None:
    """
    Main function to generate synthetic cloud telemetry data.
    """

    dates = generate_dates(NUMBER_OF_RECORDS)
    services = generate_services(NUMBER_OF_RECORDS)
    regions = generate_regions(NUMBER_OF_RECORDS)
    environments = generate_environments(NUMBER_OF_RECORDS)
    projects = generate_project_names(NUMBER_OF_RECORDS)
    teams = generate_team_names(NUMBER_OF_RECORDS)
    instance_pool = generate_instance_ids(NUMBER_OF_INSTANCES)

    instance_ids = assign_instance_ids(
        instance_pool,
        NUMBER_OF_RECORDS,
    )


    df = pd.DataFrame(
        {
            "date": dates,
            "instance_id": instance_ids,
            "service_name": services,
            "region": regions,
            "environment": environments,
            "project_name": projects,
            "team_name": teams,
        }
    )
    print(df.head())

if __name__ == "__main__":
    main()

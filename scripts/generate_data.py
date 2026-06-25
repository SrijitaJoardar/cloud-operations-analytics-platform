from datetime import datetime, timedelta
from random import choice, randint, uniform

import pandas as pd
from faker import Faker

from config.settings import NUMBER_OF_RECORDS, RAW_DATA_FILE
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

if __name__ == "__main__":
    dates = generate_dates(5)

    for date in dates:
        print(date)
"""Generate distribution-center reference records.

The module creates warehouse-like locations with geographic attributes,
time-zone assignments, and capacity values. Location fields are sampled from
GeoNames postal-code records.
"""

import logging
import random

import pandas as pd
from faker import Faker

import data_generator.config as config
from data_generator.constants import REGIONS, TIME_ZONES
from data_generator.modules.geonames import load_us_zip_records

fake = Faker()


def generate_distribution_centers():
    """Build distribution-center rows with location and operational attributes.

    Returns:
        pd.DataFrame: Distribution-center table including IDs, names, address
        fields, region, time zone, and capacity.
    """
    logger = logging.getLogger(__name__)
    total = config.N_DCS
    logger.info("Generating %d distribution centers", total)
    rows = []
    geo_records = load_us_zip_records()
    for dc_id in range(1, total + 1):
        city, state, zip_code = random.choice(geo_records)
        region = random.choice(REGIONS)

        tz = "Central"
        for tz_name, states in TIME_ZONES.items():
            if state in states:
                tz = tz_name
                break

        rows.append(
            {
                "dc_id": dc_id,
                "dc_name": f"DC {dc_id} {city}",
                "address_line1": fake.street_address(),
                "city": city,
                "state": state,
                "zip": zip_code,
                "region": region,
                "time_zone": tz,
                "capacity_units": random.randint(50000, 500000),
            }
        )
    logger.info("Generated %d distribution centers", total)
    return pd.DataFrame(rows)

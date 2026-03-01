"""Generate customer master and sub-account records.

This module creates a mixed customer population with account hierarchy,
location attributes, segment labels, credit limits, and discount percentages.
"""

import logging
import random
from datetime import timedelta

import numpy as np
import pandas as pd
from faker import Faker

import data_generator.config as config
from data_generator.config import START_DATE
from data_generator.constants import REGIONS
from data_generator.modules.geonames import load_us_zip_records

fake = Faker()


def random_past_date():
    """Return a random account-open date prior to the configured start period.

    Returns:
        datetime.date: Historical date used for `account_open_date`.
    """
    return (START_DATE - timedelta(days=random.randint(365, 365 * 5))).date()


def generate_customers():
    """Generate customer masters and sub-accounts with commercial attributes.

    Returns:
        pd.DataFrame: Customer table containing account identifiers,
        hierarchy fields, address details, segment labels, credit limits,
        discounts, and account-open dates.
    """
    logger = logging.getLogger(__name__)
    rows = []
    geo_records = load_us_zip_records()
    master_count = min(6000, config.N_CUSTOMERS)
    master_ids = list(range(1, master_count + 1))
    sub_ids = list(range(master_count + 1, config.N_CUSTOMERS + 1))

    segments = ["Dealer", "Retailer", "Corporate", "Education", "Government"]

    logger.info("Generating %d master customers", len(master_ids))
    log_interval = min(config.LOG_INTERVAL, len(master_ids)) if master_ids else config.LOG_INTERVAL
    for idx, cid in enumerate(master_ids, start=1):
        city, state, zip_code = random.choice(geo_records)
        rows.append(
            {
                "customer_id": cid,
                "master_account_number": f"M{cid:06d}",
                "sub_account_number": None,
                "customer_name": fake.company(),
                "segment": random.choice(segments),
                "address_line1": fake.street_address(),
                "city": city,
                "state": state,
                "zip": zip_code,
                "region": random.choice(REGIONS),
                "credit_limit": round(np.random.uniform(5000, 250000), 2),
                "discount_percentage": round(
                    np.random.uniform(config.DISCOUNT_PCT_MIN, config.DISCOUNT_PCT_MAX),
                    2,
                ),
                "account_open_date": random_past_date(),
            }
        )
        if idx % log_interval == 0:
            logger.info("Generated %d / %d master customers", idx, len(master_ids))

    logger.info("Generating %d sub customers", len(sub_ids))
    log_interval = min(config.LOG_INTERVAL, len(sub_ids)) if sub_ids else config.LOG_INTERVAL
    for idx, cid in enumerate(sub_ids, start=1):
        master = random.choice(master_ids)
        city, state, zip_code = random.choice(geo_records)
        rows.append(
            {
                "customer_id": cid,
                "master_account_number": f"M{master:06d}",
                "sub_account_number": f"S{cid:06d}",
                "customer_name": fake.company(),
                "segment": random.choice(segments),
                "address_line1": fake.street_address(),
                "city": city,
                "state": state,
                "zip": zip_code,
                "region": random.choice(REGIONS),
                "credit_limit": round(np.random.uniform(5000, 250000), 2),
                "discount_percentage": round(
                    np.random.uniform(config.DISCOUNT_PCT_MIN, config.DISCOUNT_PCT_MAX),
                    2,
                ),
                "account_open_date": random_past_date(),
            }
        )
        if idx % log_interval == 0:
            logger.info("Generated %d / %d sub customers", idx, len(sub_ids))

    logger.info("Generated %d customers", len(rows))
    return pd.DataFrame(rows)

"""Generate vendor master records used by item and inventory tables.

The module creates synthetic supplier records with stable IDs, readable names,
and compact vendor codes for downstream joins and lookups.
"""

import logging
import random
import string

import pandas as pd
from faker import Faker

import data_generator.config as config

fake = Faker()


def generate_vendors():
    """Create vendor records with IDs, names, and unique short codes.

    Returns:
        pd.DataFrame: Vendor table containing `vendor_id`, `vendor_name`, and
        `vendor_code`.
    """
    logger = logging.getLogger(__name__)
    total = config.N_VENDORS
    log_interval = min(config.LOG_INTERVAL, total) if total else config.LOG_INTERVAL
    logger.info("Generating %d vendors", total)
    rows = []
    used_codes = set()
    for vid in range(1, total + 1):
        while True:
            code = "".join(random.choices(string.ascii_uppercase, k=4))
            if code not in used_codes:
                used_codes.add(code)
                break

        rows.append(
            {
                "vendor_id": vid,
                "vendor_name": f"{fake.company()} Supply Co.",
                "vendor_code": code,
            }
        )
        if vid % log_interval == 0:
            logger.info("Generated %d / %d vendors", vid, total)
    logger.info("Generated %d vendors", total)
    return pd.DataFrame(rows)

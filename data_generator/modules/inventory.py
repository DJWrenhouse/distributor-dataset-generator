"""Generate inventory snapshots across distribution centers.

The module creates per-DC item inventory rows with quantities, cost snapshots,
reorder points, and simple stocking-location identifiers.
"""

import logging
import random
from datetime import timedelta

import pandas as pd

import data_generator.config as config
from data_generator.config import END_DATE


def random_date_in_last_year():
    """Return a random recent restock date relative to the configured end date.

    Returns:
        datetime.date: Date in the last year before `END_DATE`.
    """
    return (END_DATE - timedelta(days=random.randint(0, 365))).date()


def generate_inventory(items_df, dcs_df):
    """Generate inventory rows for a sampled subset of items in each DC.

    Args:
        items_df: Item table containing item and vendor fields.
        dcs_df: Distribution-center table containing `dc_id` values.

    Returns:
        pd.DataFrame: Inventory table with cost metrics, quantities,
        reorder points, and restock dates.
    """
    logger = logging.getLogger(__name__)
    rows = []
    total_est = int(len(dcs_df) * len(items_df) * 0.4)
    log_interval = min(config.LOG_INTERVAL, total_est) if total_est else config.LOG_INTERVAL

    # Load catalog to get base unit_cost values
    catalog = pd.read_csv(config.ITEM_CATALOG_PATH)
    catalog = catalog.reset_index(drop=True)
    catalog["item_id"] = range(1, len(catalog) + 1)
    costs = catalog.set_index("item_id")["unit_cost"]

    count = 0
    last_logged = 0

    logger.info("Generating inventory for %d distribution centers", len(dcs_df))
    for idx, (_, dc) in enumerate(dcs_df.iterrows(), start=1):
        region_factor = random.uniform(0.98, 1.05)
        dc_factor = random.uniform(0.98, 1.02)
        subset = items_df.sample(frac=0.4)

        for _, item in subset.iterrows():
            item_id = int(item["item_id"])
            base = costs[item_id]
            std_cost = base * region_factor * dc_factor
            last_cost = std_cost * random.uniform(0.98, 1.03)
            avg_cost = (std_cost + last_cost) / 2

            stocking_location = (
                f"A{random.randint(1, 50)}-{random.randint(1, 20)}-{random.randint(1, 10)}"
            )
            rows.append(
                {
                    "dc_id": dc["dc_id"],
                    "item_id": item_id,
                    "avg_cost": round(avg_cost, 2),
                    "last_cost": round(last_cost, 2),
                    "std_cost": round(std_cost, 2),
                    "qty_on_hand": random.randint(0, 500),
                    "qty_on_order": random.randint(0, 200),
                    "stocking_location": stocking_location,
                    "vendor_id": item["vendor_id"],
                    "reorder_point": random.randint(5, 50),
                    "last_restock_date": random_date_in_last_year(),
                }
            )
            count += 1
            if (count - last_logged) >= log_interval or count == total_est:
                logger.info("Generated %d inventory rows", count)
                last_logged = count

    logger.info("Generated %d inventory rows", count)
    return pd.DataFrame(rows)

"""Generate order headers for full and streamed pipeline modes.

Includes both row-by-row and vectorized chunked generation paths so callers
can choose between simplicity and memory efficiency.
"""

import logging
import random

import numpy as np
import pandas as pd

import data_generator.config as config
from data_generator.config import END_DATE, START_DATE


def random_date(start, end):
    """Return a single random datetime between two bounds.

    Args:
        start: Inclusive lower datetime bound.
        end: Inclusive upper datetime bound.

    Returns:
        datetime: Random datetime between `start` and `end`.
    """
    return start + (end - start) * random.random()


def _random_date_array(start, end, size):
    """Generate many random datetimes between two bounds using NumPy.

    Args:
        start: Lower datetime bound.
        end: Upper datetime bound.
        size: Number of random timestamps to create.

    Returns:
        pd.Series: Vector of datetime values.
    """
    start_ts = start.timestamp()
    end_ts = end.timestamp()
    samples = np.random.random(size)
    times = start_ts + samples * (end_ts - start_ts)
    return pd.to_datetime(times, unit="s")


def generate_orders_chunked(customers_df, dcs_df, chunk_size=10000, n_orders=None):
    """Yield order rows in DataFrame chunks for streamed generation.

    Args:
        customers_df: Customer table with `customer_id` values.
        dcs_df: Distribution-center table with `dc_id` values.
        chunk_size: Number of rows per yielded chunk.
        n_orders: Optional override for total orders to generate.

    Yields:
        pd.DataFrame: Order chunk containing IDs, dates, DC assignment,
        shipping method, and order status.
    """
    logger = logging.getLogger(__name__)
    cust_ids = customers_df["customer_id"].values
    dc_ids = dcs_df["dc_id"].values
    statuses = ["Complete", "Open", "Backordered", "Cancelled"]
    ship_vias = ["Ground", "2-Day", "Overnight"]

    total = n_orders or config.N_ORDERS
    order_id = 1
    chunk_idx = 0
    total_generated = 0
    last_logged = 0
    while order_id <= total:
        this_chunk = min(chunk_size, total - order_id + 1)
        chunk_idx += 1
        # vectorized choices
        customer_choices = np.random.choice(cust_ids, size=this_chunk)
        dc_choices = np.random.choice(dc_ids, size=this_chunk)
        ship_via_choices = np.random.choice(ship_vias, size=this_chunk)
        status_choices = np.random.choice(statuses, size=this_chunk, p=[0.8, 0.1, 0.07, 0.03])
        dates = _random_date_array(START_DATE, END_DATE, this_chunk)

        rows = {
            "order_id": np.arange(order_id, order_id + this_chunk),
            "customer_id": customer_choices.astype(int),
            "order_date": dates,
            "dc_id": dc_choices.astype(int),
            "ship_via": ship_via_choices,
            "order_status": status_choices,
        }

        df = pd.DataFrame(rows)
        total_generated += this_chunk
        if (total_generated - last_logged) >= config.LOG_INTERVAL or (
            order_id + this_chunk - 1
        ) == total:
            logger.info("Generated %d / %d orders", total_generated, total)
            last_logged = total_generated
        yield df
        order_id += this_chunk


def generate_orders(customers_df, dcs_df):
    """Generate the full orders table using configured totals.

    Args:
        customers_df: Customer table with valid customer IDs.
        dcs_df: Distribution-center table with valid DC IDs.

    Returns:
        pd.DataFrame: Complete order-header table for the configured run.
    """
    rows = []
    cust_ids = customers_df["customer_id"].values
    dc_ids = dcs_df["dc_id"].values
    statuses = ["Complete", "Open", "Backordered", "Cancelled"]
    ship_vias = ["Ground", "2-Day", "Overnight"]
    logger = logging.getLogger(__name__)
    # log progress occasionally for large generation runs
    log_interval = (
        min(config.LOG_INTERVAL, config.N_ORDERS) if config.N_ORDERS else config.LOG_INTERVAL
    )
    logger.info("Generating %d orders", config.N_ORDERS)

    for oid in range(1, config.N_ORDERS + 1):
        rows.append(
            {
                "order_id": oid,
                "customer_id": int(np.random.choice(cust_ids)),
                "order_date": random_date(START_DATE, END_DATE),
                "dc_id": int(np.random.choice(dc_ids)),
                "ship_via": random.choice(ship_vias),
                "order_status": random.choices(statuses, weights=[0.8, 0.1, 0.07, 0.03])[0],
            }
        )

        if oid % log_interval == 0:
            logger.info("Generated %d / %d orders", oid, config.N_ORDERS)

    logger.info("Generated %d orders", config.N_ORDERS)
    return pd.DataFrame(rows)

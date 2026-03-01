"""
Generate realistic inventory adjustment records.

Schema fields (example):

    adjustment_id         BIGSERIAL PRIMARY KEY
    dc_id                 BIGINT NOT NULL
    item_id               BIGINT NOT NULL
    adjustment_type       VARCHAR(50)
    quantity              INTEGER
    reason_code           VARCHAR(50)
    user_id               BIGINT
    adjustment_timestamp  TIMESTAMP

Business-driven realism:
- High-velocity items have more adjustments.
- Reason codes follow realistic warehouse patterns.
- Quantities scale with item velocity.
- Timestamps spread across the last 90 days.
"""

import logging
import random
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

import data_generator.config as config

# -----------------------------
# Reason code distributions
# -----------------------------

ADJUSTMENT_TYPES = [
    "Cycle Count",
    "Damage",
    "Shrink",
    "Return to Stock",
    "Administrative Correction",
]

REASON_CODES = {
    "Cycle Count": [
        "Count Variance",
        "Recount Adjustment",
        "Audit Correction",
    ],
    "Damage": [
        "Crushed",
        "Leaking",
        "Broken",
        "Packaging Damage",
    ],
    "Shrink": [
        "Lost",
        "Misplaced",
        "Theft Suspected",
    ],
    "Return to Stock": [
        "Customer Return",
        "Restock",
        "RMA",
    ],
    "Administrative Correction": [
        "Data Entry Error",
        "Unit of Measure Correction",
        "Transfer Correction",
    ],
}


def _estimate_velocity(unit_price):
    """Velocity proxy based on price (lower price → higher velocity)."""
    return max(1, int((120 / max(1, unit_price)) * random.uniform(0.5, 2.0)))


def generate_inventory_adjustments(items_df, dcs_df, n_adjustments_per_dc=300):
    """
    Generate inventory adjustments for each DC.

    Args:
        items_df (pd.DataFrame): Must include item_id (unit prices sourced from catalog).
        dcs_df (pd.DataFrame): Must include dc_id.
        n_adjustments_per_dc (int): Approx number of adjustments per DC.

    Returns:
        pd.DataFrame: Inventory adjustments table.
    """
    logger = logging.getLogger(__name__)
    logger.info("Generating inventory adjustments (%d per DC)", n_adjustments_per_dc)

    # Load catalog to get base unit_price values
    catalog = pd.read_csv(config.ITEM_CATALOG_PATH)
    catalog = catalog.reset_index(drop=True)
    catalog["item_id"] = range(1, len(catalog) + 1)
    prices = catalog.set_index("item_id")["unit_price"]

    rows = []
    adjustment_id = 1
    now = datetime.now()

    # Precompute velocities for realism
    items_df = items_df.copy()
    items_df["velocity"] = items_df["item_id"].map(lambda x: _estimate_velocity(prices[x]))

    # Weighted sampling: high-velocity items get more adjustments
    weights = items_df["velocity"].to_numpy()
    item_ids = items_df["item_id"].to_numpy()

    for _, dc in dcs_df.iterrows():
        dc_id = int(dc["dc_id"])

        # Number of adjustments for this DC
        n_adj = int(random.gauss(n_adjustments_per_dc, n_adjustments_per_dc * 0.2))
        n_adj = max(50, n_adj)

        for _ in range(n_adj):
            # Weighted random item selection
            item_id = int(np.random.choice(item_ids, p=weights / weights.sum()))
            item_row = items_df.loc[items_df["item_id"] == item_id].iloc[0]
            velocity = item_row["velocity"]

            # Adjustment type
            adj_type = random.choice(ADJUSTMENT_TYPES)
            reason = random.choice(REASON_CODES[adj_type])

            # Quantity: scale with velocity
            qty = int(max(1, random.gauss(mu=velocity * 0.05, sigma=velocity * 0.02)))
            qty = min(qty, max(1, velocity))  # cap extreme values

            # Random user_id (placeholder)
            user_id = random.randint(1000, 1100)

            # Timestamp within last 90 days
            days_back = random.randint(0, 90)
            seconds_back = random.randint(0, 86400)
            timestamp = now - timedelta(days=days_back, seconds=seconds_back)

            rows.append(
                {
                    "adjustment_id": adjustment_id,
                    "dc_id": dc_id,
                    "item_id": item_id,
                    "adjustment_type": adj_type,
                    "quantity": qty,
                    "reason_code": reason,
                    "user_id": user_id,
                    "adjustment_timestamp": timestamp,
                }
            )

            adjustment_id += 1

    df = pd.DataFrame(rows)
    logger.info("Generated %d inventory adjustment records", len(df))
    return df

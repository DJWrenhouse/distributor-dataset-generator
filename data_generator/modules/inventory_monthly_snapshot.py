"""
Generate monthly inventory snapshots for each item at each distribution center.

Schema fields (example):

    snapshot_id            BIGSERIAL PRIMARY KEY
    dc_id                  BIGINT NOT NULL
    item_id                BIGINT NOT NULL
    snapshot_month         DATE NOT NULL
    quantity_on_hand       INTEGER
    quantity_allocated     INTEGER
    quantity_available     INTEGER
    days_on_hand           INTEGER
    excess_quantity        INTEGER
    obsolete_quantity      INTEGER
    aging_0_30             INTEGER
    aging_31_60            INTEGER
    aging_61_90            INTEGER
    aging_90_plus          INTEGER
    cycle_count_accuracy   NUMERIC(5,2)

Business-driven realism:
- Velocity drives inventory levels.
- Lifecycle stage influences excess/obsolete.
- ABC class influences cycle count accuracy.
- Aging buckets reflect realistic warehouse aging patterns.
"""

import logging
import random
from datetime import datetime

import pandas as pd
from dateutil.relativedelta import relativedelta

import data_generator.config as config


def _estimate_velocity(unit_price):
    """Velocity proxy based on price (lower price → higher velocity)."""
    return max(1, int((120 / max(1, unit_price)) * random.uniform(0.5, 2.0)))


def _abc_class(unit_price, velocity):
    """ABC classification based on revenue contribution."""
    revenue = unit_price * velocity
    if revenue > 5000:
        return "A"
    if revenue > 1500:
        return "B"
    return "C"


def _lifecycle_stage(item_id):
    """Lifecycle stage based on item_id as a proxy for age."""
    if item_id % 10 == 0:
        return "End of Life"
    if item_id % 7 == 0:
        return "Mature"
    if item_id % 5 == 0:
        return "Growth"
    return "New"


def _aging_buckets(qty):
    """Generate realistic aging buckets that sum to qty."""
    if qty <= 0:
        return 0, 0, 0, 0

    # Random distribution with bias toward newer stock
    a0_30 = int(qty * random.uniform(0.40, 0.70))
    a31_60 = int(qty * random.uniform(0.10, 0.25))
    a61_90 = int(qty * random.uniform(0.05, 0.15))
    a90_plus = max(0, qty - (a0_30 + a31_60 + a61_90))

    return a0_30, a31_60, a61_90, a90_plus


def _cycle_count_accuracy(abc):
    """Cycle count accuracy based on ABC class."""
    if abc == "A":
        return round(random.uniform(0.95, 0.99), 2)
    if abc == "B":
        return round(random.uniform(0.90, 0.96), 2)
    return round(random.uniform(0.80, 0.92), 2)


def generate_inventory_monthly_snapshot(items_df, dcs_df, months_back=12):
    """
    Generate monthly inventory snapshots for each item at each DC.

    Args:
        items_df (pd.DataFrame): Must include item_id.
        dcs_df (pd.DataFrame): Must include dc_id.
        months_back (int): Number of months of history to generate.

    Returns:
        pd.DataFrame: Inventory snapshot table.
    """
    logger = logging.getLogger(__name__)
    logger.info("Generating inventory monthly snapshots (%d months)", months_back)

    # Load catalog to get base unit_price values
    catalog = pd.read_csv(config.ITEM_CATALOG_PATH)
    catalog = catalog.reset_index(drop=True)
    catalog["item_id"] = range(1, len(catalog) + 1)
    prices = catalog.set_index("item_id")["unit_price"]

    rows = []
    snapshot_id = 1
    today = datetime.today()

    for _, dc in dcs_df.iterrows():
        dc_id = int(dc["dc_id"])

        for _, item in items_df.iterrows():
            item_id = int(item["item_id"])
            unit_price = float(prices[item_id])

            velocity = _estimate_velocity(unit_price)
            abc = _abc_class(unit_price, velocity)
            lifecycle = _lifecycle_stage(item_id)

            for m in range(months_back):
                snapshot_month = (today - relativedelta(months=m)).replace(day=1).date()

                # Quantity on hand driven by velocity + randomness
                base_qoh = int(velocity * random.uniform(0.5, 2.5))
                quantity_on_hand = max(0, base_qoh)

                # Allocated quantity: 0–30 percent of QOH
                quantity_allocated = int(quantity_on_hand * random.uniform(0.0, 0.3))
                quantity_available = max(0, quantity_on_hand - quantity_allocated)

                # Days on hand: QOH / daily demand proxy
                daily_demand = max(1, velocity / 30)
                days_on_hand = int(quantity_available / daily_demand)

                # Excess & obsolete logic
                if lifecycle == "End of Life":
                    obsolete_quantity = int(quantity_on_hand * random.uniform(0.20, 0.50))
                    excess_quantity = int(quantity_on_hand * random.uniform(0.10, 0.30))
                elif lifecycle == "Mature":
                    obsolete_quantity = int(quantity_on_hand * random.uniform(0.05, 0.15))
                    excess_quantity = int(quantity_on_hand * random.uniform(0.05, 0.20))
                else:
                    obsolete_quantity = int(quantity_on_hand * random.uniform(0.00, 0.05))
                    excess_quantity = int(quantity_on_hand * random.uniform(0.00, 0.10))

                # Aging buckets
                aging = _aging_buckets(quantity_on_hand)
                a0_30, a31_60, a61_90, a90_plus = aging

                # Cycle count accuracy
                accuracy = _cycle_count_accuracy(abc)

                rows.append(
                    {
                        "snapshot_id": snapshot_id,
                        "dc_id": dc_id,
                        "item_id": item_id,
                        "snapshot_month": snapshot_month,
                        "quantity_on_hand": quantity_on_hand,
                        "quantity_allocated": quantity_allocated,
                        "quantity_available": quantity_available,
                        "days_on_hand": days_on_hand,
                        "excess_quantity": excess_quantity,
                        "obsolete_quantity": obsolete_quantity,
                        "aging_0_30": a0_30,
                        "aging_31_60": a31_60,
                        "aging_61_90": a61_90,
                        "aging_90_plus": a90_plus,
                        "cycle_count_accuracy": accuracy,
                    }
                )

                snapshot_id += 1

    df = pd.DataFrame(rows)
    logger.info("Generated %d inventory snapshot records", len(df))
    return df

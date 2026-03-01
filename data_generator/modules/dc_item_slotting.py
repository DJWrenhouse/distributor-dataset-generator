"""
Generate realistic slotting assignments for each item at each distribution
center.

Schema fields (example):

    slotting_id           BIGSERIAL PRIMARY KEY
    dc_id                 BIGINT NOT NULL
    item_id               BIGINT NOT NULL
    pick_location         VARCHAR(50)
    replenishment_location VARCHAR(50)
    slotting_rank         INTEGER
    velocity_class        VARCHAR(1)

Business-driven realism:
- Velocity drives pick location priority.
- Cube/weight influence slotting rank.
- Velocity class (A/B/C) determines forward pick vs. reserve.
- Locations follow realistic warehouse patterns (Aisle-Bay-Level).
"""

import logging
import random

import pandas as pd

import data_generator.config as config

# -----------------------------
# Helper functions
# -----------------------------


def _estimate_velocity(unit_price):
    """Velocity proxy: lower price → higher velocity."""
    return max(1, int((120 / max(1, unit_price)) * random.uniform(0.5, 2.0)))


def _velocity_class(velocity):
    """A/B/C classification based on velocity."""
    if velocity > 80:
        return "A"
    if velocity > 30:
        return "B"
    return "C"


def _generate_location(is_pick=True):
    """
    Generate a realistic warehouse location code.
    Example: A12-B03-L02
    """
    aisle = random.randint(1, 40)
    bay = random.randint(1, 30)
    level = random.randint(1, 6 if is_pick else 10)

    return f"A{aisle:02d}-B{bay:02d}-L{level:02d}"


def _slotting_rank(velocity, cube_ft, weight_lb):
    """
    Compute a slotting rank:
    - Lower rank = better location
    - High velocity → lower rank
    - Large cube/weight → higher rank (harder to pick)
    """
    base = 100 - velocity
    size_penalty = (cube_ft * 10) + (weight_lb * 0.5)
    noise = random.uniform(-5, 5)

    return max(1, int(base + size_penalty + noise))


# -----------------------------
# Main generator
# -----------------------------


def generate_dc_item_slotting(items_df, dcs_df):
    """
    Generate slotting assignments for each item at each DC.

    Args:
        items_df (pd.DataFrame): Must include item_id, cube_ft, weight_lb
        (unit prices sourced from catalog).
        dcs_df (pd.DataFrame): Must include dc_id.

    Returns:
        pd.DataFrame: Slotting table.
    """
    logger = logging.getLogger(__name__)
    logger.info("Generating DC item slotting assignments")

    # Load catalog to get base unit_price values
    catalog = pd.read_csv(config.ITEM_CATALOG_PATH)
    catalog = catalog.reset_index(drop=True)
    catalog["item_id"] = range(1, len(catalog) + 1)
    prices = catalog.set_index("item_id")["unit_price"]

    rows = []
    slotting_id = 1

    for _, dc in dcs_df.iterrows():
        dc_id = int(dc["dc_id"])

        for _, item in items_df.iterrows():
            item_id = int(item["item_id"])
            unit_price = float(prices[item_id])
            cube_ft = float(item.get("cube_ft", 0.1) or 0.1)
            weight_lb = float(item.get("weight_lb", 1.0) or 1.0)

            velocity = _estimate_velocity(unit_price)
            vclass = _velocity_class(velocity)

            # Pick location logic:
            # A-class → forward pick
            # B-class → mix
            # C-class → mostly reserve
            if vclass == "A":
                pick_location = _generate_location(is_pick=True)
                replen_location = _generate_location(is_pick=False)
            elif vclass == "B":
                pick_location = _generate_location(is_pick=True) if random.random() < 0.6 else None
                replen_location = _generate_location(is_pick=False)
            else:
                pick_location = None
                replen_location = _generate_location(is_pick=False)

            rank = _slotting_rank(velocity, cube_ft, weight_lb)

            rows.append(
                {
                    "slotting_id": slotting_id,
                    "dc_id": dc_id,
                    "item_id": item_id,
                    "pick_location": pick_location,
                    "replenishment_location": replen_location,
                    "slotting_rank": rank,
                    "velocity_class": vclass,
                }
            )

            slotting_id += 1

    df = pd.DataFrame(rows)
    logger.info("Generated %d slotting records", len(df))
    return df

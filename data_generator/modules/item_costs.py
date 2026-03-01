"""
Generate realistic item cost records.

This module produces effective-dated item cost history with landed cost
components, matching a schema like:

    item_cost_id       BIGSERIAL PRIMARY KEY
    item_id            BIGINT NOT NULL
    effective_date     DATE NOT NULL
    unit_cost          NUMERIC(12,4)
    unit_price         NUMERIC(12,4)
    shipping_cost_per_unit NUMERIC(12,4)
    freight_cost       NUMERIC(12,4)
    duty_rate          NUMERIC(6,4)
    landed_cost        NUMERIC(12,4)
    replacement_cost   NUMERIC(12,4)
    map_price          NUMERIC(12,4)

Business-driven realism:
- Base unit_cost, unit_price, and shipping_cost_per_unit come from the catalog.
- All three become historical records with price variations per version.
- Freight_cost depends on cube/weight.
- Duty_rate depends on product_category.
- Landed_cost = unit_cost + freight_cost + (unit_cost * duty_rate).
- Replacement_cost is a slight premium over landed_cost.
- MAP price is tied to unit_price and margin.
"""

import logging
import random
from datetime import datetime, timedelta

import pandas as pd

import data_generator.config as config


def _category_duty_rate(category: str) -> float:
    """Return a realistic duty rate based on product category."""
    if not isinstance(category, str):
        return 0.0
    c = category.lower()
    if "chemical" in c or "clean" in c:
        return random.uniform(0.05, 0.12)
    if "electronics" in c or "computer" in c:
        return random.uniform(0.02, 0.06)
    if "furniture" in c:
        return random.uniform(0.03, 0.08)
    if "industrial" in c:
        return random.uniform(0.04, 0.10)
    return random.uniform(0.00, 0.04)


def _estimate_freight_cost(cube_ft: float, weight_lb: float) -> float:
    """Estimate freight cost per unit based on cube and weight."""
    cube_ft = cube_ft or 0.0
    weight_lb = weight_lb or 0.0

    base = 0.15
    cube_component = cube_ft * random.uniform(0.05, 0.20)
    weight_component = (weight_lb / 10.0) * random.uniform(0.03, 0.10)

    return max(0.05, base + cube_component + weight_component)


def generate_item_costs(items_df: pd.DataFrame, n_versions: int = 3) -> pd.DataFrame:
    """
    Generate effective-dated item cost records with historical variations.

    Args:
        items_df (pd.DataFrame): Item master table with at least:
            - item_id
            - cube_ft
            - weight_lb
            - category (or product_category)
        n_versions (int): Number of historical cost versions per item.

    Returns:
        pd.DataFrame: Item costs table with unit_cost, unit_price, and
                      shipping_cost_per_unit as historical records.
    """
    logger = logging.getLogger(__name__)
    logger.info(
        "Generating item cost history for %d items (versions=%d)",
        len(items_df),
        n_versions,
    )

    # Load catalog to get base costs and prices
    catalog = pd.read_csv(config.ITEM_CATALOG_PATH)
    catalog = catalog.rename(
        columns={
            "weight_lbs": "weight_lb",
            "cubic_ft": "cube_ft",
        }
    )
    # Reset index to match item_id values (1-based)
    catalog = catalog.reset_index(drop=True)
    catalog["item_id"] = range(1, len(catalog) + 1)

    rows = []
    item_cost_id = 1
    today = datetime.today().date()

    for _, item in items_df.iterrows():
        item_id = int(item["item_id"])

        # Get catalog row (item_id is 1-based, so subtract 1 for index)
        catalog_row = catalog.iloc[item_id - 1]
        base_cost = float(catalog_row.get("unit_cost", 0.0) or 0.0)
        base_price = float(catalog_row.get("unit_price", 0.0) or 0.0)
        base_shipping_cost = float(catalog_row.get("shipping_cost_per_unit", 0.0) or 0.0)

        cube_ft = float(item.get("cube_ft", 0.0) or 0.0)
        weight_lb = float(item.get("weight_lb", 0.0) or 0.0)
        category = item.get("category", "")

        # Generate multiple effective-dated versions per item
        # Newer versions are closer to today
        for version in range(n_versions):
            days_ago = random.randint(30 * (version + 1), 30 * (version + 3))
            effective_date = today - timedelta(days=days_ago)

            # Random walk around base values for unit_cost, unit_price, shipping_cost
            cost_factor = random.uniform(0.92, 1.12)
            unit_cost = round(base_cost * cost_factor, 4)

            price_factor = random.uniform(0.90, 1.15)
            unit_price = round(base_price * price_factor, 4)

            shipping_factor = random.uniform(0.88, 1.20)
            shipping_cost_per_unit = round(base_shipping_cost * shipping_factor, 4)

            freight_cost = round(_estimate_freight_cost(cube_ft, weight_lb), 4)
            duty_rate = round(_category_duty_rate(category), 4)

            duty_amount = unit_cost * duty_rate
            landed_cost = round(unit_cost + freight_cost + duty_amount, 4)

            replacement_factor = random.uniform(1.01, 1.08)
            replacement_cost = round(landed_cost * replacement_factor, 4)

            # MAP price: somewhere between cost-based and list price
            margin_factor = random.uniform(1.10, 1.35)
            map_price = round(max(unit_price * 0.8, landed_cost * margin_factor), 4)

            rows.append(
                {
                    "item_cost_id": item_cost_id,
                    "item_id": item_id,
                    "effective_date": effective_date,
                    "unit_cost": unit_cost,
                    "unit_price": unit_price,
                    "shipping_cost_per_unit": shipping_cost_per_unit,
                    "freight_cost": freight_cost,
                    "duty_rate": duty_rate,
                    "landed_cost": landed_cost,
                    "replacement_cost": replacement_cost,
                    "map_price": map_price,
                }
            )

            item_cost_id += 1

    df = pd.DataFrame(rows)
    logger.info("Generated %d item cost records", len(df))
    return df

"""
Generate realistic item-level demand forecasts.

This module produces monthly forecast records for each item, including
ABC/XYZ classification, seasonality, lifecycle stage, and forecast error
metrics. It matches a schema like:

    forecast_id          BIGSERIAL PRIMARY KEY
    item_id              BIGINT NOT NULL
    forecast_month       DATE NOT NULL
    forecast_quantity    INTEGER
    abc_class            VARCHAR(1)
    xyz_class            VARCHAR(1)
    seasonality_index    NUMERIC(6,4)
    lifecycle_stage      VARCHAR(20)
    forecast_error_mape  NUMERIC(6,4)
    forecast_error_mad   NUMERIC(12,4)

Business-driven realism:
- ABC class based on revenue contribution (unit_price * velocity).
- XYZ class based on demand variability.
- Seasonality tied to product category.
- Lifecycle stage based on item age.
- Forecast error tied to ABC/XYZ class.
"""

import logging
import random
from datetime import datetime

import pandas as pd
from dateutil.relativedelta import relativedelta

import data_generator.config as config

# -----------------------------
# Helper functions
# -----------------------------


def _assign_abc_class(unit_price, velocity):
    """Assign ABC class based on revenue contribution."""
    revenue = unit_price * velocity
    if revenue > 5000:
        return "A"
    if revenue > 1500:
        return "B"
    return "C"


def _assign_xyz_class(velocity):
    """Assign XYZ class based on demand variability."""
    if velocity > 80:
        return "X"
    if velocity > 30:
        return "Y"
    return "Z"


def _seasonality_factor(category, month):
    """Return a seasonality index based on category + month."""
    c = category.lower()

    # Office supplies spike in August/September
    if "office" in c:
        if month in (8, 9):
            return random.uniform(1.10, 1.25)
        return random.uniform(0.95, 1.05)

    # Cleaning supplies spike in spring
    if "clean" in c:
        if month in (3, 4, 5):
            return random.uniform(1.10, 1.30)
        return random.uniform(0.90, 1.05)

    # Break room spikes in November/December
    if "break" in c:
        if month in (11, 12):
            return random.uniform(1.15, 1.35)
        return random.uniform(0.90, 1.05)

    # Default mild seasonality
    return random.uniform(0.95, 1.05)


def _lifecycle_stage(item_id):
    """Assign lifecycle stage based on item_id as a proxy for age."""
    if item_id % 10 == 0:
        return "End of Life"
    if item_id % 7 == 0:
        return "Mature"
    if item_id % 5 == 0:
        return "Growth"
    return "New"


def _forecast_error(abc, xyz):
    """Return realistic MAPE and MAD based on ABC/XYZ class."""
    # Lower error for A/X items, higher for C/Z
    base_mape = {
        "A": 0.05,
        "B": 0.10,
        "C": 0.18,
    }[abc]

    variability = {
        "X": 0.8,
        "Y": 1.2,
        "Z": 1.8,
    }[xyz]

    mape = round(base_mape * variability * random.uniform(0.8, 1.3), 4)
    mad = round(mape * random.uniform(8, 20), 4)

    return mape, mad


# -----------------------------
# Main generator
# -----------------------------


def generate_item_forecast(items_df, months_forward=12):
    """
    Generate item-level forecasts for the next N months.

    Args:
        items_df (pd.DataFrame): Must include:
            - item_id
            - category (or product_category)
        months_forward (int): Number of months to forecast.

    Returns:
        pd.DataFrame: Forecast table.
    """
    logger = logging.getLogger(__name__)
    logger.info(
        "Generating item forecasts for %d items (%d months forward)",
        len(items_df),
        months_forward,
    )

    # Load catalog to get base unit_price values
    catalog = pd.read_csv(config.ITEM_CATALOG_PATH)
    catalog = catalog.reset_index(drop=True)
    catalog["item_id"] = range(1, len(catalog) + 1)

    rows = []
    forecast_id = 1
    today = datetime.today()

    for _, item in items_df.iterrows():
        item_id = int(item["item_id"])

        # Get catalog row to retrieve unit_price
        catalog_row = catalog.iloc[item_id - 1]
        unit_price = float(catalog_row.get("unit_price", 0.0) or 0.0)

        category = item.get("category", "")

        # Velocity proxy: based on price + random noise
        velocity = max(1, int((100 / max(1, unit_price)) * random.uniform(0.5, 2.0)))

        abc = _assign_abc_class(unit_price, velocity)
        xyz = _assign_xyz_class(velocity)
        lifecycle = _lifecycle_stage(item_id)

        for m in range(months_forward):
            forecast_month = (today + relativedelta(months=m)).replace(day=1).date()
            month_num = forecast_month.month

            seasonality = _seasonality_factor(category, month_num)

            # Base forecast quantity
            base_qty = velocity * seasonality * random.uniform(0.8, 1.2)
            forecast_qty = max(1, int(base_qty))

            mape, mad = _forecast_error(abc, xyz)

            rows.append(
                {
                    "forecast_id": forecast_id,
                    "item_id": item_id,
                    "forecast_month": forecast_month,
                    "forecast_quantity": forecast_qty,
                    "abc_class": abc,
                    "xyz_class": xyz,
                    "seasonality_index": round(seasonality, 4),
                    "lifecycle_stage": lifecycle,
                    "forecast_error_mape": mape,
                    "forecast_error_mad": mad,
                }
            )

            forecast_id += 1

    df = pd.DataFrame(rows)
    logger.info("Generated %d forecast records", len(df))
    return df

"""
Generate customer-item preference scores based on order history.

Schema fields (example):

    preference_id        BIGSERIAL PRIMARY KEY
    customer_id          BIGINT NOT NULL
    item_id              BIGINT NOT NULL
    preference_score     NUMERIC(5,2)
    last_purchased_date  DATE

Business-driven realism:
- Preference is driven by recency, frequency, and monetary value (RFM).
- Uses actual orders and order_lines to compute affinity.
"""

import logging
import random
from datetime import datetime

import numpy as np
import pandas as pd

import data_generator.config as config


def generate_customer_item_preferences(
    customers_df,
    items_df,
    orders_df,
    order_lines_df,
    min_score=5.0,
    top_n_per_customer=50,
):
    """
    Generate customer-item preference scores using order history.

    Args:
        customers_df (pd.DataFrame): Must include customer_id.
        items_df (pd.DataFrame): Must include item_id (unit prices sourced from catalog).
        orders_df (pd.DataFrame): Must include order_id, customer_id,
        order_date.
        order_lines_df (pd.DataFrame): Must include order_id, item_id,
        quantity.
        min_score (float): Minimum preference_score to keep.
        top_n_per_customer (int): Keep only top N items per customer.

    Returns:
        pd.DataFrame: Customer-item preferences.
    """
    logger = logging.getLogger(__name__)
    logger.info("Generating customer-item preferences")

    if orders_df.empty or order_lines_df.empty:
        logger.warning("No orders/order_lines provided; returning empty preferences")
        return pd.DataFrame(
            columns=[
                "preference_id",
                "customer_id",
                "item_id",
                "preference_score",
                "last_purchased_date",
            ]
        )

    # Load catalog to get base unit_price values
    catalog = pd.read_csv(config.ITEM_CATALOG_PATH)
    catalog = catalog.reset_index(drop=True)
    catalog["item_id"] = range(1, len(catalog) + 1)

    # Merge orders and lines to get customer-item facts
    orders = orders_df[["order_id", "customer_id", "order_date"]].copy()

    quantity_col = "quantity" if "quantity" in order_lines_df.columns else "quantity_ordered"
    if quantity_col not in order_lines_df.columns:
        raise KeyError("order_lines_df must include either 'quantity' or 'quantity_ordered'")

    lines = order_lines_df[["order_id", "item_id", quantity_col]].copy()
    if quantity_col != "quantity":
        lines = lines.rename(columns={quantity_col: "quantity"})

    merged = lines.merge(orders, on="order_id", how="inner")
    merged = merged.merge(
        catalog[["item_id", "unit_price"]],
        on="item_id",
        how="left",
    )

    merged["order_date"] = pd.to_datetime(merged["order_date"], errors="coerce")
    merged["quantity"] = pd.to_numeric(merged["quantity"], errors="coerce")
    merged["unit_price"] = pd.to_numeric(merged["unit_price"], errors="coerce")
    merged["extended_price"] = merged["quantity"] * merged["unit_price"].fillna(0.0)

    # Aggregate to customer-item level
    agg = (
        merged.groupby(["customer_id", "item_id"])
        .agg(
            total_qty=("quantity", "sum"),
            total_revenue=("extended_price", "sum"),
            last_purchased_date=("order_date", "max"),
            order_count=("order_id", "nunique"),
        )
        .reset_index()
    )

    if agg.empty:
        logger.warning("No aggregated customer-item history; returning empty preferences")
        return pd.DataFrame(
            columns=[
                "preference_id",
                "customer_id",
                "item_id",
                "preference_score",
                "last_purchased_date",
            ]
        )

    # RFM-style scoring
    today = datetime.today()
    agg["recency_days"] = (today - agg["last_purchased_date"]).dt.days.clip(lower=0)

    # Normalize components
    def _normalize(series):
        if series.max() == series.min():
            return pd.Series(0.5, index=series.index)
        return (series - series.min()) / (series.max() - series.min())

    freq_norm = _normalize(agg["order_count"])
    mon_norm = _normalize(agg["total_revenue"])
    rec_norm = 1.0 - _normalize(agg["recency_days"])  # lower days → higher score  # noqa: E501

    # Weighted preference score (0–100)
    agg["preference_score"] = (0.4 * freq_norm + 0.4 * mon_norm + 0.2 * rec_norm) * 100.0

    # Add a bit of noise for realism
    agg["preference_score"] = agg["preference_score"].apply(
        lambda x: max(0.0, min(100.0, x * random.uniform(0.95, 1.05)))
    )

    # Filter by minimum score
    agg = agg[agg["preference_score"] >= min_score]

    # Keep only top N items per customer
    agg = agg.sort_values(["customer_id", "preference_score"], ascending=[True, False])
    agg["rank"] = agg.groupby("customer_id")["preference_score"].rank(
        method="first", ascending=False
    )
    agg = agg[agg["rank"] <= top_n_per_customer]

    # Final shape
    agg = agg.sort_values(["customer_id", "preference_score"], ascending=[True, False])

    agg = agg.reset_index(drop=True)
    agg["preference_id"] = np.arange(1, len(agg) + 1)

    prefs = agg[
        [
            "preference_id",
            "customer_id",
            "item_id",
            "preference_score",
            "last_purchased_date",
        ]
    ].copy()

    logger.info("Generated %d customer-item preference records", len(prefs))
    return prefs

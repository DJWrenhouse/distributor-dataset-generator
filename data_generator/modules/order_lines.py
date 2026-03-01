"""Generate order-line records for standard and streamed processing paths.

This module produces line-level quantity and pricing records tied to orders
and items, including customer discount effects.
"""

import logging
import random

import numpy as np
import pandas as pd

import data_generator.config as config


def generate_order_lines(orders_df, items_df, customers_df):
    """Build order lines per order using customer discount-adjusted prices.

    Args:
        orders_df: Order-header table.
        items_df: Item table (unit prices sourced from catalog).
        customers_df: Customer table with discount percentages.

    Returns:
        pd.DataFrame: Order-line table including quantities and extended
        prices.
    """
    # Load catalog to get base unit_price values
    catalog = pd.read_csv(config.ITEM_CATALOG_PATH)
    catalog = catalog.reset_index(drop=True)
    catalog["item_id"] = range(1, len(catalog) + 1)
    prices = catalog.set_index("item_id")["unit_price"]

    rows = []
    item_ids = items_df["item_id"].values
    discounts = customers_df.set_index("customer_id")["discount_percentage"]

    logger = logging.getLogger(__name__)
    total = len(orders_df)
    log_interval = config.LOG_INTERVAL_ORDERS
    logger.info("Generating order lines for %d orders", total)
    line_id = 1
    for idx, (_, order) in enumerate(orders_df.iterrows(), start=1):
        n_lines = random.randint(1, 10)
        chosen_items = np.random.choice(item_ids, size=n_lines, replace=False)
        discount_pct = float(discounts.get(order["customer_id"], 0.0))
        discount_factor = max(0.0, 1.0 - (discount_pct / 100.0))

        for item_id in chosen_items:
            qty = random.randint(1, 20)
            price = float(prices[item_id]) * discount_factor
            rows.append(
                {
                    "order_line_id": line_id,
                    "order_id": order["order_id"],
                    "item_id": int(item_id),
                    "quantity_ordered": qty,
                    "quantity_shipped": qty,
                    "unit_price": round(price, 2),
                    "extended_price": round(price * qty, 2),
                }
            )
            line_id += 1

        # Log progress every log_interval orders
        if idx % log_interval == 0:
            logger.info(
                "Processed %d / %d orders (%d order lines generated)", idx, total, line_id - 1
            )

    logger.info("Generated %d order lines from %d orders", line_id - 1, total)
    return pd.DataFrame(rows)


def generate_order_lines_for_orders(orders_df, items_df, customers_df, start_line_id=1):
    """Vectorized order-line generation for a chunk of orders.

    This path favors speed and may include duplicate items on the same order.

    Args:
        orders_df: Chunk of orders to expand into lines.
        items_df: Item table used for line-item sampling.
        customers_df: Customer table used for discount mapping.
        start_line_id: First `order_line_id` value for this chunk.

    Returns:
        pd.DataFrame: Chunk-scoped order-line records ready for append output.
    """
    rng = np.random.default_rng()

    # Load catalog to get base unit_price values
    catalog = pd.read_csv(config.ITEM_CATALOG_PATH)
    catalog = catalog.reset_index(drop=True)
    catalog["item_id"] = range(1, len(catalog) + 1)
    prices = catalog.set_index("item_id")["unit_price"]

    order_ids = orders_df["order_id"].to_numpy()
    num_orders = len(order_ids)

    # number of lines per order (1-10)
    n_lines = rng.integers(1, 11, size=num_orders)
    total_lines = int(n_lines.sum())

    if total_lines == 0:
        return pd.DataFrame(
            columns=[
                "order_line_id",
                "order_id",
                "item_id",
                "quantity_ordered",
                "unit_price",
                "extended_price",
            ]
        )

    item_ids = items_df["item_id"].to_numpy()

    # expand order ids by their line counts
    order_id_expanded = np.repeat(order_ids, n_lines)

    # map discount per order
    discount_map = customers_df.set_index("customer_id")["discount_percentage"]
    order_discount = orders_df["customer_id"].map(discount_map).fillna(0.0).to_numpy()
    discount_expanded = np.repeat(order_discount, n_lines)

    # sample items for each line (with replacement for speed)
    item_choices = rng.choice(item_ids, size=total_lines, replace=True)

    qty = rng.integers(1, 21, size=total_lines)

    # map prices
    unit_prices = prices.reindex(item_choices).to_numpy()
    unit_prices = unit_prices * (1.0 - (discount_expanded / 100.0))

    extended = np.round(unit_prices * qty, 2)

    df = pd.DataFrame(
        {
            "order_line_id": np.arange(start_line_id, start_line_id + total_lines),
            "order_id": order_id_expanded,
            "item_id": item_choices.astype(int),
            "quantity_ordered": qty.astype(int),
            "quantity_shipped": qty.astype(int),
            "unit_price": np.round(unit_prices, 2),
            "extended_price": extended,
        }
    )

    return df

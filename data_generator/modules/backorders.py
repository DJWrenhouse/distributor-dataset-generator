"""
Generate realistic backorder records based on order lines and inventory
availability.

Schema fields (example):

    backorder_id        BIGSERIAL PRIMARY KEY
    order_id            BIGINT NOT NULL
    order_line_id       BIGINT NOT NULL
    item_id             BIGINT NOT NULL
    qty_backordered     INTEGER
    date_created        DATE
    date_filled         DATE NULL
    days_backordered    INTEGER
    reason_code         VARCHAR(50)
    is_filled_flag      BOOLEAN

Business-driven realism:
- Backorders occur when demand > available inventory.
- Fill timing depends on lead time and vendor performance.
- Reason codes reflect real distributor causes.
"""

import logging
import random
from datetime import datetime, timedelta

import pandas as pd

import data_generator.config as config

REASON_CODES = [
    "Insufficient Inventory",
    "Vendor Delay",
    "Damaged Inbound Shipment",
    "Unexpected Demand Spike",
    "Quality Hold",
]


def generate_backorders(order_lines_df, items_df, inventory_snapshot_df, backorder_rate=0.08):
    """
    Generate backorders based on order lines and inventory availability.

    Args:
        order_lines_df (pd.DataFrame): Must include:
            - order_line_id
            - order_id
            - item_id
            - quantity
        items_df (pd.DataFrame): Must include:
            - item_id
            - lead_time_days
        inventory_snapshot_df (pd.DataFrame): Must include:
            - item_id
            - quantity_available
        backorder_rate (float): Base probability of a backorder event.

    Returns:
        pd.DataFrame: Backorders table.
    """
    logger = logging.getLogger(__name__)
    total_order_lines = len(order_lines_df)
    logger.info(
        "Generating backorders for %d order lines using business-driven logic", total_order_lines
    )

    if order_lines_df.empty:
        logger.warning("No order lines provided; returning empty backorders")
        return pd.DataFrame(
            columns=[
                "backorder_id",
                "order_id",
                "order_line_id",
                "item_id",
                "qty_backordered",
                "date_created",
                "date_filled",
                "days_backordered",
                "reason_code",
                "is_filled_flag",
            ]
        )

    quantity_col = "quantity" if "quantity" in order_lines_df.columns else "quantity_ordered"
    if quantity_col not in order_lines_df.columns:
        raise KeyError("order_lines_df must include either 'quantity' or 'quantity_ordered'")

    # Merge inventory availability
    inv = inventory_snapshot_df.groupby("item_id")["quantity_available"].mean().reset_index()
    merged = order_lines_df.copy()
    if quantity_col != "quantity":
        merged["quantity"] = merged[quantity_col]

    merged = merged.merge(inv, on="item_id", how="left")
    merged["quantity_available"] = merged["quantity_available"].fillna(0)

    # Merge item lead times
    merged = merged.merge(items_df[["item_id", "lead_time_days"]], on="item_id", how="left")
    merged["lead_time_days"] = merged["lead_time_days"].fillna(7)

    rows = []
    backorder_id = 1
    today = datetime.today().date()

    # Process in chunks for better performance and logging
    chunk_size = config.CHUNK_SIZE_BACKORDERS
    total_rows = len(merged)
    total_chunks = (total_rows + chunk_size - 1) // chunk_size
    log_interval = max(1, config.LOG_INTERVAL // chunk_size)

    for chunk_idx in range(total_chunks):
        start = chunk_idx * chunk_size
        end = min(start + chunk_size, total_rows)
        chunk = merged.iloc[start:end]

        for _, row in chunk.iterrows():
            order_line_id = int(row["order_line_id"])
            order_id = int(row["order_id"])
            item_id = int(row["item_id"])
            qty_ordered = int(row["quantity"])
            qty_available = int(row["quantity_available"])
            lead_time = int(row["lead_time_days"])

            # Determine if a backorder occurs
            # Probability increases when inventory is low
            shortage = max(0, qty_ordered - qty_available)
            shortage_ratio = shortage / qty_ordered if qty_ordered > 0 else 0

            probability = backorder_rate + (shortage_ratio * 0.6)

            if random.random() > probability:
                continue  # no backorder

            # Backorder quantity
            qty_backordered = (
                shortage if shortage > 0 else max(1, int(qty_ordered * random.uniform(0.1, 0.4)))
            )

            # Creation date = today or slightly earlier
            days_back = random.randint(0, 5)
            date_created = today - timedelta(days=days_back)

            # Determine if filled
            is_filled = random.random() < 0.85  # 85 percent eventually filled

            if is_filled:
                fill_delay = int(random.gauss(mu=lead_time, sigma=lead_time * 0.3))
                fill_delay = max(1, fill_delay)
                date_filled = date_created + timedelta(days=fill_delay)
                days_backordered = fill_delay
            else:
                date_filled = None
                days_backordered = None

            reason = random.choice(REASON_CODES)

            rows.append(
                {
                    "backorder_id": backorder_id,
                    "order_id": order_id,
                    "order_line_id": order_line_id,
                    "item_id": item_id,
                    "qty_backordered": qty_backordered,
                    "date_created": date_created,
                    "date_filled": date_filled,
                    "days_backordered": days_backordered,
                    "reason_code": reason,
                    "is_filled_flag": is_filled,
                }
            )

            backorder_id += 1

        # Log progress
        if (chunk_idx + 1) % log_interval == 0 or (chunk_idx + 1) == total_chunks:
            logger.info(
                "Processed backorders chunk %d / %d (%d order lines, %d backorders generated)",
                chunk_idx + 1,
                total_chunks,
                end,
                len(rows),
            )

    df = pd.DataFrame(rows)

    if not df.empty and "days_backordered" in df.columns:
        df["days_backordered"] = df["days_backordered"].astype("Int64")

    logger.info("Generated %d backorder records from %d order lines", len(df), total_order_lines)
    return df

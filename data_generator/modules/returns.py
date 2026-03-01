"""Generate product return records linked to fulfilled customer orders.

Schema fields:
    return_id
    order_id
    order_line_id
    shipment_id
    item_id
    customer_id
    return_date
    reason_code
    condition_code
    qty_returned
    restock_flag
    credit_amount

Business-driven realism notes:
- Only completed orders are eligible for returns.
- Return timing is delayed from shipment date by 5-45 days.
- Condition code controls credit percentages and restock behavior.
"""

import logging

import numpy as np
import pandas as pd

RETURN_COLUMNS = [
    "return_id",
    "order_id",
    "order_line_id",
    "shipment_id",
    "item_id",
    "customer_id",
    "return_date",
    "reason_code",
    "condition_code",
    "qty_returned",
    "restock_flag",
    "credit_amount",
]

REASON_CODES = np.array(
    [
        "Damaged",
        "Wrong Item",
        "Not as Described",
        "Overshipment",
        "Customer Preference",
        "Defective",
    ]
)
REASON_WEIGHTS = np.array([0.24, 0.10, 0.16, 0.06, 0.28, 0.16])

CONDITION_CODES = np.array(["Resalable", "Damaged", "Defective", "Destroyed"])
CONDITION_WEIGHTS = np.array([0.55, 0.20, 0.15, 0.10])
CONDITION_DISCOUNT = {
    "Resalable": 1.00,
    "Damaged": 0.50,
    "Defective": 0.25,
    "Destroyed": 0.00,
}


def generate_returns(order_lines_df, orders_df, items_df, shipments_df):
    """Generate returns for a sampled subset of completed-order lines.

    Args:
        order_lines_df (pd.DataFrame): Order-line data with order and quantity fields.
        orders_df (pd.DataFrame): Order header data with status and customer fields.
        items_df (pd.DataFrame): Item master table (kept for interface consistency).
        shipments_df (pd.DataFrame): Shipment data with ship dates.

    Returns:
        pd.DataFrame: Returns table in canonical column order.
    """
    del items_df

    logger = logging.getLogger(__name__)
    logger.info("Generating returns from completed order lines")

    if (
        order_lines_df is None
        or orders_df is None
        or shipments_df is None
        or order_lines_df.empty
        or orders_df.empty
        or shipments_df.empty
    ):
        logger.warning("Insufficient input data for returns; returning empty DataFrame")
        return pd.DataFrame(columns=RETURN_COLUMNS)

    quantity_col = (
        "quantity_ordered" if "quantity_ordered" in order_lines_df.columns else "quantity"
    )
    if quantity_col not in order_lines_df.columns:
        raise KeyError("order_lines_df must include either 'quantity_ordered' or 'quantity'")

    order_cols = ["order_id", "order_status", "customer_id"]
    complete_orders = orders_df.loc[orders_df["order_status"] == "Complete", order_cols]
    if complete_orders.empty:
        logger.warning("No complete orders found; returning empty returns")
        return pd.DataFrame(columns=RETURN_COLUMNS)

    shipment_ref = shipments_df[["shipment_id", "order_id", "ship_date"]].copy()
    shipment_ref["ship_date"] = pd.to_datetime(shipment_ref["ship_date"], errors="coerce")
    shipment_ref = shipment_ref.sort_values(["order_id", "ship_date"]).drop_duplicates(
        subset=["order_id"], keep="first"
    )

    eligible = order_lines_df[
        ["order_line_id", "order_id", "item_id", quantity_col, "unit_price"]
    ].merge(
        complete_orders,
        on="order_id",
        how="inner",
    )
    eligible = eligible.merge(shipment_ref, on="order_id", how="inner")
    eligible = eligible[eligible["ship_date"].notna()].reset_index(drop=True)

    if eligible.empty:
        logger.warning("No eligible shipped completed order lines; returning empty returns")
        return pd.DataFrame(columns=RETURN_COLUMNS)

    sample_rate = float(np.random.uniform(0.03, 0.08))
    n_returns = max(1, int(round(len(eligible) * sample_rate)))
    n_returns = min(n_returns, len(eligible))

    selected_idx = np.random.choice(eligible.index.to_numpy(), size=n_returns, replace=False)
    sampled = eligible.loc[selected_idx].reset_index(drop=True)

    qty_base = (
        pd.to_numeric(sampled[quantity_col], errors="coerce").fillna(1).clip(lower=1).astype(int)
    )
    qty_ratio = np.random.uniform(0.2, 1.0, size=n_returns)
    qty_returned = np.clip(
        np.floor(qty_base.to_numpy() * qty_ratio).astype(int), 1, qty_base.to_numpy()
    )

    reason_code = np.random.choice(REASON_CODES, size=n_returns, p=REASON_WEIGHTS)
    condition_code = np.random.choice(CONDITION_CODES, size=n_returns, p=CONDITION_WEIGHTS)
    restock_flag = condition_code == "Resalable"

    return_offsets = np.random.randint(5, 46, size=n_returns)
    return_dates = pd.to_datetime(sampled["ship_date"]) + pd.to_timedelta(return_offsets, unit="D")

    discount_array = np.vectorize(CONDITION_DISCOUNT.get)(condition_code)
    unit_price = pd.to_numeric(sampled["unit_price"], errors="coerce").fillna(0.0).to_numpy()
    credit_amount = np.round(unit_price * qty_returned * discount_array, 2)

    returns_df = pd.DataFrame(
        {
            "return_id": np.arange(1, n_returns + 1),
            "order_id": sampled["order_id"].astype(int).to_numpy(),
            "order_line_id": sampled["order_line_id"].astype(int).to_numpy(),
            "shipment_id": sampled["shipment_id"].astype(int).to_numpy(),
            "item_id": sampled["item_id"].astype(int).to_numpy(),
            "customer_id": sampled["customer_id"].astype(int).to_numpy(),
            "return_date": return_dates.dt.date,
            "reason_code": reason_code,
            "condition_code": condition_code,
            "qty_returned": qty_returned,
            "restock_flag": restock_flag,
            "credit_amount": credit_amount,
        }
    )

    logger.info("Generated %d return rows (sample rate %.2f%%)", len(returns_df), sample_rate * 100)
    return returns_df[RETURN_COLUMNS]

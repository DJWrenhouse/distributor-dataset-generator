"""Generate purchase-order records from inventory replenishment demand.

Schema fields:
    po_id
    vendor_id
    item_id
    dc_id
    po_number
    po_date
    expected_receipt_date
    actual_receipt_date
    qty_ordered
    qty_received
    po_unit_cost
    po_status

Business-driven realism notes:
- One PO is created per inventory row with qty_on_order > 0.
- Receipts may be full, partial, open, or cancelled based on status mix.
- Vendor fill-rate performance influences received quantities.
"""

import logging

import numpy as np
import pandas as pd

PURCHASE_ORDER_COLUMNS = [
    "po_id",
    "vendor_id",
    "item_id",
    "dc_id",
    "po_number",
    "po_date",
    "expected_receipt_date",
    "actual_receipt_date",
    "qty_ordered",
    "qty_received",
    "po_unit_cost",
    "po_status",
]


def generate_purchase_orders(
    vendors_df,
    items_df,
    vendor_item_details_df,
    dcs_df,
    inventory_df,
    vendor_performance_df,
):
    """Generate purchase-order rows for inventory currently on order.

    Args:
        vendors_df (pd.DataFrame): Vendor table (interface consistency).
        items_df (pd.DataFrame): Item table with lead-time values.
        vendor_item_details_df (pd.DataFrame): Vendor-item relationship table.
        dcs_df (pd.DataFrame): Distribution center table (interface consistency).
        inventory_df (pd.DataFrame): Inventory table with qty_on_order and restock context.
        vendor_performance_df (pd.DataFrame): Vendor fill-rate performance table.

    Returns:
        pd.DataFrame: Purchase orders in canonical column order.
    """
    del vendors_df, dcs_df

    logger = logging.getLogger(__name__)
    logger.info("Generating purchase orders from inventory on-order quantities")

    if (
        items_df is None
        or inventory_df is None
        or vendor_performance_df is None
        or items_df.empty
        or inventory_df.empty
        or vendor_performance_df.empty
    ):
        logger.warning("Insufficient inputs for purchase orders; returning empty DataFrame")
        return pd.DataFrame(columns=PURCHASE_ORDER_COLUMNS)

    inventory = inventory_df.copy()
    inventory["qty_on_order"] = (
        pd.to_numeric(inventory["qty_on_order"], errors="coerce").fillna(0).astype(int)
    )
    inventory = inventory[inventory["qty_on_order"] > 0].copy()

    if inventory.empty:
        logger.warning("No inventory rows with qty_on_order > 0; returning empty purchase orders")
        return pd.DataFrame(columns=PURCHASE_ORDER_COLUMNS)

    items_ref = items_df[["item_id", "lead_time_days"]].copy()
    items_ref["lead_time_days"] = (
        pd.to_numeric(items_ref["lead_time_days"], errors="coerce").fillna(7).clip(lower=1)
    )

    perf_ref = vendor_performance_df[["vendor_id", "fill_rate"]].copy()
    perf_ref["fill_rate"] = (
        pd.to_numeric(perf_ref["fill_rate"], errors="coerce").fillna(0.90).clip(0.5, 1.0)
    )

    inventory = inventory.merge(items_ref, on="item_id", how="left")
    inventory = inventory.merge(perf_ref, on="vendor_id", how="left")
    inventory["lead_time_days"] = inventory["lead_time_days"].fillna(7).astype(int)
    inventory["fill_rate"] = inventory["fill_rate"].fillna(0.90)

    cost_by_item = (
        inventory_df[["item_id", "last_cost"]]
        .dropna(subset=["last_cost"])
        .drop_duplicates(subset=["item_id"], keep="last")
        .rename(columns={"last_cost": "po_unit_cost"})
    )

    vendor_item_cost = (
        vendor_item_details_df[["vendor_id", "item_id"]]
        .drop_duplicates()
        .merge(cost_by_item, on="item_id", how="left")
    )

    inventory = inventory.merge(vendor_item_cost, on=["vendor_id", "item_id"], how="left")
    inventory["po_unit_cost"] = (
        pd.to_numeric(inventory["po_unit_cost"], errors="coerce")
        .fillna(pd.to_numeric(inventory["last_cost"], errors="coerce"))
        .fillna(pd.to_numeric(inventory["avg_cost"], errors="coerce"))
        .fillna(0.0)
    )

    n_rows = len(inventory)
    status_values = np.array(["Received", "Partial", "Open", "Cancelled"])
    status_probs = np.array([0.70, 0.15, 0.10, 0.05])
    po_status = np.random.choice(status_values, size=n_rows, p=status_probs)

    qty_ordered = inventory["qty_on_order"].to_numpy(dtype=int)
    fill_rate = inventory["fill_rate"].to_numpy(dtype=float)

    qty_received = np.zeros(n_rows, dtype=int)
    recv_mask = po_status == "Received"
    partial_mask = po_status == "Partial"
    qty_received[recv_mask] = np.clip(
        np.rint(fill_rate[recv_mask] * qty_ordered[recv_mask]).astype(int),
        0,
        qty_ordered[recv_mask],
    )
    partial_multiplier = np.random.uniform(0.45, 0.90, size=partial_mask.sum())
    qty_received[partial_mask] = np.clip(
        np.rint(fill_rate[partial_mask] * qty_ordered[partial_mask] * partial_multiplier).astype(
            int
        ),
        1,
        np.maximum(qty_ordered[partial_mask], 1),
    )

    last_restock = pd.to_datetime(inventory["last_restock_date"], errors="coerce")
    po_dates = last_restock - pd.to_timedelta(inventory["lead_time_days"], unit="D")
    po_dates = po_dates.fillna(last_restock).fillna(pd.Timestamp.today().normalize())

    expected_receipt = po_dates + pd.to_timedelta(inventory["lead_time_days"], unit="D")
    actual_offsets = np.rint(np.random.normal(loc=1.0, scale=3.0, size=n_rows)).astype(int)
    actual_receipt = expected_receipt + pd.to_timedelta(actual_offsets, unit="D")
    actual_receipt = pd.to_datetime(
        np.maximum(
            actual_receipt.values.astype("datetime64[ns]"),
            po_dates.values.astype("datetime64[ns]"),
        )
    )
    actual_receipt = pd.Series(actual_receipt)
    actual_receipt[po_status == "Open"] = pd.NaT

    seq = np.arange(1, n_rows + 1)
    po_numbers = [
        f"PO-{int(dc_id):03d}-{int(s):08d}" for dc_id, s in zip(inventory["dc_id"].to_numpy(), seq)
    ]

    po_df = pd.DataFrame(
        {
            "po_id": seq,
            "vendor_id": inventory["vendor_id"].astype(int).to_numpy(),
            "item_id": inventory["item_id"].astype(int).to_numpy(),
            "dc_id": inventory["dc_id"].astype(int).to_numpy(),
            "po_number": po_numbers,
            "po_date": po_dates.dt.date,
            "expected_receipt_date": expected_receipt.dt.date,
            "actual_receipt_date": actual_receipt.dt.date,
            "qty_ordered": qty_ordered,
            "qty_received": qty_received,
            "po_unit_cost": np.round(inventory["po_unit_cost"].to_numpy(dtype=float), 4),
            "po_status": po_status,
        }
    )

    logger.info("Generated %d purchase orders", len(po_df))
    return po_df[PURCHASE_ORDER_COLUMNS]

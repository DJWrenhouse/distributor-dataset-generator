"""Generate shipment headers and compute shipment-level cost rollups.

This module handles shipment header creation and the later aggregation step
that rolls line-level weights, volume, and costs into shipment totals.
"""

import logging
import random
from datetime import timedelta

import numpy as np
import pandas as pd

import data_generator.config as config
from data_generator.constants import SHIPMENT_TYPES


def generate_shipments(orders_df, dcs_df, carriers_df):
    """Create one shipment header row per order.

    Args:
        orders_df: Order-header table containing order dates and DC IDs.
        dcs_df: Distribution-center table (included for interface consistency).
        carriers_df: Carrier table used for carrier assignment.

    Returns:
        pd.DataFrame: Shipment-header table with dates, carrier, type, and
        placeholder totals/cost fields.
    """
    logger = logging.getLogger(__name__)
    total = len(orders_df)
    log_interval = min(config.LOG_INTERVAL, total) if total else config.LOG_INTERVAL
    logger.info("Generating %d shipments", total)
    rows = []
    carrier_ids = carriers_df["carrier_id"].values

    for idx, (_, order) in enumerate(orders_df.iterrows(), start=1):
        ship_date = order["order_date"] + timedelta(days=random.randint(0, 3))
        transit_days = random.randint(1, 7)
        promised_date = ship_date + timedelta(days=transit_days)
        delivery_date = promised_date + timedelta(days=random.randint(-2, 2))

        shipment_type = random.choices(
            SHIPMENT_TYPES, weights=[0.25, 0.10, 0.10, 0.35, 0.10, 0.10]
        )[0]

        carrier_id = int(np.random.choice(carrier_ids))

        rows.append(
            {
                "shipment_id": order["order_id"],
                "order_id": order["order_id"],
                "dc_id": order["dc_id"],
                "carrier_id": carrier_id,
                "ship_date": ship_date,
                "promised_date": promised_date,
                "delivery_date": delivery_date,
                "shipment_status": "Delivered",
                "shipment_type": shipment_type,
                "total_weight_lb": None,
                "total_cube_ft": None,
                "estimated_cost": None,
                "actual_cost": None,
            }
        )

        if idx % log_interval == 0:
            logger.info("Generated %d / %d shipments", idx, total)

    logger.info("Generated %d shipments", total)
    return pd.DataFrame(rows)


def generate_shipments_for_orders(orders_df, carriers_df):
    """Vectorized shipment-header generation for chunked/streaming workflows.

    Args:
        orders_df: Order chunk for shipment creation.
        carriers_df: Carrier table used for randomized assignment.

    Returns:
        pd.DataFrame: Shipment chunk aligned to provided order rows.
    """
    rng = np.random.default_rng()
    n = len(orders_df)
    if n == 0:
        return pd.DataFrame(
            columns=[
                "shipment_id",
                "order_id",
                "dc_id",
                "carrier_id",
                "ship_date",
                "promised_date",
                "delivery_date",
                "shipment_status",
                "shipment_type",
                "total_weight_lb",
                "total_cube_ft",
                "estimated_cost",
                "actual_cost",
            ]
        )

    carrier_ids = carriers_df["carrier_id"].to_numpy()

    order_ids = orders_df["order_id"].to_numpy()
    dc_ids = orders_df["dc_id"].to_numpy()
    order_dates = pd.to_datetime(orders_df["order_date"]).to_numpy()

    # ship_date = order_date + [0..3] days
    ship_offsets = rng.integers(0, 4, size=n)
    ship_dates = pd.to_datetime(order_dates) + pd.to_timedelta(ship_offsets, unit="D")

    transit_days = rng.integers(1, 8, size=n)
    promised_dates = ship_dates + pd.to_timedelta(transit_days, unit="D")
    delivery_offsets = rng.integers(-2, 3, size=n)
    delivery_dates = promised_dates + pd.to_timedelta(delivery_offsets, unit="D")

    # sample shipment types with given weights
    types = rng.choice(SHIPMENT_TYPES, size=n, p=[0.25, 0.10, 0.10, 0.35, 0.10, 0.10])

    carriers_choice = rng.choice(carrier_ids, size=n)

    df = pd.DataFrame(
        {
            "shipment_id": order_ids,
            "order_id": order_ids,
            "dc_id": dc_ids,
            "carrier_id": carriers_choice.astype(int),
            "ship_date": ship_dates,
            "promised_date": promised_dates,
            "delivery_date": delivery_dates,
            "shipment_status": ["Delivered"] * n,
            "shipment_type": types,
            "total_weight_lb": [None] * n,
            "total_cube_ft": [None] * n,
            "estimated_cost": [None] * n,
            "actual_cost": [None] * n,
        }
    )

    return df


def compute_shipment_totals_and_costs(shipments, shipment_lines, items_df, show_progress=True):
    """Roll shipment-line metrics up to shipment headers.

    Args:
        shipments: Shipment-header DataFrame to enrich.
        shipment_lines: Line-level shipment details.
        items_df: Item table (used to match shipment items).
        show_progress: Whether to emit progress logs while computing.

    Returns:
        pd.DataFrame: Updated shipment headers with total weight, total cube,
        estimated cost, and actual cost.
    """
    if shipment_lines is None or shipment_lines.empty:
        shipments = shipments.copy()
        shipments["total_weight_lb"] = 0.0
        shipments["total_cube_ft"] = 0.0
        shipments["estimated_cost"] = 0.0
        shipments["actual_cost"] = 0.0
        return shipments

    # Load catalog to get base shipping_cost_per_unit values
    catalog = pd.read_csv(config.ITEM_CATALOG_PATH)
    catalog = catalog.reset_index(drop=True)
    catalog["item_id"] = range(1, len(catalog) + 1)
    catalog_idx = catalog.set_index("item_id")

    totals = (
        shipment_lines.groupby("shipment_id")[["line_weight_lb", "line_cube_ft"]]
        .sum()
        .reset_index()
    )
    merged = shipments.merge(totals, on="shipment_id", how="left")

    # estimated cost based on item shipping cost per unit from catalog
    cost_lines = shipment_lines.merge(
        catalog_idx[["shipping_cost_per_unit"]],
        left_on="item_id",
        right_index=True,
        how="left",
    )
    cost_lines["line_ship_cost"] = (
        cost_lines["shipping_cost_per_unit"].fillna(0.0) * cost_lines["quantity_shipped"]
    )
    est_costs = cost_lines.groupby("shipment_id")["line_ship_cost"].sum().reset_index()
    merged = merged.merge(est_costs, on="shipment_id", how="left")

    logger = logging.getLogger(__name__)
    total = len(merged)
    log_interval = min(config.LOG_INTERVAL, total) if total else config.LOG_INTERVAL
    if show_progress:
        logger.info("Computing shipment costs for %d shipments", total)

    est_rows = []
    for idx, (_, row) in enumerate(merged.iterrows(), start=1):
        weight = row["line_weight_lb"] if pd.notnull(row["line_weight_lb"]) else 0.0
        cube = row["line_cube_ft"] if pd.notnull(row["line_cube_ft"]) else 0.0
        est = row["line_ship_cost"] if pd.notnull(row["line_ship_cost"]) else 0.0
        actual = est * random.uniform(0.95, 1.05)

        est_rows.append(
            {
                "shipment_id": row["shipment_id"],
                "total_weight_lb": round(weight, 3),
                "total_cube_ft": round(cube, 4),
                "estimated_cost": round(est, 2),
                "actual_cost": round(actual, 2),
            }
        )

        if show_progress and idx % log_interval == 0:
            logger.info("Computed costs for %d / %d shipments", idx, total)

    est_df = pd.DataFrame(est_rows)
    shipments = shipments.drop(
        columns=["total_weight_lb", "total_cube_ft", "estimated_cost", "actual_cost"]
    )
    shipments = shipments.merge(est_df, on="shipment_id", how="left")
    if show_progress:
        logger.info("Computed shipment costs for %d shipments", total)
    return shipments

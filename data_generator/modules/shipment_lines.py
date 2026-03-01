"""Generate shipment-line rows and cartonization metadata.

This module maps order lines to shipments, expands lines into cartons where
needed, and computes line-level weight/cube metrics.
"""

import logging

import pandas as pd

import data_generator.config as config


def _build_shipment_line_rows(merged, start_line_id):
    """Build shipment-line records at order-line granularity.

    Args:
        merged: Pre-joined DataFrame with shipment, line, item, and
            carrier data.
        start_line_id: Starting ID used to number output shipment lines.

    Returns:
        pd.DataFrame: One shipment-line row per merged order-line row.
    """
    rows = []

    # total weight per shipment for freight upcharge decision
    merged["line_weight_total"] = merged["weight_lb"] * merged["quantity_ordered"]
    weight_by_shipment = merged.groupby("shipment_id")["line_weight_total"].sum().to_dict()

    line_seq_by_order = {}
    first_line_for_shipment = set()

    line_id = start_line_id
    for row in merged.itertuples(index=False):
        qty = int(row.quantity_ordered)
        order_key = row.order_id
        line_seq = line_seq_by_order.get(order_key, 0) + 1
        carton_number = f"{order_key}-C{line_seq}"

        is_courier = (row.carrier_type == "courier") or (row.shipment_type == "local")
        freight_upcharge = 0.0
        if (
            row.shipment_id not in first_line_for_shipment
            and weight_by_shipment.get(row.shipment_id, 0) > 250
        ):
            freight_upcharge = 50.0
            first_line_for_shipment.add(row.shipment_id)

        rows.append(
            {
                "shipment_line_id": line_id,
                "shipment_id": row.shipment_id,
                "order_line_id": row.order_line_id,
                "item_id": row.item_id,
                "quantity_shipped": qty,
                "carton_number": carton_number,
                "line_weight_lb": round(float(row.weight_lb) * qty, 3),
                "line_cube_ft": round(float(row.cube_ft) * qty, 4),
                "freight_upcharge": freight_upcharge,
                "from_location_type": "DC",
                "from_location_id": (row.dc_id if hasattr(row, "dc_id") else None),
                "to_location_type": "customer",
                "to_location_id": row.order_id,
                "is_courier": is_courier,
            }
        )

        line_id += 1
        line_seq_by_order[order_key] = line_seq

    return pd.DataFrame(rows)


def generate_shipment_lines(shipments_df, order_lines_df, items_df, carriers_df):
    """Generate shipment lines for the full in-memory shipment dataset.

    Args:
        shipments_df: Full shipment-header table.
        order_lines_df: Full order-line table.
        items_df: Item table with weight and volume attributes.
        carriers_df: Carrier table with carrier type metadata.

    Returns:
        pd.DataFrame: Shipment-line table with one row per mapped order line.
    """
    # Merge order lines with shipment metadata
    merged = order_lines_df.merge(
        shipments_df[["order_id", "shipment_id", "carrier_id", "shipment_type", "dc_id"]],
        on="order_id",
        how="inner",
    )

    if merged.empty:
        return pd.DataFrame(
            columns=[
                "shipment_line_id",
                "shipment_id",
                "order_line_id",
                "item_id",
                "quantity_shipped",
                "carton_number",
                "line_weight_lb",
                "line_cube_ft",
                "freight_upcharge",
                "from_location_type",
                "from_location_id",
                "to_location_type",
                "to_location_id",
                "is_courier",
            ]
        )

    # Attach item weights/cube
    item_weights = items_df.set_index("item_id")[["weight_lb", "cube_ft"]]
    merged = merged.join(item_weights, on="item_id")

    # Attach carrier type
    carrier_types = carriers_df.set_index("carrier_id")["carrier_type"]
    merged["carrier_type"] = merged["carrier_id"].map(carrier_types)

    logger = logging.getLogger(__name__)
    n = len(merged)
    chunk_size = 200000
    total_chunks = (n + chunk_size - 1) // chunk_size
    chunks = []
    log_interval = max(1, config.LOG_INTERVAL // chunk_size)
    logger.info("Generating shipment lines in %d chunks", total_chunks)

    for chunk_idx in range(total_chunks):
        start = chunk_idx * chunk_size
        end = min(start + chunk_size, n)
        chunk = merged.iloc[start:end]

        chunks.append(_build_shipment_line_rows(chunk, start_line_id=start + 1))

        is_last_chunk = (chunk_idx + 1) == total_chunks
        if (chunk_idx + 1) % log_interval == 0 or is_last_chunk:
            logger.info(
                "Generated shipment lines chunk %d / %d",
                chunk_idx + 1,
                total_chunks,
            )

    return pd.concat(chunks, ignore_index=True)


def generate_shipment_lines_for_chunk(
    shipments_df,
    order_lines_df,
    items_df,
    carriers_df,
    start_line_id=1,
    show_progress=True,
):
    """Generate shipment lines for one streamed chunk.

    Args:
        shipments_df: Shipment chunk.
        order_lines_df: Order-line chunk.
        items_df: Item table with weight and volume attributes.
        carriers_df: Carrier table with carrier type metadata.
        start_line_id: Starting ID value for continuity across chunks.
        show_progress: Whether to emit per-chunk progress logs.

    Returns:
        pd.DataFrame: Chunk-level shipment-line rows ready for append output.
    """
    # Reuse vectorized approach but allow an arbitrary start ID
    merged = order_lines_df.merge(
        shipments_df[["order_id", "shipment_id", "carrier_id", "shipment_type", "dc_id"]],
        on="order_id",
        how="inner",
    )

    if merged.empty:
        return pd.DataFrame(
            columns=[
                "shipment_line_id",
                "shipment_id",
                "order_line_id",
                "item_id",
                "quantity_shipped",
                "carton_number",
                "line_weight_lb",
                "line_cube_ft",
                "freight_upcharge",
                "from_location_type",
                "from_location_id",
                "to_location_type",
                "to_location_id",
                "is_courier",
            ]
        )

    item_weights = items_df.set_index("item_id")[["weight_lb", "cube_ft"]]
    merged = merged.join(item_weights, on="item_id")

    carrier_types = carriers_df.set_index("carrier_id")["carrier_type"]
    merged["carrier_type"] = merged["carrier_id"].map(carrier_types)

    logger = logging.getLogger(__name__)
    n = len(merged)
    chunk_size = 200000
    total_chunks = (n + chunk_size - 1) // chunk_size
    chunks = []
    log_interval = max(1, config.LOG_INTERVAL // chunk_size)
    if show_progress:
        logger.info("Generating shipment lines in %d chunks", total_chunks)

    for chunk_idx in range(total_chunks):
        start = chunk_idx * chunk_size
        end = min(start + chunk_size, n)
        chunk = merged.iloc[start:end]

        chunks.append(_build_shipment_line_rows(chunk, start_line_id=start_line_id + start))

        if show_progress and (
            ((chunk_idx + 1) % log_interval == 0) or (chunk_idx + 1) == total_chunks
        ):
            logger.info(
                "Generated shipment lines chunk %d / %d",
                chunk_idx + 1,
                total_chunks,
            )

    return pd.concat(chunks, ignore_index=True)

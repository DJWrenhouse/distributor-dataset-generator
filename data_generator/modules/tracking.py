"""Generate shipment tracking events across delivery milestones.

This module expands each shipment into a fixed sequence of lifecycle events
for visibility and analytics use cases.
"""

import logging

import numpy as np
import pandas as pd

import data_generator.config as config


def generate_tracking_events(
    shipments_df,
    dcs_df,
    show_progress=True,
    log_summary=True,
    start_event_id=1,
):
    """Create a fixed set of milestone tracking events for each shipment.

    Args:
        shipments_df: Shipment-header table with ship dates and DC IDs.
        dcs_df: Distribution-center table used for event location fields.
        show_progress: Whether to log progress during chunk processing.
        log_summary: Whether to log start/end summary messages.
        start_event_id: Starting tracking_event_id (inclusive).

    Returns:
        pd.DataFrame: Tracking-event table with event IDs, timestamps,
        event types, and location fields.
    """
    logger = logging.getLogger(__name__)
    if log_summary:
        logger.info("Generating tracking events for %d shipments", len(shipments_df))

    # Merge shipments with DC info to pull location fields
    merged = shipments_df.merge(dcs_df[["dc_id", "city", "state", "zip"]], on="dc_id", how="left")

    if merged.empty:
        return pd.DataFrame(
            columns=[
                "tracking_event_id",
                "shipment_id",
                "event_timestamp",
                "event_type",
                "location_city",
                "location_state",
                "location_zip",
                "location_type",
            ]
        )

    n_ship = len(merged)
    repeats = 4
    chunk_size = 200000
    total_chunks = (n_ship + chunk_size - 1) // chunk_size
    chunks = []

    log_interval = max(1, config.LOG_INTERVAL // chunk_size)
    for chunk_idx in range(total_chunks):
        start = chunk_idx * chunk_size
        end = min(start + chunk_size, n_ship)
        chunk = merged.iloc[start:end]
        chunk_n = len(chunk)

        shipment_ids = np.repeat(chunk["shipment_id"].to_numpy(), repeats)
        base_times = np.repeat(pd.to_datetime(chunk["ship_date"]).to_numpy(), repeats)

        offsets = np.tile(np.array([0, 6, 12, 18], dtype="timedelta64[h]"), chunk_n)
        event_timestamps = pd.to_datetime(base_times.astype("datetime64[ns]") + offsets)

        event_types = np.tile(
            np.array(["departed_dc", "arrived_hub", "out_for_delivery", "delivered"]),
            chunk_n,
        )

        cities = np.repeat(chunk["city"].to_numpy(), repeats)
        states = np.repeat(chunk["state"].to_numpy(), repeats)
        zips = np.repeat(chunk["zip"].to_numpy(), repeats)

        location_type = np.where(
            np.tile(np.arange(repeats), chunk_n) == 0, "DC", "carrier_facility"
        )

        event_id_start = start_event_id + (start * repeats)
        event_id_end = start_event_id + ((start + chunk_n) * repeats)
        tracking_event_id = np.arange(event_id_start, event_id_end)

        chunks.append(
            pd.DataFrame(
                {
                    "tracking_event_id": tracking_event_id,
                    "shipment_id": shipment_ids,
                    "event_timestamp": event_timestamps,
                    "event_type": event_types,
                    "location_city": cities,
                    "location_state": states,
                    "location_zip": zips,
                    "location_type": location_type,
                }
            )
        )

        if show_progress and (
            ((chunk_idx + 1) % log_interval == 0) or (chunk_idx + 1) == total_chunks
        ):
            logger.info("Generated tracking events chunk %d / %d", chunk_idx + 1, total_chunks)

    df = pd.concat(chunks, ignore_index=True)
    if log_summary:
        logger.info("Generated %d tracking events", len(df))
    return df

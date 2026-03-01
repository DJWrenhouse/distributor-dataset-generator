"""Regression tests for tracking event ID generation."""

import pandas as pd

from data_generator.modules.tracking import generate_tracking_events


def _sample_shipments(start_shipment_id: int, count: int) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "shipment_id": range(start_shipment_id, start_shipment_id + count),
            "ship_date": ["2025-01-01"] * count,
            "dc_id": [1] * count,
        }
    )


def _sample_dcs() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "dc_id": [1],
            "city": ["Dallas"],
            "state": ["TX"],
            "zip": ["75001"],
        }
    )


def test_tracking_event_ids_do_not_reset_across_chunks():
    """Ensure streamed tracking IDs remain globally unique and sequential."""
    dcs = _sample_dcs()

    first_chunk = generate_tracking_events(
        _sample_shipments(start_shipment_id=1, count=2),
        dcs,
        show_progress=False,
        log_summary=False,
    )
    second_chunk = generate_tracking_events(
        _sample_shipments(start_shipment_id=3, count=2),
        dcs,
        show_progress=False,
        log_summary=False,
        start_event_id=len(first_chunk) + 1,
    )

    combined = pd.concat([first_chunk, second_chunk], ignore_index=True)
    assert combined["tracking_event_id"].is_unique
    expected_ids = list(range(1, len(combined) + 1))
    assert combined["tracking_event_id"].tolist() == expected_ids

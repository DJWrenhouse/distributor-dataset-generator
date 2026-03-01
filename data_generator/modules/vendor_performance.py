"""
Generate realistic vendor performance metrics.

This module produces one performance record per vendor, using business-driven
rules that reflect how real distributors evaluate supplier reliability.

Output fields match the SQL schema:

    vendor_id              BIGINT PRIMARY KEY
    on_time_delivery_rate  NUMERIC(5,2)
    avg_days_late          NUMERIC(6,2)
    defect_rate            NUMERIC(5,2)
    fill_rate              NUMERIC(5,2)
    last_audit_date        DATE
"""

import logging
import random
from datetime import datetime, timedelta

import pandas as pd


def generate_vendor_performance(vendors_df):
    """
    Generate performance metrics for each vendor.

    Args:
        vendors_df (pd.DataFrame): Vendor master table containing vendor_id.

    Returns:
        pd.DataFrame: Vendor performance table.
    """
    logger = logging.getLogger(__name__)
    total = len(vendors_df)
    logger.info("Generating vendor performance metrics for %d vendors", total)

    rows = []

    # Business-driven realism:
    # Larger vendors (higher vendor_id) tend to perform better.
    # Smaller vendors have more variability.
    for _, vendor in vendors_df.iterrows():
        vid = vendor["vendor_id"]

        # Base performance improves slightly with vendor_id
        size_factor = min(1.0, 0.6 + (vid / (total * 1.5)))

        # On-time delivery rate: 70–99 percent
        on_time = round(random.uniform(0.70, 0.95) * size_factor + random.uniform(0.00, 0.05), 2)
        on_time = min(on_time, 0.99)

        # Average days late: 0–10 days, smaller for better vendors
        avg_days_late = round(random.uniform(0.0, 8.0) * (1.2 - size_factor), 2)

        # Defect rate: 0.1–5 percent
        defect_rate = round(random.uniform(0.001, 0.03) * (1.3 - size_factor), 3)
        defect_rate = min(defect_rate, 0.05)

        # Fill rate: 85–99 percent
        fill_rate = round(random.uniform(0.85, 0.97) * size_factor + random.uniform(0.00, 0.03), 2)
        fill_rate = min(fill_rate, 0.99)

        # Last audit date: within last 2 years
        days_back = random.randint(0, 730)
        last_audit = (datetime.today() - timedelta(days=days_back)).date()

        rows.append(
            {
                "vendor_id": vid,
                "on_time_delivery_rate": on_time,
                "avg_days_late": avg_days_late,
                "defect_rate": defect_rate,
                "fill_rate": fill_rate,
                "last_audit_date": last_audit,
            }
        )

    logger.info("Generated vendor performance records")
    return pd.DataFrame(rows)

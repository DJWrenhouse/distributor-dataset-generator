"""
Generate vendor-item relationship details.

This module produces realistic vendor-item associations and purchasing rules,
matching the SQL schema:

    vendor_item_id     BIGSERIAL PRIMARY KEY
    vendor_id          BIGINT NOT NULL
    item_id            BIGINT NOT NULL
    vendor_item_number VARCHAR(100)
    moq                INTEGER
    order_multiple     INTEGER
    vendor_rank        INTEGER
    UNIQUE (vendor_id, item_id)

Business-driven realism:
- Each vendor supplies a subset of items.
- Better-performing vendors (from vendor_performance) receive better ranks.
- MOQ and order multiples depend on vendor rank and item cube/weight.
"""

import logging
import random

import numpy as np
import pandas as pd


def generate_vendor_item_details(vendors_df, items_df, vendor_performance_df):
    """
    Generate vendor-item details with realistic business logic.

    Args:
        vendors_df (pd.DataFrame): Vendor master table.
        items_df (pd.DataFrame): Item master table.
        vendor_performance_df (pd.DataFrame): Vendor performance metrics.

    Returns:
        pd.DataFrame: Vendor-item details table.
    """
    logger = logging.getLogger(__name__)
    logger.info("Generating vendor-item details")

    rows = []
    vendor_item_id = 1

    perf = vendor_performance_df.set_index("vendor_id")
    item_ids = items_df["item_id"].to_numpy()
    total_items = len(item_ids)
    cube_by_item = items_df.set_index("item_id")["cube_ft"].fillna(0.1)
    total_vendors = len(vendors_df)
    log_interval = max(1, total_vendors // 10)

    for idx, (_, vendor) in enumerate(vendors_df.iterrows(), start=1):
        vid = int(vendor["vendor_id"])
        vendor_prefix = str(vendor["vendor_name"])[:3].upper()

        perf_score = (
            float(perf.loc[vid, "fill_rate"]) * 0.5
            + float(perf.loc[vid, "on_time_delivery_rate"]) * 0.5
        )

        vendor_rank = max(1, min(5, int(round((1 - perf_score) * 5))))

        num_items = int(
            np.clip(
                random.gauss(mu=total_items * 0.15, sigma=total_items * 0.05),
                20,
                total_items * 0.4,
            )
        )

        supplied_items = np.random.choice(item_ids, size=num_items, replace=False)

        for item_id in supplied_items:
            vendor_item_number = f"{vendor_prefix}-{int(item_id):06d}"

            base_moq = random.choice([6, 12, 24, 48])
            moq = max(1, int(base_moq * (vendor_rank / 3)))

            cube = float(cube_by_item.get(int(item_id), 0.1))

            if cube < 0.1:
                order_multiple = random.choice([1, 2, 5])
            elif cube < 0.5:
                order_multiple = random.choice([2, 5, 10])
            else:
                order_multiple = random.choice([5, 10, 20])

            order_multiple = max(1, int(order_multiple * (vendor_rank / 3)))

            rows.append(
                {
                    "vendor_item_id": vendor_item_id,
                    "vendor_id": vid,
                    "item_id": int(item_id),
                    "vendor_item_number": vendor_item_number,
                    "moq": moq,
                    "order_multiple": order_multiple,
                    "vendor_rank": vendor_rank,
                }
            )

            vendor_item_id += 1

        if idx % log_interval == 0 or idx == total_vendors:
            logger.info(
                "Processed %d / %d vendors (%d vendor-item records)", idx, total_vendors, len(rows)
            )

    logger.info("Generated %d vendor-item detail records", len(rows))
    return pd.DataFrame(rows)

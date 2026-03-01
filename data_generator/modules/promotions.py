"""Generate promotion events and order-to-promotion bridge records.

Schema fields (promotions):
    promo_id, promo_name, promo_code, start_date, end_date,
    discount_type, discount_value, applies_to, category, customer_segment

Schema fields (promo_orders):
    promo_order_id, promo_id, order_id

Business-driven realism notes:
- Promotions span 7-30 days with mixed discount mechanics.
- Orders are probabilistically tagged when placed during active promos.
"""

import logging

import numpy as np
import pandas as pd

import data_generator.config as config

PROMOTIONS_COLUMNS = [
    "promo_id",
    "promo_name",
    "promo_code",
    "start_date",
    "end_date",
    "discount_type",
    "discount_value",
    "applies_to",
    "category",
    "customer_segment",
]

PROMO_ORDERS_COLUMNS = ["promo_order_id", "promo_id", "order_id"]


def generate_promotions(items_df, customers_df):
    """Generate synthetic promotion events across the configured date window.

    Args:
        items_df (pd.DataFrame): Item table used for category selection.
        customers_df (pd.DataFrame): Customer table used for segment selection.

    Returns:
        pd.DataFrame: Promotions table in canonical column order.
    """
    logger = logging.getLogger(__name__)
    logger.info("Generating promotions")

    if items_df is None or items_df.empty:
        logger.warning("Missing items table; returning empty promotions")
        return pd.DataFrame(columns=PROMOTIONS_COLUMNS)

    n_promos = int(np.random.randint(300, 501))
    start_ts = pd.Timestamp(config.START_DATE).normalize()
    end_ts = pd.Timestamp(config.END_DATE).normalize()
    max_offset = max(0, (end_ts - start_ts).days - 6)

    start_offsets = np.random.randint(0, max_offset + 1, size=n_promos)
    durations = np.random.randint(7, 31, size=n_promos)
    promo_starts = start_ts + pd.to_timedelta(start_offsets, unit="D")
    promo_ends = promo_starts + pd.to_timedelta(durations - 1, unit="D")
    promo_ends = pd.to_datetime(
        np.minimum(promo_ends.values.astype("datetime64[ns]"), end_ts.to_datetime64())
    )

    discount_type = np.random.choice(
        np.array(["PCTOFF", "FIXEDOFF", "BOGO"]),
        size=n_promos,
        p=[0.60, 0.25, 0.15],
    )
    discount_value = np.zeros(n_promos, dtype=float)
    pct_mask = discount_type == "PCTOFF"
    fixed_mask = discount_type == "FIXEDOFF"
    bogo_mask = discount_type == "BOGO"
    discount_value[pct_mask] = np.round(np.random.uniform(5.0, 25.0, size=pct_mask.sum()), 2)
    discount_value[fixed_mask] = np.round(np.random.uniform(5.0, 50.0, size=fixed_mask.sum()), 2)
    discount_value[bogo_mask] = 1.0

    applies_to = np.random.choice(np.array(["CATEGORY", "ITEM"]), size=n_promos, p=[0.70, 0.30])
    categories = items_df["category"].dropna().astype(str).unique()
    if len(categories) == 0:
        categories = np.array(["General"])
    category_values = np.where(
        applies_to == "CATEGORY",
        np.random.choice(categories, size=n_promos),
        pd.NA,
    )

    segment_values = np.full(n_promos, pd.NA, dtype=object)
    if customers_df is not None and not customers_df.empty and "segment" in customers_df.columns:
        segments = customers_df["segment"].dropna().astype(str).unique()
        if len(segments) > 0:
            has_segment = np.random.choice(np.array([True, False]), size=n_promos, p=[0.50, 0.50])
            segment_values[has_segment] = np.random.choice(segments, size=has_segment.sum())

    promo_ids = np.arange(1, n_promos + 1)
    promo_year = pd.to_datetime(promo_starts).year.to_numpy()
    promo_codes = [f"PROMO-{int(y)}-{int(i):04d}" for i, y in zip(promo_ids, promo_year)]

    promotions_df = pd.DataFrame(
        {
            "promo_id": promo_ids,
            "promo_name": [f"Promotion {int(i):04d}" for i in promo_ids],
            "promo_code": promo_codes,
            "start_date": pd.to_datetime(promo_starts).date,
            "end_date": pd.to_datetime(promo_ends).date,
            "discount_type": discount_type,
            "discount_value": discount_value,
            "applies_to": applies_to,
            "category": category_values,
            "customer_segment": segment_values,
        }
    )

    logger.info("Generated %d promotions", len(promotions_df))
    return promotions_df[PROMOTIONS_COLUMNS]


def generate_promo_orders(promotions_df, orders_df):
    """Generate order-to-promotion assignments for active promo windows.

    Args:
        promotions_df (pd.DataFrame): Promotions with start/end ranges.
        orders_df (pd.DataFrame): Orders with order dates.

    Returns:
        pd.DataFrame: Promo-order bridge rows in canonical column order.
    """
    logger = logging.getLogger(__name__)
    logger.info("Generating promo_orders bridge")

    if promotions_df is None or orders_df is None or promotions_df.empty or orders_df.empty:
        logger.warning("Missing inputs for promo_orders; returning empty DataFrame")
        return pd.DataFrame(columns=PROMO_ORDERS_COLUMNS)

    promo = promotions_df[["promo_id", "start_date", "end_date"]].copy()
    promo["start_date"] = pd.to_datetime(promo["start_date"], errors="coerce").dt.normalize()
    promo["end_date"] = pd.to_datetime(promo["end_date"], errors="coerce").dt.normalize()
    promo = promo.dropna(subset=["start_date", "end_date"])
    if promo.empty:
        return pd.DataFrame(columns=PROMO_ORDERS_COLUMNS)

    orders = orders_df[["order_id", "order_date"]].copy()
    orders["order_date"] = pd.to_datetime(orders["order_date"], errors="coerce").dt.normalize()
    orders = orders.dropna(subset=["order_date"]).reset_index(drop=True)
    if orders.empty:
        return pd.DataFrame(columns=PROMO_ORDERS_COLUMNS)

    promo_starts = promo["start_date"].to_numpy(dtype="datetime64[ns]")
    promo_ends = promo["end_date"].to_numpy(dtype="datetime64[ns]")
    promo_ids = promo["promo_id"].to_numpy(dtype=int)

    assignments = []
    for dt in orders["order_date"].drop_duplicates().to_numpy(dtype="datetime64[ns]"):
        active_mask = (promo_starts <= dt) & (promo_ends >= dt)
        active_promos = promo_ids[active_mask]
        if active_promos.size == 0:
            continue

        date_order_ids = orders.loc[orders["order_date"] == pd.Timestamp(dt), "order_id"].to_numpy(
            dtype=int
        )
        assign_prob = float(np.random.uniform(0.10, 0.15))
        draw = np.random.random(size=len(date_order_ids))
        selected_order_ids = date_order_ids[draw < assign_prob]
        if selected_order_ids.size == 0:
            continue

        selected_promos = np.random.choice(
            active_promos, size=selected_order_ids.size, replace=True
        )
        assignments.append(
            pd.DataFrame(
                {
                    "promo_id": selected_promos.astype(int),
                    "order_id": selected_order_ids.astype(int),
                }
            )
        )

    if not assignments:
        logger.info("No promo-order assignments generated")
        return pd.DataFrame(columns=PROMO_ORDERS_COLUMNS)

    promo_orders_df = pd.concat(assignments, ignore_index=True)
    promo_orders_df.insert(0, "promo_order_id", np.arange(1, len(promo_orders_df) + 1))

    logger.info("Generated %d promo_orders rows", len(promo_orders_df))
    return promo_orders_df[PROMO_ORDERS_COLUMNS]

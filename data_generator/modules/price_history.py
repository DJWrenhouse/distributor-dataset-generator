"""Generate effective-dated list and contract price history records.

Schema fields:
    price_id
    item_id
    customer_id
    effective_date
    end_date
    list_price
    discount_pct
    net_price
    promo_flag
    channel

Business-driven realism notes:
- Every item has a list-price history with 2-5 effective periods.
- Prices follow bounded random-walk changes over time.
- High-preference customer-item pairs receive contract discounts.
"""

import logging

import numpy as np
import pandas as pd

import data_generator.config as config

PRICE_HISTORY_COLUMNS = [
    "price_id",
    "item_id",
    "customer_id",
    "effective_date",
    "end_date",
    "list_price",
    "discount_pct",
    "net_price",
    "promo_flag",
    "channel",
]


def _empty_price_history():
    return pd.DataFrame(columns=PRICE_HISTORY_COLUMNS)


def generate_price_history(items_df, item_costs_df, customers_df, customer_item_preferences_df):
    """Generate item-level list price history plus contract price rows.

    Args:
        items_df (pd.DataFrame): Item table with `item_id`.
        item_costs_df (pd.DataFrame): Item cost history containing baseline `unit_price`.
        customers_df (pd.DataFrame): Customer table (interface consistency).
        customer_item_preferences_df (pd.DataFrame): Customer-item preference scores.

    Returns:
        pd.DataFrame: Price history table in canonical column order.
    """
    del customers_df

    logger = logging.getLogger(__name__)
    logger.info("Generating price history")

    if items_df is None or item_costs_df is None or items_df.empty or item_costs_df.empty:
        logger.warning("Missing items/item_costs inputs; returning empty price history")
        return _empty_price_history()

    item_ref = items_df[["item_id"]].drop_duplicates().copy()
    cost_ref = item_costs_df[["item_id", "effective_date", "unit_price"]].copy()
    cost_ref["effective_date"] = pd.to_datetime(cost_ref["effective_date"], errors="coerce")
    cost_ref["unit_price"] = pd.to_numeric(cost_ref["unit_price"], errors="coerce")
    cost_ref = cost_ref.dropna(subset=["effective_date", "unit_price"]).sort_values(
        ["item_id", "effective_date"]
    )

    latest_cost = cost_ref.drop_duplicates(subset=["item_id"], keep="last")[
        ["item_id", "unit_price"]
    ]
    base_items = item_ref.merge(latest_cost, on="item_id", how="left")
    base_items["unit_price"] = base_items["unit_price"].fillna(10.0)

    start_ts = pd.Timestamp(config.START_DATE).normalize()
    end_ts = pd.Timestamp(config.END_DATE).normalize()
    total_days = max(1, (end_ts - start_ts).days)

    standard_rows = []
    for _, row in base_items.iterrows():
        item_id = int(row["item_id"])
        baseline = float(row["unit_price"])

        n_versions = int(np.random.randint(2, 6))
        spacing = np.linspace(0, total_days, n_versions).astype(int)
        jitter = np.random.randint(-10, 11, size=n_versions)
        offsets = np.clip(spacing + jitter, 0, total_days)
        offsets[0] = 0
        offsets[-1] = total_days
        offsets = np.sort(offsets)

        list_prices = np.zeros(n_versions, dtype=float)
        list_prices[0] = max(0.01, baseline)
        if n_versions > 1:
            pct_change = np.random.uniform(0.03, 0.12, size=n_versions - 1)
            direction = np.random.choice(np.array([-1.0, 1.0]), size=n_versions - 1)
            change_factors = 1.0 + (pct_change * direction)
            for idx in range(1, n_versions):
                list_prices[idx] = max(0.01, list_prices[idx - 1] * change_factors[idx - 1])

        effective_dates = (start_ts + pd.to_timedelta(offsets, unit="D")).date
        end_dates = np.full(n_versions, np.datetime64("NaT"), dtype="datetime64[ns]")
        if n_versions > 1:
            end_dates[:-1] = pd.to_datetime(effective_dates[1:]) - pd.to_timedelta(1, unit="D")

        channels = np.random.choice(
            np.array(["Standard", "Catalog"]), size=n_versions, p=[0.8, 0.2]
        )
        for idx in range(n_versions):
            standard_rows.append(
                {
                    "item_id": item_id,
                    "customer_id": pd.NA,
                    "effective_date": effective_dates[idx],
                    "end_date": pd.to_datetime(end_dates[idx]).date()
                    if pd.notna(end_dates[idx])
                    else pd.NaT,
                    "list_price": round(float(list_prices[idx]), 4),
                    "discount_pct": 0.0,
                    "net_price": round(float(list_prices[idx]), 4),
                    "promo_flag": False,
                    "channel": channels[idx],
                }
            )

    standard_df = pd.DataFrame(standard_rows)
    if standard_df.empty:
        return _empty_price_history()

    contract_df = pd.DataFrame(
        columns=[
            "item_id",
            "customer_id",
            "effective_date",
            "end_date",
            "list_price",
            "discount_pct",
            "net_price",
            "promo_flag",
            "channel",
        ]
    )

    if customer_item_preferences_df is not None and not customer_item_preferences_df.empty:
        prefs = customer_item_preferences_df[["customer_id", "item_id", "preference_score"]].copy()
        prefs["preference_score"] = pd.to_numeric(prefs["preference_score"], errors="coerce")
        prefs = prefs.dropna(subset=["preference_score"])
        if not prefs.empty:
            threshold = float(prefs["preference_score"].quantile(0.80))
            top_prefs = prefs[prefs["preference_score"] >= threshold][
                ["customer_id", "item_id"]
            ].drop_duplicates()
            latest_list = standard_df.sort_values(["item_id", "effective_date"]).drop_duplicates(
                subset=["item_id"], keep="last"
            )[["item_id", "effective_date", "list_price"]]
            contract_df = top_prefs.merge(latest_list, on="item_id", how="inner")
            if not contract_df.empty:
                discount_pct = np.random.uniform(0.05, 0.15, size=len(contract_df))
                contract_df["discount_pct"] = np.round(discount_pct, 4)
                contract_df["net_price"] = np.round(
                    contract_df["list_price"].to_numpy(dtype=float) * (1.0 - discount_pct), 4
                )
                contract_df["end_date"] = pd.NaT
                contract_df["promo_flag"] = False
                contract_df["channel"] = "Contract"

    all_prices = pd.concat([standard_df, contract_df], ignore_index=True, sort=False)
    all_prices = all_prices[
        [
            "item_id",
            "customer_id",
            "effective_date",
            "end_date",
            "list_price",
            "discount_pct",
            "net_price",
            "promo_flag",
            "channel",
        ]
    ].copy()

    all_prices.insert(0, "price_id", np.arange(1, len(all_prices) + 1))
    all_prices["list_price"] = np.round(
        pd.to_numeric(all_prices["list_price"], errors="coerce").fillna(0.0), 4
    )
    all_prices["discount_pct"] = np.round(
        pd.to_numeric(all_prices["discount_pct"], errors="coerce").fillna(0.0),
        4,
    )
    all_prices["net_price"] = np.round(
        pd.to_numeric(all_prices["net_price"], errors="coerce").fillna(0.0), 4
    )

    logger.info("Generated %d price history rows", len(all_prices))
    return all_prices[PRICE_HISTORY_COLUMNS]

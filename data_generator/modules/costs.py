"""Generate carrier and transportation-cost reference tables.

This module produces carrier master data plus supporting driver-rate and fuel
cost tables used for shipment cost simulation.
"""

import logging
import random
from datetime import datetime, timedelta

import pandas as pd

import data_generator.config as config
from data_generator.config import (
    DRIVER_HOURLY_RANGE,
    DRIVER_PER_MILE_RANGE,
    DRIVER_PER_STOP_RANGE,
    FUEL_BASE_DIESEL,
    FUEL_BASE_GAS,
)
from data_generator.constants import US_STATES


def generate_carriers():
    """Generate carrier master data with baseline pricing inputs.

    Returns:
        pd.DataFrame: Carrier table containing carrier type, service level,
        base rates, and fuel surcharge percentage.
    """
    templates = [
        ("UPS", "parcel", "ground"),
        ("FedEx", "parcel", "2-day"),
        ("USPS", "parcel", "ground"),
        ("Regional LTL 1", "LTL", "standard"),
        ("Regional LTL 2", "LTL", "standard"),
        ("Local Courier 1", "courier", "same-day"),
        ("Local Courier 2", "courier", "same-day"),
        ("Regional Parcel 1", "parcel", "ground"),
        ("Company Fleet 1", "company", "standard"),
        ("Company Fleet 2", "company", "standard"),
    ]

    generated_types = ["parcel", "LTL", "courier", "company"]
    default_levels = {
        "parcel": "ground",
        "LTL": "standard",
        "courier": "same-day",
        "company": "standard",
    }

    target = max(1, int(getattr(config, "N_CARRIERS", 10)))

    logger = logging.getLogger(__name__)
    rows = []
    logger.info("Generating %d carriers", target)
    for carrier_id in range(1, target + 1):
        if carrier_id <= len(templates):
            name, carrier_type, service_level = templates[carrier_id - 1]
        else:
            index = (carrier_id - len(templates) - 1) % len(generated_types)
            carrier_type = generated_types[index]
            service_level = default_levels[carrier_type]
            name = f"Synthetic {carrier_type.title()} Carrier {carrier_id}"

        if carrier_type == "parcel":
            base_rate_per_lb = round(random.uniform(0.40, 1.20), 4)
            base_rate_per_mile = None
        elif carrier_type == "LTL":
            base_rate_per_lb = round(random.uniform(0.10, 0.40), 4)
            base_rate_per_mile = round(random.uniform(1.50, 3.50), 4)
        elif carrier_type == "courier":
            base_rate_per_lb = round(random.uniform(0.50, 1.50), 4)
            base_rate_per_mile = round(random.uniform(1.00, 2.50), 4)
        else:  # company
            base_rate_per_lb = round(random.uniform(0.10, 0.40), 4)
            base_rate_per_mile = round(random.uniform(1.50, 3.50), 4)

        fuel_surcharge_pct = round(random.uniform(5.0, 18.0), 2)

        rows.append(
            {
                "carrier_id": carrier_id,
                "carrier_name": name,
                "carrier_type": carrier_type,
                "service_level": service_level,
                "base_rate_per_lb": base_rate_per_lb,
                "base_rate_per_mile": base_rate_per_mile,
                "fuel_surcharge_pct": fuel_surcharge_pct,
            }
        )

    logger.info("Generated %d carriers", target)
    return pd.DataFrame(rows)


def generate_driver_costs(carriers_df):
    """Generate driver cost records by carrier and cost type.

    Args:
        carriers_df: Carrier reference table.

    Returns:
        pd.DataFrame: Driver-cost table including hourly, per-mile, and
        per-stop rate records.
    """
    logger = logging.getLogger(__name__)
    rows = []
    today = datetime.today().date()
    total = len(carriers_df)
    log_interval = min(config.LOG_INTERVAL, total) if total else config.LOG_INTERVAL
    logger.info("Generating driver costs for %d carriers", total)

    min_ltl_hourly = None
    min_ltl_per_mile = None
    min_ltl_per_stop = None

    non_company = carriers_df[carriers_df["carrier_type"] != "company"]
    company = carriers_df[carriers_df["carrier_type"] == "company"]

    for idx, (_, carrier) in enumerate(non_company.iterrows(), start=1):
        hourly = round(random.uniform(*DRIVER_HOURLY_RANGE), 4)
        per_mile = round(random.uniform(*DRIVER_PER_MILE_RANGE), 4)
        per_stop = round(random.uniform(*DRIVER_PER_STOP_RANGE), 4)

        if carrier["carrier_type"] == "LTL":
            min_ltl_hourly = hourly if min_ltl_hourly is None else min(min_ltl_hourly, hourly)
            min_ltl_per_mile = (
                per_mile if min_ltl_per_mile is None else min(min_ltl_per_mile, per_mile)
            )
            min_ltl_per_stop = (
                per_stop if min_ltl_per_stop is None else min(min_ltl_per_stop, per_stop)
            )

        rows.append(
            {
                "driver_cost_id": len(rows) + 1,
                "carrier_id": carrier["carrier_id"],
                "company_driver": False,
                "cost_type": "hourly",
                "rate_amount": hourly,
                "effective_date": today - timedelta(days=random.randint(0, 365)),
            }
        )
        rows.append(
            {
                "driver_cost_id": len(rows) + 1,
                "carrier_id": carrier["carrier_id"],
                "company_driver": False,
                "cost_type": "per_mile",
                "rate_amount": per_mile,
                "effective_date": today - timedelta(days=random.randint(0, 365)),
            }
        )
        rows.append(
            {
                "driver_cost_id": len(rows) + 1,
                "carrier_id": carrier["carrier_id"],
                "company_driver": False,
                "cost_type": "per_stop",
                "rate_amount": per_stop,
                "effective_date": today - timedelta(days=random.randint(0, 365)),
            }
        )

        if idx % log_interval == 0:
            logger.info(
                "Generated driver costs for %d / %d carriers",
                idx,
                total,
            )

    min_ltl_hourly = min_ltl_hourly or DRIVER_HOURLY_RANGE[0]
    min_ltl_per_mile = min_ltl_per_mile or DRIVER_PER_MILE_RANGE[0]
    min_ltl_per_stop = min_ltl_per_stop or DRIVER_PER_STOP_RANGE[0]

    for _, carrier in company.iterrows():
        rows.append(
            {
                "driver_cost_id": len(rows) + 1,
                "carrier_id": carrier["carrier_id"],
                "company_driver": True,
                "cost_type": "hourly",
                "rate_amount": round(min_ltl_hourly * 0.8, 4),
                "effective_date": today - timedelta(days=random.randint(0, 365)),
            }
        )
        rows.append(
            {
                "driver_cost_id": len(rows) + 1,
                "carrier_id": carrier["carrier_id"],
                "company_driver": True,
                "cost_type": "per_mile",
                "rate_amount": round(min_ltl_per_mile * 0.8, 4),
                "effective_date": today - timedelta(days=random.randint(0, 365)),
            }
        )
        rows.append(
            {
                "driver_cost_id": len(rows) + 1,
                "carrier_id": carrier["carrier_id"],
                "company_driver": True,
                "cost_type": "per_stop",
                "rate_amount": round(min_ltl_per_stop * 0.8, 4),
                "effective_date": today - timedelta(days=random.randint(0, 365)),
            }
        )

    logger.info("Generated driver costs for %d carriers", total)
    return pd.DataFrame(rows)


def generate_fuel_costs():
    """Generate state-level diesel and gasoline price snapshots.

    Returns:
        pd.DataFrame: Fuel-cost table with state, fuel type, price, and
        effective-date fields.
    """
    logger = logging.getLogger(__name__)
    rows = []
    today = datetime.today().date()
    total = len(US_STATES)
    log_interval = min(config.LOG_INTERVAL, total) if total else config.LOG_INTERVAL
    logger.info("Generating fuel costs for %d states", total)

    fuel_ranges = [
        ("diesel", FUEL_BASE_DIESEL),
        ("gas", FUEL_BASE_GAS),
    ]

    for idx, state in enumerate(US_STATES, start=1):
        for fuel_type, base_range in fuel_ranges:
            base = random.uniform(*base_range)
            noise = random.uniform(-0.20, 0.20)
            price = max(1.50, base + noise)

            rows.append(
                {
                    "fuel_cost_id": len(rows) + 1,
                    "region": None,
                    "state": state,
                    "fuel_type": fuel_type,
                    "cost_per_gallon": round(price, 4),
                    "effective_date": today - timedelta(days=random.randint(0, 60)),
                }
            )

        if idx % log_interval == 0:
            logger.info(
                "Generated fuel costs for %d / %d states",
                idx,
                total,
            )

    logger.info("Generated fuel costs for %d states", total)
    return pd.DataFrame(rows)

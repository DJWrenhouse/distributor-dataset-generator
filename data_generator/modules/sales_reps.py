"""Generate sales representative master records and customer contacts.

Schema fields (sales_reps):
    rep_id, rep_name, email, region, territory, hire_date

Schema fields (customer_contacts):
    contact_id, customer_id, rep_id, contact_name, role, email, last_contact_date

Business-driven realism notes:
- Reps are regionally distributed and assigned territories.
- Contacts are aligned to customer region with segment-sensitive role logic.
"""

import logging

import numpy as np
import pandas as pd
from faker import Faker

import data_generator.config as config
from data_generator.constants import REGIONS

SALES_REPS_COLUMNS = ["rep_id", "rep_name", "email", "region", "territory", "hire_date"]
CUSTOMER_CONTACTS_COLUMNS = [
    "contact_id",
    "customer_id",
    "rep_id",
    "contact_name",
    "role",
    "email",
    "last_contact_date",
]


def generate_sales_reps():
    """Generate a fixed population of sales reps.

    Returns:
        pd.DataFrame: Sales rep records in canonical column order.
    """
    logger = logging.getLogger(__name__)
    logger.info("Generating sales reps")

    fake = Faker()
    n_reps = 75
    start_date = pd.Timestamp(config.START_DATE).normalize()

    region_choices = np.random.choice(np.array(REGIONS), size=n_reps, replace=True)
    territory_numbers = np.random.randint(1, 13, size=n_reps)
    hire_offsets = np.random.randint(365, 365 * 8 + 1, size=n_reps)
    hire_dates = (start_date - pd.to_timedelta(hire_offsets, unit="D")).date

    reps = []
    for idx in range(n_reps):
        reps.append(
            {
                "rep_id": idx + 1,
                "rep_name": fake.name(),
                "email": fake.email(),
                "region": region_choices[idx],
                "territory": f"{region_choices[idx]} {int(territory_numbers[idx])}",
                "hire_date": hire_dates[idx],
            }
        )

    reps_df = pd.DataFrame(reps)
    logger.info("Generated %d sales reps", len(reps_df))
    return reps_df[SALES_REPS_COLUMNS]


def generate_customer_contacts(customers_df, sales_reps_df):
    """Generate primary and optional secondary contacts for each customer.

    Args:
        customers_df (pd.DataFrame): Customer table with region/segment/account fields.
        sales_reps_df (pd.DataFrame): Sales rep table with regional assignment.

    Returns:
        pd.DataFrame: Customer contacts in canonical column order.
    """
    logger = logging.getLogger(__name__)
    logger.info("Generating customer contacts")

    if customers_df is None or sales_reps_df is None or customers_df.empty or sales_reps_df.empty:
        logger.warning("Missing customers or sales reps; returning empty customer_contacts")
        return pd.DataFrame(columns=CUSTOMER_CONTACTS_COLUMNS)

    fake = Faker()
    end_date = pd.Timestamp(config.END_DATE).normalize()

    rep_by_region = {
        region: grp["rep_id"].to_numpy(dtype=int)
        for region, grp in sales_reps_df.groupby("region", dropna=False)
    }
    all_rep_ids = sales_reps_df["rep_id"].to_numpy(dtype=int)

    customer_ids = customers_df["customer_id"].to_numpy(dtype=int)
    customer_regions = (
        customers_df.get("region", pd.Series([None] * len(customers_df))).astype(object).to_numpy()
    )
    segments = (
        customers_df.get("segment", pd.Series([""] * len(customers_df))).astype(str).to_numpy()
    )
    sub_account = customers_df.get("sub_account_number", pd.Series([pd.NA] * len(customers_df)))
    is_sub = pd.notna(sub_account).to_numpy()

    primary_rep_ids = np.empty(len(customers_df), dtype=int)
    for idx, region in enumerate(customer_regions):
        candidates = rep_by_region.get(region)
        if candidates is None or len(candidates) == 0:
            primary_rep_ids[idx] = int(np.random.choice(all_rep_ids))
        else:
            primary_rep_ids[idx] = int(np.random.choice(candidates))

    roles = np.where(
        np.isin(segments, ["Government", "Education"]),
        "CS Rep",
        np.where(is_sub, "Inside Sales", "Account Manager"),
    )

    contact_offsets = np.random.randint(0, 181, size=len(customers_df))
    contact_dates = (end_date - pd.to_timedelta(contact_offsets, unit="D")).date

    rows = []
    contact_id = 1
    for idx, customer_id in enumerate(customer_ids):
        rows.append(
            {
                "contact_id": contact_id,
                "customer_id": int(customer_id),
                "rep_id": int(primary_rep_ids[idx]),
                "contact_name": fake.name(),
                "role": roles[idx],
                "email": fake.email(),
                "last_contact_date": contact_dates[idx],
            }
        )
        contact_id += 1

        if segments[idx] in {"Corporate", "Government"}:
            region = customer_regions[idx]
            candidates = rep_by_region.get(region, all_rep_ids)
            secondary_pool = candidates[candidates != primary_rep_ids[idx]]
            if len(secondary_pool) == 0:
                secondary_pool = all_rep_ids[all_rep_ids != primary_rep_ids[idx]]
            if len(secondary_pool) == 0:
                secondary_pool = all_rep_ids
            secondary_rep = int(np.random.choice(secondary_pool))

            rows.append(
                {
                    "contact_id": contact_id,
                    "customer_id": int(customer_id),
                    "rep_id": secondary_rep,
                    "contact_name": fake.name(),
                    "role": "Inside Sales",
                    "email": fake.email(),
                    "last_contact_date": (
                        end_date - pd.to_timedelta(int(np.random.randint(0, 181)), unit="D")
                    ).date(),
                }
            )
            contact_id += 1

    contacts_df = pd.DataFrame(rows)
    logger.info("Generated %d customer contact rows", len(contacts_df))
    return contacts_df[CUSTOMER_CONTACTS_COLUMNS]

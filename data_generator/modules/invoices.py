"""
Generate realistic invoices from orders and order lines.

Schema fields (example):

    invoice_id        BIGSERIAL PRIMARY KEY
    invoice_number    VARCHAR(50)
    order_id          BIGINT NOT NULL
    customer_id       BIGINT NOT NULL
    invoice_date      DATE
    due_date          DATE
    total_amount      NUMERIC(14,2)
    freight_billed    NUMERIC(12,2)
    tax_amount        NUMERIC(12,2)
    discount_amount   NUMERIC(12,2)
    payment_terms     VARCHAR(50)

Business-driven realism:
- One invoice per order.
- Total amount from extended line amounts.
- Freight billed based on weight/cube and customer terms.
- Tax amount based on simple tax rules.
- Discount based on payment terms.
"""

import logging
import random
from datetime import timedelta

import pandas as pd

import data_generator.config as config


def _select_payment_terms():
    """Randomly select realistic payment terms."""
    terms = [
        ("Net 30", 30, 0.0),
        ("Net 45", 45, 0.0),
        ("Net 60", 60, 0.0),
        ("2% 10 Net 30", 30, 0.02),
        ("1% 10 Net 45", 45, 0.01),
    ]
    return random.choice(terms)


def _estimate_freight(order_lines, items_df):
    """
    Estimate freight billed based on item weight and cube.
    order_lines: subset of order_lines_df for a single order.
    """
    merged = order_lines.merge(
        items_df[["item_id", "cube_ft", "weight_lb"]],
        on="item_id",
        how="left",
    )

    merged["cube_ft"] = merged["cube_ft"].fillna(0.1)
    merged["weight_lb"] = merged["weight_lb"].fillna(1.0)

    merged["total_cube"] = merged["cube_ft"] * merged["quantity"]
    merged["total_weight"] = merged["weight_lb"] * merged["quantity"]

    total_cube = merged["total_cube"].sum()
    total_weight = merged["total_weight"].sum()

    base = 5.0
    cube_component = total_cube * random.uniform(0.2, 0.6)
    weight_component = (total_weight / 50.0) * random.uniform(0.5, 1.5)

    freight = base + cube_component + weight_component
    return max(0.0, round(freight, 2))


def _tax_rate_for_customer(customer_row):
    """Simple tax rule: some customers are tax exempt."""
    if customer_row is None:
        return 0.07
    if str(customer_row.get("tax_exempt_flag", "N")).upper() == "Y":
        return 0.0
    return 0.07


def generate_invoices(orders_df, order_lines_df, customers_df, items_df):
    """
    Generate invoices from orders and order lines.

    Args:
        orders_df (pd.DataFrame): Must include order_id, customer_id,
        order_date.
        order_lines_df (pd.DataFrame): Must include order_id, item_id,
        quantity, unit_price.
        customers_df (pd.DataFrame): Must include customer_id, tax_exempt_flag
        (optional).
        items_df (pd.DataFrame): Must include item_id, cube_ft, weight_lb
        (for freight).

    Returns:
        pd.DataFrame: Invoices table.
    """
    logger = logging.getLogger(__name__)
    total_orders = len(orders_df)
    logger.info("Generating invoices from %d orders", total_orders)

    if orders_df.empty or order_lines_df.empty:
        logger.warning("No orders/order_lines provided; returning empty invoices")
        return pd.DataFrame(
            columns=[
                "invoice_id",
                "invoice_number",
                "order_id",
                "customer_id",
                "invoice_date",
                "due_date",
                "total_amount",
                "freight_billed",
                "tax_amount",
                "discount_amount",
                "payment_terms",
            ]
        )

    customers = customers_df.set_index("customer_id") if not customers_df.empty else None

    rows = []
    invoice_id = 1

    # Order lines should already have unit_price from generate_order_lines
    # No need to merge from items_df since it no longer contains pricing

    order_lines_df = order_lines_df.copy()
    if "quantity" not in order_lines_df.columns:
        if "quantity_ordered" in order_lines_df.columns:
            order_lines_df["quantity"] = order_lines_df["quantity_ordered"]
        else:
            raise KeyError("order_lines_df must include either 'quantity' or 'quantity_ordered'")

    order_lines_df["unit_price"] = order_lines_df["unit_price"].fillna(0.0)
    order_lines_df["extended_price"] = order_lines_df["quantity"] * order_lines_df["unit_price"]

    # Pre-group order lines by order_id for efficiency
    order_lines_grouped = order_lines_df.groupby("order_id")

    # Process in chunks for better performance and logging
    chunk_size = config.CHUNK_SIZE_INVOICES
    total_rows = len(orders_df)
    total_chunks = (total_rows + chunk_size - 1) // chunk_size
    log_interval = max(1, config.LOG_INTERVAL // chunk_size)

    for chunk_idx in range(total_chunks):
        start = chunk_idx * chunk_size
        end = min(start + chunk_size, total_rows)
        chunk = orders_df.iloc[start:end]

        for _, order in chunk.iterrows():
            order_id = int(order["order_id"])
            customer_id = int(order["customer_id"])
            order_date = pd.to_datetime(order["order_date"]).date()

            # Get order lines for this order
            try:
                lines = order_lines_grouped.get_group(order_id)
            except KeyError:
                # No lines for this order
                continue

            merchandise_total = round(lines["extended_price"].sum(), 2)

            # Freight billed
            freight_billed = _estimate_freight(lines, items_df)

            # Tax
            cust_row = (
                customers.loc[customer_id]
                if customers is not None and customer_id in customers.index
                else None
            )
            tax_rate = _tax_rate_for_customer(cust_row)
            taxable_amount = merchandise_total + freight_billed
            tax_amount = round(taxable_amount * tax_rate, 2)

            # Payment terms and discount
            terms_desc, terms_days, discount_pct = _select_payment_terms()

            invoice_date = order_date + timedelta(days=random.randint(0, 2))
            due_date = invoice_date + timedelta(days=terms_days)

            # Discount amount: assume customer sometimes takes discount
            if discount_pct > 0 and random.random() < 0.6:
                discount_amount = round(merchandise_total * discount_pct, 2)
            else:
                discount_amount = 0.0

            total_amount = round(
                merchandise_total + freight_billed + tax_amount - discount_amount, 2
            )

            invoice_number = f"INV-{order_id:08d}"

            rows.append(
                {
                    "invoice_id": invoice_id,
                    "invoice_number": invoice_number,
                    "order_id": order_id,
                    "customer_id": customer_id,
                    "invoice_date": invoice_date,
                    "due_date": due_date,
                    "total_amount": total_amount,
                    "freight_billed": freight_billed,
                    "tax_amount": tax_amount,
                    "discount_amount": discount_amount,
                    "payment_terms": terms_desc,
                }
            )

            invoice_id += 1

        # Log progress
        if (chunk_idx + 1) % log_interval == 0 or (chunk_idx + 1) == total_chunks:
            logger.info(
                "Processed invoices chunk %d / %d (%d orders processed, %d invoices generated)",
                chunk_idx + 1,
                total_chunks,
                end,
                len(rows),
            )

    df = pd.DataFrame(rows)
    logger.info("Generated %d invoices from %d orders", len(df), total_orders)
    return df

"""
Generate accounts receivable ledger entries from invoices.

Schema fields (example):

    ar_entry_id       BIGSERIAL PRIMARY KEY
    invoice_id        BIGINT NOT NULL
    customer_id       BIGINT NOT NULL
    posting_date      DATE
    amount_due        NUMERIC(14,2)
    amount_paid       NUMERIC(14,2)
    balance           NUMERIC(14,2)
    aging_bucket      VARCHAR(20)
    credit_hold_flag  BOOLEAN
    writeoff_reason   VARCHAR(100)

Business-driven realism:
- Some invoices are fully paid, some partially, some unpaid.
- Aging buckets based on days past due.
- Credit hold more likely for chronically late customers.
- Occasional small write-offs.
"""

import logging
import random
from datetime import datetime, timedelta

import pandas as pd

AGING_BUCKETS = [
    (0, 0, "Current"),
    (1, 30, "1-30"),
    (31, 60, "31-60"),
    (61, 90, "61-90"),
    (91, 9999, "90+"),
]

WRITE_OFF_REASONS = [
    "Small Balance Write-off",
    "Customer Dispute Resolved",
    "Uncollectible",
    "Settlement Adjustment",
]


def _aging_bucket(days_past_due):
    for low, high, label in AGING_BUCKETS:
        if low <= days_past_due <= high:
            return label
    return "Unknown"


def generate_ar_ledger(invoices_df, customers_df):
    """
    Generate AR ledger entries from invoices.

    Args:
        invoices_df (pd.DataFrame): Must include:
            - invoice_id
            - customer_id
            - invoice_date
            - due_date
            - total_amount
        customers_df (pd.DataFrame): Must include:
            - customer_id
            - credit_limit (optional)
            - risk_score (optional)

    Returns:
        pd.DataFrame: AR ledger table.
    """
    logger = logging.getLogger(__name__)
    logger.info("Generating AR ledger from %d invoices", len(invoices_df))

    if invoices_df.empty:
        logger.warning("No invoices provided; returning empty AR ledger")
        return pd.DataFrame(
            columns=[
                "ar_entry_id",
                "invoice_id",
                "customer_id",
                "posting_date",
                "amount_due",
                "amount_paid",
                "balance",
                "aging_bucket",
                "credit_hold_flag",
                "writeoff_reason",
            ]
        )

    today = datetime.today().date()
    customers = customers_df.set_index("customer_id") if not customers_df.empty else None

    rows = []
    ar_entry_id = 1

    for _, inv in invoices_df.iterrows():
        invoice_id = int(inv["invoice_id"])
        customer_id = int(inv["customer_id"])
        invoice_date = pd.to_datetime(inv["invoice_date"]).date()
        due_date = pd.to_datetime(inv["due_date"]).date()
        total_amount = float(inv["total_amount"])

        # Customer risk profile
        if customers is not None and customer_id in customers.index:
            cust = customers.loc[customer_id]
            risk_score = float(cust.get("risk_score", random.uniform(0.1, 0.9)))
        else:
            risk_score = random.uniform(0.1, 0.9)

        # Payment behavior based on risk_score
        # Low risk → more likely fully paid on time
        # High risk → more likely unpaid or partially paid
        base_pay_prob = 0.9 - (risk_score * 0.4)
        base_pay_prob = max(0.2, min(0.95, base_pay_prob))

        if random.random() < base_pay_prob:
            # Paid (fully or mostly)
            pay_ratio = random.uniform(0.8, 1.0)
            amount_paid = round(total_amount * pay_ratio, 2)
        else:
            # Unpaid or lightly paid
            pay_ratio = random.uniform(0.0, 0.4)
            amount_paid = round(total_amount * pay_ratio, 2)

        balance = round(total_amount - amount_paid, 2)

        # Posting date: near invoice date
        posting_date = invoice_date + timedelta(days=random.randint(0, 5))

        # Aging
        if balance <= 0:
            days_past_due = 0
        else:
            days_past_due = (today - due_date).days
            if days_past_due < 0:
                days_past_due = 0

        aging = _aging_bucket(days_past_due)

        # Credit hold more likely if high risk and aging is bad
        credit_hold_flag = False
        if balance > 0 and risk_score > 0.6 and days_past_due > 30:
            credit_hold_flag = random.random() < 0.5

        # Occasional write-off for old, small balances
        writeoff_reason = None
        if balance > 0 and days_past_due > 90 and balance < 200:
            if random.random() < 0.3:
                writeoff_reason = random.choice(WRITE_OFF_REASONS)
                amount_paid = round(amount_paid + balance, 2)
                balance = 0.0
                aging = "Current"

        rows.append(
            {
                "ar_entry_id": ar_entry_id,
                "invoice_id": invoice_id,
                "customer_id": customer_id,
                "posting_date": posting_date,
                "amount_due": round(total_amount, 2),
                "amount_paid": round(amount_paid, 2),
                "balance": round(balance, 2),
                "aging_bucket": aging,
                "credit_hold_flag": credit_hold_flag,
                "writeoff_reason": writeoff_reason,
            }
        )

        ar_entry_id += 1

    df = pd.DataFrame(rows)
    logger.info("Generated %d AR ledger records", len(df))
    return df

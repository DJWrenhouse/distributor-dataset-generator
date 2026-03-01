"""Generate smaller preview datasets for rapid validation and iteration."""

import argparse
import logging
import os
import random
from contextlib import contextmanager
from pathlib import Path

import numpy as np
import pandas as pd
from faker import Faker

import data_generator.config as config
from data_generator.modules.ar_ledger import generate_ar_ledger
from data_generator.modules.backorders import generate_backorders
from data_generator.modules.costs import (
    generate_carriers,
    generate_driver_costs,
    generate_fuel_costs,
)
from data_generator.modules.customer_item_preferences import (
    generate_customer_item_preferences,
)
from data_generator.modules.customers import generate_customers
from data_generator.modules.dc_item_slotting import generate_dc_item_slotting
from data_generator.modules.distribution_centers import (
    generate_distribution_centers,
)
from data_generator.modules.inventory import generate_inventory
from data_generator.modules.inventory_adjustments import (
    generate_inventory_adjustments,
)
from data_generator.modules.inventory_monthly_snapshot import (
    generate_inventory_monthly_snapshot,
)
from data_generator.modules.invoices import generate_invoices
from data_generator.modules.item_costs import generate_item_costs
from data_generator.modules.item_forecast import generate_item_forecast
from data_generator.modules.items import generate_items
from data_generator.modules.labor_costs import generate_labor_costs
from data_generator.modules.order_lines import generate_order_lines
from data_generator.modules.orders import generate_orders
from data_generator.modules.price_history import generate_price_history
from data_generator.modules.promotions import (
    generate_promo_orders,
    generate_promotions,
)
from data_generator.modules.purchase_orders import generate_purchase_orders
from data_generator.modules.returns import generate_returns
from data_generator.modules.sales_reps import (
    generate_customer_contacts,
    generate_sales_reps,
)
from data_generator.modules.shipment_lines import generate_shipment_lines
from data_generator.modules.shipments import (
    compute_shipment_totals_and_costs,
    generate_shipments,
)
from data_generator.modules.tracking import generate_tracking_events
from data_generator.modules.vendor_item_details import (
    generate_vendor_item_details,
)
from data_generator.modules.vendor_performance import (
    generate_vendor_performance,
)
from data_generator.modules.vendors import generate_vendors

TABLES = [
    "vendors",
    "vendor_performance",
    "vendor_item_details",
    "distribution_centers",
    "customers",
    "items",
    "item_costs",
    "item_forecast",
    "inventory",
    "inventory_monthly_snapshot",
    "inventory_adjustments",
    "dc_item_slotting",
    "carriers",
    "driver_costs",
    "fuel_costs",
    "orders",
    "order_lines",
    "shipments",
    "shipment_lines",
    "tracking_events",
    "customer_item_preferences",
    "backorders",
    "invoices",
    "ar_ledger",
    "returns",
    "purchase_orders",
    "price_history",
    "promotions",
    "promo_orders",
    "labor_costs",
    "sales_reps",
    "customer_contacts",
]

AVG_ORDER_LINES_PER_ORDER = 5.5

CSV_BYTES_PER_ROW_ESTIMATE = {
    "vendors": 90,
    "vendor_performance": 80,
    "vendor_item_details": 120,
    "distribution_centers": 160,
    "customers": 180,
    "items": 170,
    "item_costs": 140,
    "item_forecast": 120,
    "inventory": 135,
    "inventory_monthly_snapshot": 150,
    "inventory_adjustments": 130,
    "dc_item_slotting": 110,
    "carriers": 115,
    "driver_costs": 90,
    "fuel_costs": 75,
    "orders": 80,
    "order_lines": 75,
    "shipments": 135,
    "shipment_lines": 145,
    "tracking_events": 115,
    "customer_item_preferences": 120,
    "backorders": 110,
    "invoices": 120,
    "ar_ledger": 120,
    "returns": 115,
    "purchase_orders": 130,
    "price_history": 110,
    "promotions": 95,
    "promo_orders": 75,
    "labor_costs": 105,
    "sales_reps": 90,
    "customer_contacts": 110,
}

PARQUET_BYTES_PER_ROW_ESTIMATE = {
    "vendors": 42,
    "vendor_performance": 32,
    "vendor_item_details": 56,
    "distribution_centers": 64,
    "customers": 78,
    "items": 72,
    "item_costs": 52,
    "item_forecast": 48,
    "inventory": 62,
    "inventory_monthly_snapshot": 70,
    "inventory_adjustments": 60,
    "dc_item_slotting": 52,
    "carriers": 48,
    "driver_costs": 44,
    "fuel_costs": 36,
    "orders": 38,
    "order_lines": 36,
    "shipments": 58,
    "shipment_lines": 62,
    "tracking_events": 52,
    "customer_item_preferences": 48,
    "backorders": 46,
    "invoices": 48,
    "ar_ledger": 48,
    "returns": 52,
    "purchase_orders": 58,
    "price_history": 48,
    "promotions": 44,
    "promo_orders": 36,
    "labor_costs": 48,
    "sales_reps": 42,
    "customer_contacts": 50,
}


@contextmanager
def _config_override(**kwargs):
    """Temporarily override config values for preview generation.

    Args:
        **kwargs: Config attribute names and override values.

    Yields:
        None: Context manager that restores original values on exit.
    """
    original = {}
    for key, value in kwargs.items():
        original[key] = getattr(config, key)
        setattr(config, key, value)
    try:
        yield
    finally:
        for key, value in original.items():
            setattr(config, key, value)


def _parse_tables(values):
    """Parse and validate table names from CLI arguments.

    Args:
        values: List of comma-separated or individual table names.

    Returns:
        list: Sorted, deduplicated list of valid table names.
    """
    if not values:
        return TABLES
    tables = []
    for v in values:
        parts = [p.strip() for p in v.split(",") if p.strip()]
        tables.extend(parts)
    tables = [t for t in tables if t in TABLES]
    return sorted(set(tables), key=TABLES.index)


def _format_timestamp_cols(df, cols):
    """Format timestamp columns to standardized string format.

    Args:
        df: DataFrame to format.
        cols: List of column names to format as timestamps.

    Returns:
        pd.DataFrame: DataFrame with formatted timestamp columns.
    """
    if df is None or df.empty:
        return df
    for col in cols:
        if col in df.columns:
            series = pd.to_datetime(df[col], errors="coerce")
            df[col] = series.dt.strftime("%Y-%m-%d %H:%M:%S")
    return df


def _write_table(out_dir, name, df, output_format):
    """Write a table to disk in the specified format.

    Args:
        out_dir: Output directory path.
        name: Table name (used for filename).
        df: DataFrame to write.
        output_format: 'csv' or 'parquet'.
    """
    if df is None:
        return

    os.makedirs(out_dir, exist_ok=True)

    if output_format == "csv":
        if name in {"orders", "shipments", "tracking_events"}:
            df = _format_timestamp_cols(
                df,
                [
                    "order_date",
                    "ship_date",
                    "promised_date",
                    "delivery_date",
                    "event_timestamp",
                ],
            )
        df.to_csv(os.path.join(out_dir, f"{name}.csv"), index=False)
    else:
        df.to_parquet(os.path.join(out_dir, f"{name}.parquet"), index=False)


def _apply_row_limit(df, row_limit, seed):
    """Apply row limit to a DataFrame by random sampling.

    Args:
        df: Source DataFrame.
        row_limit: Maximum rows to keep (None for no limit).
        seed: Random seed for reproducible sampling.

    Returns:
        pd.DataFrame: Sampled DataFrame or original if under limit.
    """
    if df is None or df.empty or row_limit is None:
        return df
    if len(df) <= row_limit:
        return df
    return df.sample(n=row_limit, random_state=seed).reset_index(drop=True)


def _summarize_table(name, df):
    """Create a summary string for a table.

    Args:
        name: Table name.
        df: DataFrame to summarize.

    Returns:
        str: Summary string with row count, column count, and null count.
    """
    if df is None:
        return f"{name}: None"
    rows, cols = df.shape
    nulls = int(df.isna().sum().sum()) if rows else 0
    return f"{name}: rows={rows}, cols={cols}, nulls={nulls}"


def _format_bytes(size_bytes):
    """Convert byte count to human-readable format.

    Args:
        size_bytes: Size in bytes.

    Returns:
        str: Formatted size string with appropriate unit (B/KB/MB/GB/TB).
    """
    units = ["B", "KB", "MB", "GB", "TB"]
    value = float(max(0, size_bytes))
    for unit in units:
        if value < 1024.0 or unit == units[-1]:
            return f"{value:.2f} {unit}"
        value /= 1024.0
    return f"{value:.2f} TB"


def _estimate_rows_by_table(args, selected_tables):
    """Calculate approximate row counts for preview tables.

    Args:
        args: Parsed command-line arguments with preview parameters.
        selected_tables: List of table names to include in estimates.

    Returns:
        Dict[str, int]: Mapping of table names to estimated row counts.
    """
    catalog = pd.read_csv(config.ITEM_CATALOG_PATH)

    base_item_count = int(len(catalog))

    item_count = min(base_item_count, int(args.item_limit)) if args.item_limit else base_item_count

    n_vendors = int(args.n_vendors)
    n_carriers = int(args.n_carriers)
    n_dcs = int(args.n_dcs)
    n_customers = int(args.n_customers)
    n_orders = int(args.n_orders)

    if args.row_limit_strict and args.row_limit is not None:
        n_vendors = min(n_vendors, args.row_limit)
        n_carriers = min(n_carriers, args.row_limit)
        n_dcs = min(n_dcs, args.row_limit)
        n_customers = min(n_customers, args.row_limit)
        n_orders = min(n_orders, args.row_limit)
        item_count = min(item_count, args.row_limit)

    order_lines = int(round(n_orders * AVG_ORDER_LINES_PER_ORDER))

    estimates = {
        "vendors": n_vendors,
        "vendor_performance": n_vendors,
        "vendor_item_details": int(n_vendors * item_count * 0.15),
        "distribution_centers": n_dcs,
        "customers": n_customers,
        "items": item_count,
        "item_costs": item_count * 3,
        "item_forecast": item_count * 12,
        "inventory": int(round(n_dcs * item_count * 0.4)),
        "inventory_monthly_snapshot": int(round(n_dcs * item_count * 0.4)) * 12,
        "inventory_adjustments": n_dcs * 50,
        "dc_item_slotting": n_dcs * item_count,
        "carriers": n_carriers,
        "driver_costs": n_carriers * 3,
        "fuel_costs": 100,
        "orders": n_orders,
        "order_lines": order_lines,
        "shipments": n_orders,
        "shipment_lines": order_lines,
        "tracking_events": n_orders * 4,
        "customer_item_preferences": n_customers * 20,
        "backorders": int(order_lines * 0.08),
        "invoices": n_orders,
        "ar_ledger": n_orders,
        "returns": int(order_lines * 0.05),
        "purchase_orders": int(n_dcs * item_count * 0.15),
        "price_history": item_count * 3,
        "promotions": 300,
        "promo_orders": int(n_orders * 0.10),
        "labor_costs": n_dcs * 5 * 130,
        "sales_reps": 75,
        "customer_contacts": int(n_customers * 1.3),
    }

    if args.row_limit is not None:
        estimates = {name: min(rows, args.row_limit) for name, rows in estimates.items()}

    return {name: estimates[name] for name in selected_tables}


def _log_preview_estimate(logger, rows_by_table, output_format):
    """Log preflight estimates for preview generation.

    Args:
        logger: Logger instance for output.
        rows_by_table: Mapping of table names to estimated row counts.
        output_format: Output format ('csv' or 'parquet').
    """
    bytes_per_row = (
        CSV_BYTES_PER_ROW_ESTIMATE if output_format == "csv" else PARQUET_BYTES_PER_ROW_ESTIMATE
    )

    logger.info("Preflight estimate (preview)")
    logger.info("Output format: %s", output_format)
    logger.info(
        "Assumptions: avg order lines per order %.1f, tracking events pershipment 4",
        AVG_ORDER_LINES_PER_ORDER,
    )

    total_rows = 0
    total_bytes = 0

    for name, rows in rows_by_table.items():
        estimated_bytes = int(rows * bytes_per_row.get(name, 0))
        total_rows += rows
        total_bytes += estimated_bytes

        logger.info(
            "Estimate %-26s rows=%10d  size approx %s",
            name,
            rows,
            _format_bytes(estimated_bytes),
        )

    logger.info("Estimated preview output rows: %d", total_rows)
    logger.info("Estimated preview output size: %s", _format_bytes(total_bytes))


def _confirm_preview_or_abort(rows_by_table, output_format, assume_yes):
    """Prompt user to confirm preview generation or abort.

    Args:
        rows_by_table: Mapping of table names to estimated row counts.
        output_format: Output format ('csv' or 'parquet').
        assume_yes: If True, skip prompt and proceed.

    Returns:
        bool: True to proceed, False to abort.
    """
    if assume_yes:
        return True

    if not os.isatty(0):
        return True

    bytes_per_row = (
        CSV_BYTES_PER_ROW_ESTIMATE if output_format == "csv" else PARQUET_BYTES_PER_ROW_ESTIMATE
    )

    total_bytes = sum(
        int(rows * bytes_per_row.get(name, 0)) for name, rows in rows_by_table.items()
    )

    try:
        response = (
            input(
                "Estimated preview output is about "
                f"{_format_bytes(total_bytes)} across "
                f"{len(rows_by_table)} files. "
                "Continue generation? [y/N]: "
            )
            .strip()
            .lower()
        )
    except EOFError:
        return True

    return response in {"y", "yes"}


def parse_args():
    """Parse CLI arguments for preview dataset generation."""
    parser = argparse.ArgumentParser(description="Generate preview data for selected tables")

    parser.add_argument(
        "--tables",
        action="append",
        help="Comma separated list of tables to generate",
    )

    parser.add_argument(
        "--output-dir",
        default=str(Path("output") / "preview"),
        help="Output directory",
    )

    parser.add_argument(
        "--output-format",
        choices=["csv", "parquet"],
        default="csv",
    )

    parser.add_argument("--seed", type=int, default=1)

    parser.add_argument(
        "--assume-yes",
        action="store_true",
        help="Skip confirmation prompt and proceed",
    )

    parser.add_argument(
        "--no-confirm",
        action="store_true",
        help="Disable preflight estimate confirmation prompt",
    )

    parser.add_argument("--n-vendors", type=int, default=50)
    parser.add_argument(
        "--n-carriers",
        type=int,
        default=config.N_CARRIERS,
    )
    parser.add_argument("--n-dcs", type=int, default=5)
    parser.add_argument("--n-customers", type=int, default=200)
    parser.add_argument("--n-orders", type=int, default=200)
    parser.add_argument("--item-limit", type=int, default=500)

    parser.add_argument(
        "--row-limit",
        type=int,
        default=None,
        help="Cap rows per table after generation",
    )

    parser.add_argument(
        "--row-limit-strict",
        action="store_true",
        help="Apply row limit to upstream tables before downstream generation",
    )

    parser.add_argument("--log-interval", type=int, default=1000)

    return parser.parse_args()


def main():
    """Run preview dataset generation for selected tables."""
    args = parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    logger = logging.getLogger(__name__)

    random.seed(args.seed)
    np.random.seed(args.seed)
    Faker.seed(args.seed)

    tables = _parse_tables(args.tables)
    logger.info("Generating preview tables: %s", ", ".join(tables))

    estimated_rows = _estimate_rows_by_table(args, tables)
    _log_preview_estimate(logger, estimated_rows, args.output_format)

    if not args.no_confirm:
        should_continue = _confirm_preview_or_abort(
            estimated_rows,
            args.output_format,
            assume_yes=args.assume_yes,
        )
        if not should_continue:
            logger.info("Preview generation cancelled at confirmation prompt")
            return

    n_vendors = args.n_vendors
    n_carriers = args.n_carriers
    n_dcs = args.n_dcs
    n_customers = args.n_customers
    n_orders = args.n_orders

    if args.row_limit_strict and args.row_limit is not None:
        n_vendors = min(n_vendors, args.row_limit)
        n_carriers = min(n_carriers, args.row_limit)
        n_dcs = min(n_dcs, args.row_limit)
        n_customers = min(n_customers, args.row_limit)
        n_orders = min(n_orders, args.row_limit)

    with _config_override(
        N_VENDORS=n_vendors,
        N_CARRIERS=n_carriers,
        N_DCS=n_dcs,
        N_CUSTOMERS=n_customers,
        N_ORDERS=n_orders,
        LOG_INTERVAL=max(1, args.log_interval),
    ):
        vendors = generate_vendors()
        if args.row_limit_strict:
            vendors = _apply_row_limit(vendors, args.row_limit, args.seed)

        vendor_performance = generate_vendor_performance(vendors)

        dcs = generate_distribution_centers()
        if args.row_limit_strict:
            dcs = _apply_row_limit(dcs, args.row_limit, args.seed)

        customers = generate_customers()
        if args.row_limit_strict:
            customers = _apply_row_limit(customers, args.row_limit, args.seed)

        items = generate_items(vendors)

        if args.item_limit and len(items) > args.item_limit:
            items = items.sample(
                n=args.item_limit,
                random_state=args.seed,
            ).reset_index(drop=True)

            items["item_id"] = np.arange(1, len(items) + 1)

        if args.row_limit_strict:
            items = _apply_row_limit(items, args.row_limit, args.seed)
            items["item_id"] = np.arange(1, len(items) + 1)

        vendor_item_details = generate_vendor_item_details(
            vendors,
            items,
            vendor_performance,
        )

        item_costs = generate_item_costs(items)
        item_forecast = generate_item_forecast(items)

        inventory = generate_inventory(items, dcs)
        inventory_monthly_snapshot = generate_inventory_monthly_snapshot(items, dcs)
        inventory_adjustments = generate_inventory_adjustments(items, dcs)
        dc_item_slotting = generate_dc_item_slotting(items, dcs)

        carriers = generate_carriers()
        if args.row_limit_strict:
            carriers = _apply_row_limit(carriers, args.row_limit, args.seed)

        driver_costs = generate_driver_costs(carriers)
        fuel_costs = generate_fuel_costs()

        orders = generate_orders(customers, dcs)
        if args.row_limit_strict:
            orders = _apply_row_limit(orders, args.row_limit, args.seed)

        order_lines = generate_order_lines(orders, items, customers)

        shipments = generate_shipments(orders, dcs, carriers)

        shipment_lines = generate_shipment_lines(
            shipments,
            order_lines,
            items,
            carriers,
        )

        shipments = compute_shipment_totals_and_costs(
            shipments,
            shipment_lines,
            items,
        )

        tracking_events = generate_tracking_events(shipments, dcs)

        customer_item_preferences = generate_customer_item_preferences(
            customers,
            items,
            orders,
            order_lines,
        )

        backorders = generate_backorders(
            order_lines,
            items,
            inventory_monthly_snapshot,
        )

        invoices = generate_invoices(
            orders,
            order_lines,
            customers,
            items,
        )

        ar_ledger = generate_ar_ledger(invoices, customers)

        sales_reps = generate_sales_reps()
        labor_costs = generate_labor_costs(dcs)
        price_history = generate_price_history(
            items,
            item_costs,
            customers,
            customer_item_preferences,
        )
        promotions = generate_promotions(items, customers)
        purchase_orders = generate_purchase_orders(
            vendors,
            items,
            vendor_item_details,
            dcs,
            inventory,
            vendor_performance,
        )
        returns = generate_returns(order_lines, orders, items, shipments)
        promo_orders = generate_promo_orders(promotions, orders)
        customer_contacts = generate_customer_contacts(customers, sales_reps)

    table_map = {
        "vendors": vendors,
        "vendor_performance": vendor_performance,
        "vendor_item_details": vendor_item_details,
        "distribution_centers": dcs,
        "customers": customers,
        "items": items,
        "item_costs": item_costs,
        "item_forecast": item_forecast,
        "inventory": inventory,
        "inventory_monthly_snapshot": inventory_monthly_snapshot,
        "inventory_adjustments": inventory_adjustments,
        "dc_item_slotting": dc_item_slotting,
        "carriers": carriers,
        "driver_costs": driver_costs,
        "fuel_costs": fuel_costs,
        "orders": orders,
        "order_lines": order_lines,
        "shipments": shipments,
        "shipment_lines": shipment_lines,
        "tracking_events": tracking_events,
        "customer_item_preferences": customer_item_preferences,
        "backorders": backorders,
        "invoices": invoices,
        "ar_ledger": ar_ledger,
        "returns": returns,
        "purchase_orders": purchase_orders,
        "price_history": price_history,
        "promotions": promotions,
        "promo_orders": promo_orders,
        "labor_costs": labor_costs,
        "sales_reps": sales_reps,
        "customer_contacts": customer_contacts,
    }

    for name in tables:
        df = _apply_row_limit(
            table_map.get(name),
            args.row_limit,
            args.seed,
        )
        _write_table(args.output_dir, name, df, args.output_format)
        logger.info("Wrote %s", name)

    logger.info("Preview summary")
    for name in tables:
        logger.info(_summarize_table(name, table_map.get(name)))


if __name__ == "__main__":
    main()

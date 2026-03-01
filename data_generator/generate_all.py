"""Run the full synthetic distributor dataset generation workflow.

This entrypoint orchestrates reference-table generation, transactional-table
generation, and cost-table generation, then persists results to CSV or Parquet.
It supports both in-memory and streamed generation paths.
"""

# pyright: reportMissingImports=false

import argparse
import logging
import os
import random
import tempfile
from pathlib import Path
from typing import Dict

import numpy as np
import pandas as pd
from faker import Faker

import data_generator.config as config
from data_generator.constants import US_STATES
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
from data_generator.modules.order_lines import (
    generate_order_lines,
    generate_order_lines_for_orders,
)
from data_generator.modules.orders import (
    generate_orders,
    generate_orders_chunked,
)
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
from data_generator.modules.shipment_lines import (
    generate_shipment_lines,
    generate_shipment_lines_for_chunk,
)
from data_generator.modules.shipments import (
    compute_shipment_totals_and_costs,
    generate_shipments,
    generate_shipments_for_orders,
)
from data_generator.modules.tracking import generate_tracking_events
from data_generator.modules.vendor_item_details import (
    generate_vendor_item_details,
)
from data_generator.modules.vendor_performance import (
    generate_vendor_performance,
)
from data_generator.modules.vendors import generate_vendors


def _combine_parquet_parts(base_path: Path, out_path: Path):
    """Combine parquet part files matching base_path_part*.parquet.

    Merge all matching parts into a single output parquet file.
    """
    import glob

    pattern = str(base_path) + "_part*.parquet"
    parts = sorted(glob.glob(pattern))
    if not parts:
        return

    dfs = [pd.read_parquet(p) for p in parts]
    combined = pd.concat(dfs, ignore_index=True)
    tmp = tempfile.NamedTemporaryFile(
        delete=False,
        dir=out_path.parent,
        prefix="." + out_path.name,
    )
    tmp.close()
    combined.to_parquet(tmp.name, index=False)
    os.replace(tmp.name, str(out_path))
    for p in parts:
        try:
            os.remove(p)
        except Exception:
            pass


OUTPUT_DIR = "output"
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


def ensure_output_dir(path: str):
    """Create the output directory if it does not already exist."""
    os.makedirs(path, exist_ok=True)


def validate_table(name: str, df):
    """Validate that a generated table exists and is non-empty."""
    logger = logging.getLogger(__name__)
    if df is None:
        raise ValueError(f"Table {name} is None")
    if hasattr(df, "shape"):
        logger.info("%s: shape=%s", name, df.shape)
        if df.shape[0] == 0:
            raise ValueError(f"Table {name} is empty")


def parse_args():
    """Parse CLI arguments for full dataset generation."""
    parser = argparse.ArgumentParser(description="Generate synthetic distributor dataset")
    parser.add_argument(
        "--output-dir",
        default=OUTPUT_DIR,
        help="Directory to write CSV output",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Random seed for reproducibility",
    )
    parser.add_argument(
        "--n-orders",
        type=int,
        default=None,
        help="Override number of orders to generate",
    )
    parser.add_argument(
        "--n-carriers",
        type=int,
        default=None,
        help="Override number of carriers to generate",
    )
    parser.add_argument("--verbose", action="store_true", help="Enable verboselogging")
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
    parser.add_argument(
        "--stream",
        action="store_true",
        help="Stream large tables to CSV/parquet in chunks",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=10000,
        help="Chunk size when streaming large tables",
    )
    parser.add_argument(
        "--log-interval",
        type=int,
        default=config.LOG_INTERVAL,
        help="Rows between progress log lines",
    )
    write_group = parser.add_mutually_exclusive_group()
    write_group.add_argument(
        "--write-through",
        dest="write_through",
        action="store_true",
        help="Write tables as soon as they are finalized",
    )
    write_group.add_argument(
        "--no-write-through",
        dest="write_through",
        action="store_false",
        help="Write tables only at the end (higher memory)",
    )
    parser.set_defaults(write_through=True)
    parser.add_argument(
        "--output-format",
        choices=["csv", "parquet"],
        default="csv",
        help="Output format for files",
    )
    return parser.parse_args()


def configure_logging(verbose: bool):
    """Configure process-wide logging for generator runs."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


def set_seed(seed: int):
    """Seed random generators for reproducible output."""
    random.seed(seed)
    np.random.seed(seed)
    Faker.seed(seed)


def _format_bytes(size_bytes: int) -> str:
    """Convert byte count to human-readable format (B, KB, MB, GB, TB).

    Args:
        size_bytes: Size in bytes.

    Returns:
        str: Formatted size string with appropriate unit.
    """
    units = ["B", "KB", "MB", "GB", "TB"]
    value = float(max(0, size_bytes))
    for unit in units:
        if value < 1024.0 or unit == units[-1]:
            return f"{value:.2f} {unit}"
        value /= 1024.0
    return f"{value:.2f} TB"


def _estimate_table_rows(item_count: int, category_count: int) -> Dict[str, int]:
    """Calculate approximate row counts for all output tables.

    Args:
        item_count: Number of items in the catalog.
        category_count: Number of distinct categories.

    Returns:
        Dict[str, int]: Mapping of table names to estimated row counts.
    """
    orders = int(config.N_ORDERS)
    shipments = orders
    order_lines = int(round(orders * AVG_ORDER_LINES_PER_ORDER))
    shipment_lines = order_lines
    tracking_events = shipments * 4
    inventory = int(round(config.N_DCS * item_count * 0.4))

    vendor_item_details = int(round(config.N_VENDORS * item_count * 0.15))
    item_costs = item_count * 3
    item_forecast = item_count * 12
    inventory_monthly_snapshot = inventory * 12
    inventory_adjustments = int(config.N_DCS * 300)
    dc_item_slotting = int(config.N_DCS * item_count)
    customer_item_preferences = int(config.N_CUSTOMERS * 50)
    backorders = int(round(order_lines * 0.08))
    invoices = orders
    ar_ledger = invoices
    returns = int(round(order_lines * 0.05))
    purchase_orders = int(round(inventory * 0.60))
    price_history = int(item_count * 3)
    promotions = 400
    promo_orders = int(round(orders * 0.10))
    labor_costs = int(config.N_DCS * 5 * 130)
    sales_reps = 75
    customer_contacts = int(round(config.N_CUSTOMERS * 1.3))

    return {
        "categories": int(category_count),
        "vendors": int(config.N_VENDORS),
        "vendor_performance": int(config.N_VENDORS),
        "vendor_item_details": vendor_item_details,
        "distribution_centers": int(config.N_DCS),
        "customers": int(config.N_CUSTOMERS),
        "items": int(item_count),
        "item_costs": item_costs,
        "item_forecast": item_forecast,
        "inventory": inventory,
        "inventory_monthly_snapshot": inventory_monthly_snapshot,
        "inventory_adjustments": inventory_adjustments,
        "dc_item_slotting": dc_item_slotting,
        "carriers": int(config.N_CARRIERS),
        "driver_costs": int(config.N_CARRIERS) * 3,
        "fuel_costs": int(len(US_STATES) * 2),
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


def _log_generation_estimate(
    logger: logging.Logger, rows_by_table: Dict[str, int], output_format: str
):
    """Log preflight estimates for row counts and file sizes.

    Args:
        logger: Logger instance for output.
        rows_by_table: Mapping of table names to estimated row counts.
        output_format: Output format ('csv' or 'parquet').
    """
    bytes_per_row = (
        CSV_BYTES_PER_ROW_ESTIMATE if output_format == "csv" else PARQUET_BYTES_PER_ROW_ESTIMATE
    )

    logger.info("Preflight estimate (based on current configuration)")
    logger.info("Output format: %s", output_format)
    logger.info(
        "Assumptions: avg order lines/order=%.1f, tracking events/shipment=4",
        AVG_ORDER_LINES_PER_ORDER,
    )

    total_rows = 0
    total_bytes = 0
    for name, rows in rows_by_table.items():
        estimated_bytes = int(rows * bytes_per_row.get(name, 0))
        total_rows += rows
        total_bytes += estimated_bytes
        logger.info(
            "Estimate %-26s rows=%12d  size≈%s",
            name,
            rows,
            _format_bytes(estimated_bytes),
        )

    logger.info("Estimated total rows: %d", total_rows)
    logger.info("Estimated total output size: %s", _format_bytes(total_bytes))


def _confirm_generation_or_abort(
    rows_by_table: Dict[str, int], output_format: str, assume_yes: bool
) -> bool:
    """Prompt user to confirm generation or abort based on size estimates.

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
                "Estimated output size is about "
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


def main():
    """Run the full dataset generation workflow."""
    args = parse_args()
    configure_logging(args.verbose)
    logger = logging.getLogger(__name__)

    def format_timestamp_cols(df, cols):
        if df is None or df.empty:
            return df
        for col in cols:
            if col in df.columns:
                series = pd.to_datetime(df[col], errors="coerce")
                month = series.dt.month
                day = series.dt.day
                weekday = series.dt.weekday
                spring_sunday = (month == 3) & (day >= 8) & (day <= 14) & (weekday == 6)
                spring_gap = spring_sunday & (series.dt.hour == 2)
                fall_sunday = (month == 11) & (day >= 1) & (day <= 7) & (weekday == 6)
                fall_ambiguous = fall_sunday & (series.dt.hour == 1)
                if spring_gap.any():
                    series.loc[spring_gap] = series.loc[spring_gap] + pd.Timedelta(hours=1)
                if fall_ambiguous.any():
                    series.loc[fall_ambiguous] = series.loc[fall_ambiguous] + pd.Timedelta(hours=1)
                df[col] = series.dt.strftime("%Y-%m-%d %H:%M:%S")
        return df

    def write_table(name: str, df):
        if df is None:
            return None
        validate_table(name, df)
        if args.output_format == "csv":
            if name in {"orders", "shipments", "tracking_events"}:
                df = format_timestamp_cols(
                    df,
                    [
                        "order_date",
                        "ship_date",
                        "promised_date",
                        "delivery_date",
                        "event_timestamp",
                    ],
                )
            path = os.path.join(args.output_dir, f"{name}.csv")
            tmp = tempfile.NamedTemporaryFile(delete=False, dir=args.output_dir, prefix=f".{name}.")
            tmp.close()
            df.to_csv(tmp.name, index=False)
            os.replace(tmp.name, path)
            logger.info("Wrote %s to %s", name, path)
        else:
            path = os.path.join(args.output_dir, f"{name}.parquet")
            tmp = tempfile.NamedTemporaryFile(delete=False, dir=args.output_dir, prefix=f".{name}.")
            tmp.close()
            df.to_parquet(tmp.name, index=False)
            os.replace(tmp.name, path)
            logger.info("Wrote %s to %s", name, path)
        return None

    if args.seed is not None:
        set_seed(args.seed)
        logger.info("Set random seed to %s", args.seed)

    if args.n_orders is not None:
        config.N_ORDERS = args.n_orders
        logger.info("Override N_ORDERS to %s", config.N_ORDERS)

    if args.n_carriers is not None:
        config.N_CARRIERS = args.n_carriers
        logger.info("Override N_CARRIERS to %s", config.N_CARRIERS)

    catalog = pd.read_csv(config.ITEM_CATALOG_PATH)
    estimated_rows = _estimate_table_rows(
        item_count=int(len(catalog)),
        category_count=int(catalog["product_category"].dropna().nunique()),
    )
    _log_generation_estimate(logger, estimated_rows, args.output_format)

    if not args.no_confirm:
        should_continue = _confirm_generation_or_abort(
            estimated_rows,
            args.output_format,
            assume_yes=args.assume_yes,
        )
        if not should_continue:
            if not os.isatty(0):
                logger.warning("No interactive terminal detected and confirmation is enabled.")
                logger.warning("Use --assume-yes to continue or --no-confirm to skip this prompt.")
            else:
                logger.info("Generation cancelled by user at preflight confirmation prompt.")
            return

    config.LOG_INTERVAL = max(1, args.log_interval)
    logger.info("Log interval set to %s", config.LOG_INTERVAL)

    ensure_output_dir(args.output_dir)
    written_tables = set()

    logger.info("Starting generation of reference tables")
    vendors = generate_vendors()
    vendor_performance = generate_vendor_performance(vendors)
    dcs = generate_distribution_centers()
    customers = generate_customers()

    logger.info("Generating items and inventory")
    items = generate_items(vendors)
    vendor_item_details = generate_vendor_item_details(vendors, items, vendor_performance)
    item_costs = generate_item_costs(items)
    item_forecast = generate_item_forecast(items)

    inventory = generate_inventory(items, dcs)
    inventory_monthly_snapshot = generate_inventory_monthly_snapshot(items, dcs)
    inventory_adjustments = generate_inventory_adjustments(items, dcs)
    dc_item_slotting = generate_dc_item_slotting(items, dcs)

    logger.info("Generating cost tables")
    carriers = generate_carriers()
    driver_costs = generate_driver_costs(carriers)
    fuel_costs = generate_fuel_costs()

    # Write all small/reference tables before generating large
    # transactional tables
    if args.write_through:
        logger.info(
            "Writing all reference and dimension tables before "
            "generating large transactional tables"
        )
        write_table("vendors", vendors)
        written_tables.add("vendors")
        write_table("vendor_performance", vendor_performance)
        written_tables.add("vendor_performance")
        write_table("distribution_centers", dcs)
        written_tables.add("distribution_centers")
        write_table("customers", customers)
        written_tables.add("customers")
        write_table("items", items)
        written_tables.add("items")
        write_table("vendor_item_details", vendor_item_details)
        written_tables.add("vendor_item_details")
        write_table("item_costs", item_costs)
        written_tables.add("item_costs")
        write_table("item_forecast", item_forecast)
        written_tables.add("item_forecast")
        write_table("inventory", inventory)
        written_tables.add("inventory")
        write_table("inventory_monthly_snapshot", inventory_monthly_snapshot)
        written_tables.add("inventory_monthly_snapshot")
        write_table("inventory_adjustments", inventory_adjustments)
        written_tables.add("inventory_adjustments")
        write_table("dc_item_slotting", dc_item_slotting)
        written_tables.add("dc_item_slotting")
        write_table("carriers", carriers)
        written_tables.add("carriers")
        write_table("driver_costs", driver_costs)
        written_tables.add("driver_costs")
        write_table("fuel_costs", fuel_costs)
        written_tables.add("fuel_costs")

    logger.info("Generating orders and related artifacts")
    returns = None
    purchase_orders = None
    price_history = None
    promotions = None
    promo_orders = None
    labor_costs = None
    sales_reps = None
    customer_contacts = None

    if args.stream:
        logger.info(
            "Streaming enabled: chunk_size=%s, format=%s",
            args.chunk_size,
            args.output_format,
        )

        out_dir = Path(args.output_dir)
        orders_base = out_dir / "orders"
        order_lines_base = out_dir / "order_lines"
        shipments_base = out_dir / "shipments"
        shipment_lines_base = out_dir / "shipment_lines"
        tracking_base = out_dir / "tracking_events"

        first_write = {
            "orders": True,
            "order_lines": True,
            "shipments": True,
            "shipment_lines": True,
            "tracking": True,
        }

        order_line_id_counter = 1
        shipment_line_id_counter = 1
        tracking_event_id_counter = 1

        chunk_idx = 0
        gen = generate_orders_chunked(
            customers,
            dcs,
            chunk_size=args.chunk_size,
            n_orders=config.N_ORDERS,
        )
        orders_count = 0
        order_lines_count = 0
        shipments_count = 0
        shipment_lines_count = 0
        shipment_costs_count = 0
        tracking_count = 0
        last_logged = {
            "orders": 0,
            "order_lines": 0,
            "shipments": 0,
            "shipment_lines": 0,
            "shipment_costs": 0,
            "tracking": 0,
        }

        def should_log(count, last_count):
            return (count - last_count) >= config.LOG_INTERVAL

        for orders_chunk in gen:
            chunk_idx += 1
            orders_count += len(orders_chunk)
            if should_log(orders_count, last_logged["orders"]) or orders_count == config.N_ORDERS:
                logger.info("Orders: %d / %d", orders_count, config.N_ORDERS)
                last_logged["orders"] = orders_count

            if args.output_format == "csv":
                orders_chunk = format_timestamp_cols(orders_chunk, ["order_date"])
                orders_chunk.to_csv(
                    str(orders_base) + ".csv",
                    mode="a",
                    header=first_write["orders"],
                    index=False,
                )
                first_write["orders"] = False
            else:
                orders_chunk.to_parquet(
                    str(orders_base) + f"_part{chunk_idx:04d}.parquet",
                    index=False,
                )

            ol_chunk = generate_order_lines_for_orders(
                orders_chunk,
                items,
                customers,
                start_line_id=order_line_id_counter,
            )
            if not ol_chunk.empty:
                order_lines_count += len(ol_chunk)
                if should_log(order_lines_count, last_logged["order_lines"]):
                    logger.info("Order lines: %d", order_lines_count)
                    last_logged["order_lines"] = order_lines_count
                if args.output_format == "csv":
                    ol_chunk.to_csv(
                        str(order_lines_base) + ".csv",
                        mode="a",
                        header=first_write["order_lines"],
                        index=False,
                    )
                    first_write["order_lines"] = False
                else:
                    ol_chunk.to_parquet(
                        str(order_lines_base) + f"_part{chunk_idx:04d}.parquet",
                        index=False,
                    )
                order_line_id_counter += len(ol_chunk)

            ships_chunk = generate_shipments_for_orders(orders_chunk, carriers)
            if not ships_chunk.empty:
                shipments_count += len(ships_chunk)
                if should_log(shipments_count, last_logged["shipments"]):
                    logger.info("Shipments: %d", shipments_count)
                    last_logged["shipments"] = shipments_count

            sl_chunk = None
            if not ol_chunk.empty and not ships_chunk.empty:
                sl_chunk = generate_shipment_lines_for_chunk(
                    ships_chunk,
                    ol_chunk,
                    items,
                    carriers,
                    start_line_id=shipment_line_id_counter,
                    show_progress=False,
                )
                if sl_chunk is not None and not sl_chunk.empty:
                    shipment_lines_count += len(sl_chunk)
                    if should_log(shipment_lines_count, last_logged["shipment_lines"]):
                        logger.info("Shipment lines: %d", shipment_lines_count)
                        last_logged["shipment_lines"] = shipment_lines_count
                    if args.output_format == "csv":
                        sl_chunk.to_csv(
                            str(shipment_lines_base) + ".csv",
                            mode="a",
                            header=first_write["shipment_lines"],
                            index=False,
                        )
                        first_write["shipment_lines"] = False
                    else:
                        sl_chunk.to_parquet(
                            str(shipment_lines_base) + f"_part{chunk_idx:04d}.parquet",
                            index=False,
                        )
                    shipment_line_id_counter += len(sl_chunk)

            if not ships_chunk.empty:
                ships_with_costs = compute_shipment_totals_and_costs(
                    ships_chunk,
                    sl_chunk if sl_chunk is not None else pd.DataFrame(),
                    items,
                    show_progress=False,
                )
                shipment_costs_count += len(ships_with_costs)
                if should_log(shipment_costs_count, last_logged["shipment_costs"]):
                    logger.info("Shipment costs: %d", shipment_costs_count)
                    last_logged["shipment_costs"] = shipment_costs_count
                if args.output_format == "csv":
                    ships_with_costs = format_timestamp_cols(
                        ships_with_costs, ["ship_date", "delivery_date"]
                    )
                    ships_with_costs.to_csv(
                        str(shipments_base) + ".csv",
                        mode="a",
                        header=first_write["shipments"],
                        index=False,
                    )
                    first_write["shipments"] = False
                else:
                    ships_with_costs.to_parquet(
                        str(shipments_base) + f"_part{chunk_idx:04d}.parquet",
                        index=False,
                    )

            tracking_chunk = generate_tracking_events(
                ships_chunk,
                dcs,
                show_progress=False,
                log_summary=False,
                start_event_id=tracking_event_id_counter,
            )
            if not tracking_chunk.empty:
                if args.output_format == "csv":
                    tracking_chunk = format_timestamp_cols(tracking_chunk, ["event_timestamp"])
                    tracking_chunk.to_csv(
                        str(tracking_base) + ".csv",
                        mode="a",
                        header=first_write["tracking"],
                        index=False,
                    )
                    first_write["tracking"] = False
                else:
                    tracking_chunk.to_parquet(
                        str(tracking_base) + f"_part{chunk_idx:04d}.parquet",
                        index=False,
                    )
                tracking_count += len(tracking_chunk)
                tracking_event_id_counter += len(tracking_chunk)
                if should_log(tracking_count, last_logged["tracking"]):
                    logger.info("Tracking events: %d", tracking_count)
                    last_logged["tracking"] = tracking_count

        orders = None
        order_lines = None
        shipments = None
        shipment_lines = None
        tracking_events = None
        customer_item_preferences = None
        backorders = None
        invoices = None
        ar_ledger = None
        logger.info(
            "Final counts - Orders: %d, Order lines: %d, Shipments: %d, "
            "Shipment lines: %d, Shipment costs: %d, Tracking events: %d",
            orders_count,
            order_lines_count,
            shipments_count,
            shipment_lines_count,
            shipment_costs_count,
            tracking_count,
        )
        if args.output_format == "parquet":
            try:
                logger.info("Combining parquet parts into single files")
                _combine_parquet_parts(orders_base, out_dir / "orders.parquet")
                _combine_parquet_parts(order_lines_base, out_dir / "order_lines.parquet")
                _combine_parquet_parts(shipments_base, out_dir / "shipments.parquet")
                _combine_parquet_parts(shipment_lines_base, out_dir / "shipment_lines.parquet")
                _combine_parquet_parts(tracking_base, out_dir / "tracking_events.parquet")
            except Exception as e:
                logger.warning("Failed to combine parquet parts: %s", e)

        # Generate non-transactional tables after streaming
        logger.info("Generating customer_item_preferences, backorders, invoices, ar_ledger")

        # For streaming mode, we need to load orders back to generate
        # these tables. Load all streamed data from CSV or Parquet files
        if args.output_format == "csv":
            orders = pd.read_csv(out_dir / "orders.csv", low_memory=False)
            order_lines = pd.read_csv(out_dir / "order_lines.csv", low_memory=False)
            shipments = pd.read_csv(out_dir / "shipments.csv", low_memory=False)
        else:
            orders = pd.read_parquet(out_dir / "orders.parquet")
            order_lines = pd.read_parquet(out_dir / "order_lines.parquet")
            shipments = pd.read_parquet(out_dir / "shipments.parquet")

        customer_item_preferences = generate_customer_item_preferences(
            customers, items, orders, order_lines
        )
        backorders = generate_backorders(order_lines, items, inventory_monthly_snapshot)
        invoices = generate_invoices(orders, order_lines, customers, items)
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

        # Write these tables
        write_table("customer_item_preferences", customer_item_preferences)
        write_table("backorders", backorders)
        write_table("invoices", invoices)
        write_table("ar_ledger", ar_ledger)
        write_table("sales_reps", sales_reps)
        write_table("labor_costs", labor_costs)
        write_table("price_history", price_history)
        write_table("promotions", promotions)
        write_table("purchase_orders", purchase_orders)
        write_table("returns", returns)
        write_table("promo_orders", promo_orders)
        write_table("customer_contacts", customer_contacts)
    else:
        orders = generate_orders(customers, dcs)
        order_lines = generate_order_lines(orders, items, customers)
        shipments = generate_shipments(orders, dcs, carriers)
        shipment_lines = generate_shipment_lines(shipments, order_lines, items, carriers)
        shipments = compute_shipment_totals_and_costs(shipments, shipment_lines, items)
        tracking_events = generate_tracking_events(shipments, dcs)

        customer_item_preferences = generate_customer_item_preferences(
            customers, items, orders, order_lines
        )
        backorders = generate_backorders(order_lines, items, inventory_monthly_snapshot)
        invoices = generate_invoices(orders, order_lines, customers, items)
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

        if args.write_through:
            orders = write_table("orders", orders)
            order_lines = write_table("order_lines", order_lines)
            shipments = write_table("shipments", shipments)
            shipment_lines = write_table("shipment_lines", shipment_lines)
            tracking_events = write_table("tracking_events", tracking_events)
            customer_item_preferences = write_table(
                "customer_item_preferences", customer_item_preferences
            )
            backorders = write_table("backorders", backorders)
            invoices = write_table("invoices", invoices)
            ar_ledger = write_table("ar_ledger", ar_ledger)
            sales_reps = write_table("sales_reps", sales_reps)
            labor_costs = write_table("labor_costs", labor_costs)
            price_history = write_table("price_history", price_history)
            promotions = write_table("promotions", promotions)
            purchase_orders = write_table("purchase_orders", purchase_orders)
            returns = write_table("returns", returns)
            promo_orders = write_table("promo_orders", promo_orders)
            customer_contacts = write_table("customer_contacts", customer_contacts)

    # Write any remaining tables that weren't written during generation
    if args.write_through:
        # Small tables (skip if already written earlier)
        if customers is not None and "customers" not in written_tables:
            customers = write_table("customers", customers)
        if items is not None and "items" not in written_tables:
            items = write_table("items", items)
        if carriers is not None and "carriers" not in written_tables:
            carriers = write_table("carriers", carriers)
        if dcs is not None and "distribution_centers" not in written_tables:
            dcs = write_table("distribution_centers", dcs)
        if vendor_performance is not None and "vendor_performance" not in written_tables:
            vendor_performance = write_table("vendor_performance", vendor_performance)
        if vendor_item_details is not None and "vendor_item_details" not in written_tables:
            vendor_item_details = write_table("vendor_item_details", vendor_item_details)
        if item_costs is not None and "item_costs" not in written_tables:
            item_costs = write_table("item_costs", item_costs)
        if item_forecast is not None and "item_forecast" not in written_tables:
            item_forecast = write_table("item_forecast", item_forecast)
        if (
            inventory_monthly_snapshot is not None
            and "inventory_monthly_snapshot" not in written_tables
        ):
            inventory_monthly_snapshot = write_table(
                "inventory_monthly_snapshot", inventory_monthly_snapshot
            )
        if inventory_adjustments is not None and "inventory_adjustments" not in written_tables:
            inventory_adjustments = write_table("inventory_adjustments", inventory_adjustments)
        if dc_item_slotting is not None and "dc_item_slotting" not in written_tables:
            dc_item_slotting = write_table("dc_item_slotting", dc_item_slotting)
        if (
            customer_item_preferences is not None
            and "customer_item_preferences" not in written_tables
        ):
            customer_item_preferences = write_table(
                "customer_item_preferences", customer_item_preferences
            )
        if backorders is not None and "backorders" not in written_tables:
            backorders = write_table("backorders", backorders)
        if invoices is not None and "invoices" not in written_tables:
            invoices = write_table("invoices", invoices)
        if ar_ledger is not None and "ar_ledger" not in written_tables:
            ar_ledger = write_table("ar_ledger", ar_ledger)
        if sales_reps is not None and "sales_reps" not in written_tables:
            sales_reps = write_table("sales_reps", sales_reps)
        if labor_costs is not None and "labor_costs" not in written_tables:
            labor_costs = write_table("labor_costs", labor_costs)
        if price_history is not None and "price_history" not in written_tables:
            price_history = write_table("price_history", price_history)
        if promotions is not None and "promotions" not in written_tables:
            promotions = write_table("promotions", promotions)
        if purchase_orders is not None and "purchase_orders" not in written_tables:
            purchase_orders = write_table("purchase_orders", purchase_orders)
        if returns is not None and "returns" not in written_tables:
            returns = write_table("returns", returns)
        if promo_orders is not None and "promo_orders" not in written_tables:
            promo_orders = write_table("promo_orders", promo_orders)
        if customer_contacts is not None and "customer_contacts" not in written_tables:
            customer_contacts = write_table("customer_contacts", customer_contacts)

    tables = {
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

    for name, df in tables.items():
        if df is None:
            logger.info("Skipping %s (streamed to disk already or not generated)", name)
            continue
        write_table(name, df)


if __name__ == "__main__":
    main()

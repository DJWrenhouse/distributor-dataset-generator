![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)
# Synthetic Distributor Dataset Generator

A production-quality Python data generator that creates large, realistic synthetic datasets modeled after a wholesale distributor or B2B logistics operation. Designed for data engineers, analytics engineers, BI developers, data scientists, and database architects who need realistic relational data without accessing proprietary systems.

---

## What This Is

This project generates a **complete, multi-table synthetic dataset** covering every major domain of a wholesale distribution business: product catalog, inventory, procurement, order management, shipping, transportation costing, customer accounts, invoicing, accounts receivable, product returns, pricing history, promotional campaigns, labor operations, and sales force management. All data is generated with **business-driven logic** — distributions, relationships, and values reflect how real distributor datasets actually behave, not just random noise.

If you are looking for any of the following, this project may be exactly what you need:

- A **synthetic supply chain dataset** for SQL practice, analytics development, or BI tool demos
- A **fake distributor database** with realistic orders, inventory, customers, returns, and price history
- A **logistics dataset generator** with carriers, shipments, tracking events, and freight costs
- A **large-scale test dataset** for data warehouse or lakehouse development
- A **B2B order management dataset** with customer hierarchies, discounts, AR aging, promotions, and sales rep assignments
- A **procurement & returns dataset** with vendor purchase orders, product returns, and restocking logic
- A **pricing & promotion dataset** with effective-dated contract prices and promotional campaigns
- A **Python data generation framework** you can extend for your own schema
- A **reproducible dataset** with configurable seed, row counts, and output format

---

## Key Features

**Scale** — Configured by default to generate 6 million orders with proportional downstream tables. Scales down to small smoke-test sizes (100 orders) or up as far as your hardware allows.

**Streaming / chunked generation** — Large transactional tables (`orders`, `order_lines`, `shipments`, `shipment_lines`, `tracking_events`) can be streamed to disk in configurable chunks to keep peak memory usage low on commodity hardware.

**Reproducibility** — Pass `--seed` to lock `random`, `numpy`, and `Faker` so every run with the same seed produces the same output.

**Two output formats** — CSV (default) or Parquet, selectable via `--output-format`.

**Business-driven realism** — Every table uses domain logic, not uniform random draws:
- Backorder probability increases when inventory is short relative to demand
- Vendor performance scores (fill rate, on-time rate) feed downstream vendor-item rankings and MOQ/order-multiple rules
- Customer discount percentages are applied consistently across order lines, invoices, and AR entries
- Inventory aging buckets, excess, and obsolete quantities reflect item lifecycle stage
- Shipment costs roll up from line-level item shipping cost per unit, adjusted for weight, cube, and carrier type
- AR payment behavior is governed by per-customer risk score
- Item NMFC freight class is assigned by product category with density-based fallback and mild stochastic variation
- Forecast error (MAPE/MAD) is tied to ABC/XYZ classification
- Product returns are sampled from completed orders with condition-based credit adjustments
- Purchase orders are derived from inventory on-order levels with vendor fill-rate applied
- Price history tracks item list prices with 2–5 effective-dated versions and contract prices for high-value customer-item pairs
- Promotions are time-bounded campaigns (PCTOFF/FIXEDOFF/BOGO) with 10–15% of orders tagged as promotional
- Labor costs reflect per-DC department workload (Receiving, Picking, Packing, Shipping, Inventory Control) scaled by capacity utilization
- Sales rep assignments and customer contacts model regional territories and account tier relationships

**Preview mode** — A separate `generate_preview` entrypoint generates small per-table samples (configurable row counts) with a validation summary so you can inspect shapes and distributions before running a full generation job.

**Preflight estimates** — Before any generation begins, the CLI prints expected row counts and approximate file sizes for every output table and asks for confirmation. Non-interactive runs skip the prompt automatically.

**Heartbeat logging** — Long-running generators emit periodic progress logs so you can monitor multi-hour runs.

**Write-through mode** — Reference tables are written to disk as soon as they are finalized, reducing peak memory pressure before large transactional tables are generated.

---

## Generated Tables

### Reference / Master Data

| Table | Description |
|---|---|
| `categories` | Product category reference, sourced from item catalog |
| `vendors` | Vendor master (2,000 vendors by default) |
| `vendor_performance` | Per-vendor fill rate, on-time delivery rate, defect rate, audit date |
| `vendor_item_details` | Vendor–item cross-reference with MOQ, order multiple, and vendor rank |
| `distribution_centers` | 35 DCs with GeoNames-sourced city/state/zip, region, time zone, capacity |
| `customers` | 10,000 customers with account hierarchy (master/sub), segment, credit limit, discount %, address |
| `items` | Product master from external catalog with dimensions, lead time, NMFC class, logistics attributes |
| `item_costs` | Effective-dated cost history: unit cost, unit price, shipping cost, freight cost, duty rate, landed cost, MAP price |
| `item_forecast` | Monthly demand forecasts with ABC/XYZ class, seasonality index, lifecycle stage, MAPE/MAD |

### Inventory & Operations

| Table | Description |
|---|---|
| `inventory` | Per-DC item inventory with qty on hand, on order, reorder point, and cost snapshots |
| `inventory_monthly_snapshot` | 12-month rolling inventory history per DC/item with aging buckets, excess, obsolete, cycle count accuracy |
| `inventory_adjustments` | Warehouse adjustment records (cycle count, damage, shrink, RMA) with velocity-weighted sampling |
| `dc_item_slotting` | Warehouse slotting assignments with pick/replenishment locations, velocity class (A/B/C), slotting rank |

### Transportation & Order Management

| Table | Description |
|---|---|
| `carriers` | Carrier master with type (parcel, LTL, courier, company fleet), service level, base rates, fuel surcharge |
| `driver_costs` | Per-carrier driver cost rates: hourly, per-mile, per-stop |
| `fuel_costs` | State-level diesel and gasoline price snapshots |
| `orders` | Order headers: customer, DC, date, ship method, status (80% Complete, 10% Open, 7% Backordered, 3% Cancelled) |
| `order_lines` | Line-level items, quantities, and discount-adjusted prices (1–10 lines per order) |
| `shipments` | One shipment per order with carrier, ship/promised/delivery dates, type, and rolled-up weight/cube/cost |
| `shipment_lines` | One row per order line per shipment with carton number, weight, cube, freight upcharge |
| `tracking_events` | Four milestone events per shipment: `departed_dc`, `arrived_hub`, `out_for_delivery`, `delivered` |

### Commercial & Finance

| Table | Description |
|---|---|
| `customer_item_preferences` | RFM-scored customer–item affinity (recency + frequency + monetary), top 50 items per customer |
| `backorders` | Backorder records with fill timing, reason codes, and shortage-probability logic |
| `invoices` | One invoice per order with merchandise total, freight billed, tax, discount, and payment terms |
| `ar_ledger` | AR entries with amount paid, balance, aging bucket (Current / 1–30 / 31–60 / 61–90 / 90+), credit hold flag, write-off reason |

### Returns, Pricing & Sales

| Table | Description |
|---|---|
| `returns` | Product return records with condition code, reason, qty returned, and restocking decisions. Samples ~4–8% of completed orders. |
| `purchase_orders` | Vendor purchase orders derived from inventory on-order quantities with receipt tracking, fill rate modeling, and status (Received/Partial/Open/Cancelled). |
| `price_history` | Effective-dated item list prices and contract prices with historical versions (2–5 per item) and discount percentages for customer-item pairs. |
| `promotions` | Time-bounded promotional campaigns (7–30 days) with discount type (PCTOFF/FIXEDOFF/BOGO), target category, and customer segment constraints. |
| `promo_orders` | Bridge table linking promotions to orders, capturing which orders received active promotion discounts. |

### Human Resources & Operations

| Table | Description |
|---|---|
| `labor_costs` | Weekly distribution-center labor records by department (Receiving, Picking, Packing, Shipping, Inventory Control) with hours worked, cost rates, and headcount. 104 weeks × 5 departments × num_DCs. |
| `sales_reps` | Sales representative master with regional assignment, territory, hire date, and contact email. 75 reps distributed across 6 regions. |
| `customer_contacts` | Customer contact assignments linking customers to sales reps with contact name, role, and last contact date. One primary contact per customer; secondary contacts for Government/Corporate segments. |

**Total: 32 output tables** across reference, inventory, transportation, finance, pricing, and operations domains.

---

## Approximate Row Counts (Default Configuration)

| Table | Approx Rows |
|---|---|
| orders | 6,000,000 |
| order_lines | ~33,000,000 |
| shipments | 6,000,000 |
| shipment_lines | ~33,000,000 |
| tracking_events | ~24,000,000 |
| invoices | ~6,000,000 |
| ar_ledger | ~6,000,000 |
| backorders | ~2,600,000 |
| inventory_monthly_snapshot | ~252,000 (35 DCs × catalog items × 12 months) |
| dc_item_slotting | ~1,575,000 (35 DCs × catalog items) |
| item_costs | ~3× item count (versioned history) |
| item_forecast | ~12× item count (monthly forward) |
| returns | ~400,000–600,000 (4–8% of completed order lines) |
| purchase_orders | ~120,000–200,000 (varies with inventory on-order levels) |
| price_history | ~120,000–180,000 (2–5 versions per item + contract prices) |
| promotions | ~300–400 (7–30 day windows over full date range) |
| promo_orders | ~500,000–800,000 (10–15% of orders tagged with active promos) |
| labor_costs | ~20,000–25,000 (104 weeks × 5 departments × 35 DCs) |
| sales_reps | 75 (fixed population across 6 regions) |
| customer_contacts | ~12,000–15,000 (1+ contact per customer) |

Reference tables (vendors, customers, items, carriers, etc.) are small (hundreds to low thousands of rows). Transactional tables scale linearly with `--n-orders`.

---

## Requirements

- Python 3.9+
- pandas
- numpy
- pyarrow (for Parquet output)
- Faker
- python-dateutil
- requests (for GeoNames auto-download)

Install:

```bash
pip install -r requirements.txt
```

Or install as a package:

```bash
pip install -e .
```

---

## Development Setup & Code Quality

This project uses **pre-commit hooks** to catch common errors (syntax errors, code style issues) before commits.

After cloning and installing dependencies, set up pre-commit:

```bash
pip install pre-commit      # Already in requirements.txt
pre-commit install          # Install git hooks
pre-commit run --all-files  # (Optional) Run checks on all files
```

On subsequent commits, the following checks run automatically:
- **Python syntax validation** — catches unterminated strings, missing colons, etc.
- **Code formatting with Black** — maintains consistent code style
- **Linting with flake8** — detects common mistakes (max 120 char line length)
- **Trailing whitespace & line endings** — removes cleanup issues
- **Private key detection** — prevents accidental credential commits

If any check fails, the commit is blocked and errors are shown. Fix the issues and commit again.

---

## Quick Start

```bash
# Create and activate a virtual environment
python -m venv .venv
.\.venv\Scripts\Activate.ps1       # Windows PowerShell
# source .venv/bin/activate         # Linux/macOS

pip install -r requirements.txt

# Generate a small test dataset (100 orders, reproducible)
python -m data_generator.generate_all \
  --n-orders 100 \
  --seed 42 \
  --output-dir output \
  --assume-yes

# Generate a medium dataset with streaming to save memory
python -m data_generator.generate_all \
  --n-orders 500000 \
  --seed 42 \
  --output-dir output \
  --stream \
  --chunk-size 10000 \
  --assume-yes

# Generate preview samples for individual tables
python -m data_generator.generate_preview \
  --tables orders,order_lines,shipments,invoices \
  --n-orders 500 \
  --output-dir output/preview \
  --assume-yes
```

---

## CLI Reference — `generate_all`

```
python -m data_generator.generate_all [flags]
```

| Flag | Default | Description |
|---|---|---|
| `--output-dir` | `output` | Directory to write output files |
| `--seed` | unset | Integer seed for `random`, `numpy`, and `Faker` |
| `--n-orders` | 6,000,000 | Override order count |
| `--n-carriers` | 20 | Override carrier count |
| `--verbose` | off | Enable debug-level logging |
| `--assume-yes` | off | Skip preflight confirmation prompt |
| `--no-confirm` | off | Disable confirmation prompt (automation) |
| `--stream` | off | Stream large transactional tables in chunks |
| `--chunk-size` | 10,000 | Rows per chunk when `--stream` is enabled |
| `--log-interval` | 500,000 | Rows between progress log lines |
| `--write-through` | on | Write each table to disk as soon as it is finalized |
| `--no-write-through` | off | Hold all tables in memory until the end |
| `--output-format` | `csv` | Output format: `csv` or `parquet` |

---

## CLI Reference — `generate_preview`

```
python -m data_generator.generate_preview [flags]
```

| Flag | Default | Description |
|---|---|---|
| `--tables` | all | Comma-separated table names, repeatable |
| `--output-dir` | `output/preview` | Output directory |
| `--output-format` | `csv` | `csv` or `parquet` |
| `--seed` | 1 | Random seed |
| `--assume-yes` | off | Skip confirmation |
| `--no-confirm` | off | Disable confirmation prompt |
| `--n-vendors` | 50 | Vendor count for preview |
| `--n-carriers` | config default | Carrier count for preview |
| `--n-dcs` | 5 | DC count for preview |
| `--n-customers` | 200 | Customer count for preview |
| `--n-orders` | 200 | Order count for preview |
| `--item-limit` | 500 | Max catalog items to use |
| `--row-limit` | unset | Cap rows per output table after generation |
| `--row-limit-strict` | off | Apply row cap to upstream tables before downstream generation |
| `--log-interval` | 1,000 | Rows between progress logs |

---

## Streaming Notes

When `--stream` is enabled, `orders`, `order_lines`, `shipments`, `shipment_lines`, and `tracking_events` are generated and written in chunks rather than held in memory all at once. This is recommended for any run above ~500k orders on a machine with less than 32 GB of RAM.

In Parquet mode, streamed tables are written as numbered part files and then merged into a single `.parquet` file at the end of the run.

Tracking event IDs are monotonically increasing across chunks — no per-chunk ID reset — so the final `tracking_events` table has globally unique, sequential IDs. This behavior is covered by a regression test (`tests/test_tracking_event_ids.py`).

Timestamp fields in CSV output are normalized to `yyyy-MM-dd HH:mm:ss`. DST edge-case local times are shifted forward by one hour.

---

## GeoNames Data

City, state, and ZIP code fields are sampled from the GeoNames US postal codes dataset (CC BY 4.0). The generator downloads `US.zip` on the first run and caches `US.txt` in `data/geonames/`. For offline environments, pre-download `US.txt` from https://download.geonames.org/export/zip/ and place it at `data/geonames/US.txt`.

---

## Item Catalog

Items are generated from an external product catalog CSV at `data/items/items_with_dimensions.csv`. The catalog provides product codes, descriptions, categories, subcategories, unit costs, unit prices, and physical dimensions. The generator enriches these records with synthetic IDs, vendor assignments, lead times, logistics attributes (density, dimensional weight, NMFC class), and stochastic variation.

---

## Configuration

Default generation parameters are set in `data_generator/config.py`. The most commonly overridden values are also exposed as CLI flags. Key defaults:

```python
START_DATE = datetime(2023, 7, 1)
END_DATE   = datetime(2025, 12, 31)

N_VENDORS   = 2000
N_DCS       = 35
N_CUSTOMERS = 10000
N_CARRIERS  = 20
N_ORDERS    = 6000000

DISCOUNT_PCT_MIN = 0.0
DISCOUNT_PCT_MAX = 5.0
```

Shared lookup constants (US states, regions, time zones, shipment types) live in `data_generator/constants.py` so they are consistent across all modules.

---

## Project Structure

```
data_generator/
├── generate_all.py            # Full-generation CLI entrypoint
├── generate_preview.py        # Preview/sample CLI entrypoint
├── config.py                  # Numeric defaults and date ranges
├── constants.py               # Shared lookup lists (regions, shipment types, etc.)
└── modules/
    ├── ar_ledger.py           # Accounts receivable ledger
    ├── backorders.py          # Backorder generation with shortage logic
    ├── categories.py          # Product category reference
    ├── costs.py               # Carriers, driver costs, fuel costs
    ├── customer_item_preferences.py  # RFM-based preference scoring
    ├── customer_contacts.py   # Customer contact assignments
    ├── customers.py           # Customer master with account hierarchy
    ├── dc_item_slotting.py    # Warehouse slotting assignments
    ├── distribution_centers.py
    ├── geonames.py            # GeoNames dataset loader and cache
    ├── inventory.py
    ├── inventory_adjustments.py
    ├── inventory_monthly_snapshot.py
    ├── invoices.py
    ├── item_costs.py          # Effective-dated cost history
    ├── item_forecast.py       # ABC/XYZ demand forecasts
    ├── items.py               # Item master with NMFC logic
    ├── labor_costs.py         # DC weekly labor records by department
    ├── order_lines.py         # Row-by-row and vectorized line generation
    ├── orders.py              # Row-by-row and chunked order generation
    ├── price_history.py       # Effective-dated item list & contract prices
    ├── promotions.py          # Promotional campaigns and orders bridge
    ├── purchase_orders.py     # Vendor purchase orders from inventory
    ├── returns.py             # Product returns from completed orders
    ├── sales_reps.py          # Sales rep master and customer contacts
    ├── shipment_lines.py      # Shipment line expansion and cartonization
    ├── shipments.py           # Shipment headers and cost rollup
    ├── tracking.py            # Milestone tracking events
    ├── vendor_item_details.py
    ├── vendor_performance.py
    └── vendors.py

data/
├── items/
│   └── items_with_dimensions.csv   # External product catalog (required)
└── geonames/
    └── US.txt                      # Auto-downloaded GeoNames postal data

sql/
└── schema.sql                      # PostgreSQL schema for all 32 tables

tests/
├── test_smoke.py                   # End-to-end streamed generation smoke test
├── test_new_tables_smoke.py        # Validation for extension tables
└── test_tracking_event_ids.py      # Regression test for ID uniqueness across chunks
```

---

## Running Tests

```bash
pip install pytest
pytest tests/
```

The smoke test runs a full end-to-end generation job with 100 orders and verifies output files are created. The tracking event ID test verifies that streamed chunk boundaries do not reset event IDs.

---

## SQL Schema

`sql/schema.sql` contains a PostgreSQL-compatible DDL for all 32 tables, including 24 core tables and 8 extension tables. The schema includes primary keys, foreign key references, and appropriate numeric precision for cost and rate fields. It can be adapted for other databases (MySQL, SQLite, DuckDB, BigQuery, Snowflake) with minor type adjustments.

**Core tables** (orders, shipments, inventory, etc.) are foundational operational and reference data.

**Extension tables** (returns, purchase_orders, price_history, promotions, labor_costs, sales_reps) are additive analyses and derived tables that enhance the core dataset with business logic for returns management, procurement planning, dynamic pricing, promotional campaigns, operational labor tracking, and sales force management. These tables are generated automatically within both `generate_all()` and `generate_preview()` pipelines and are not destructive to any existing data.

To load the generated CSVs into PostgreSQL after generation:

```sql
\copy orders FROM 'output/orders.csv' CSV HEADER;
\copy order_lines FROM 'output/order_lines.csv' CSV HEADER;
-- repeat for remaining tables
```

---

## Use Cases

This dataset is well-suited for:

- **SQL and analytics practice** — complex joins across a normalized relational schema with realistic cardinalities
- **Data warehouse / lakehouse development** — building and testing dimensional models (star schema, OBT) from raw operational data
- **BI and dashboarding demos** — pre-built data for inventory, order fill rate, freight cost, AR aging, returns, pricing, and sales analytics dashboards
- **Machine learning feature engineering** — customer segmentation, demand forecasting, backorder prediction, churn modeling, price optimization, and sales forecasting
- **dbt project development** — staging, intermediate, and mart layer modeling on a realistic source schema with extension tables
- **Data pipeline testing** — end-to-end ELT pipeline development with a source that has known shapes, nulls, and edge cases
- **Database performance benchmarking** — large fact tables (33M+ order lines, 1M+ returns) suitable for query optimizer testing
- **Returns & procurement analytics** — realistic product return patterns, purchase order workflows, and vendor performance analysis
- **Pricing & promotion analytics** — effective-dated price history, promotional campaign attribution, and discount impact modeling
- **Interview and assessment datasets** — realistic supply chain data for technical screening exercises covering core and extended domains

---

## Notes on Realism

- **Customer hierarchy** — 60% of customers are master accounts; the remainder are sub-accounts linked to a master, reflecting a typical distributor B2B account structure.
- **Order status distribution** — 80% Complete, 10% Open, 7% Backordered, 3% Cancelled.
- **Shipment type distribution** — 35% standard, 25% local, 10% each for wrap-and-label, drop-ship, expedited, and freight.
- **Backorder fill rate** — 85% of backorders are eventually filled. Fill timing uses a Gaussian distribution around item lead time.
- **AR payment behavior** — governed by a per-customer risk score. Low-risk customers pay 80–100% of invoices; high-risk customers pay 0–40%. Invoices over 90 days past due with balances under $200 have a 30% chance of being written off.
- **Vendor performance** — fill rate and on-time delivery rate improve slightly with vendor ID (a proxy for vendor size/maturity), with added randomness to avoid perfect correlation.
- **Item NMFC class** — assigned by product category/subcategory override first, then density-based fallback (the standard density-to-NMFC staircase), with mild stochastic variation to adjacent classes for realism.
- **Inventory aging** — aging buckets (0–30, 31–60, 61–90, 90+) are generated with a bias toward newer stock. End-of-life items carry 20–50% obsolete quantity; mature items carry 5–15%.

---

## License

This project is licensed under the [MIT License](LICENSE). See `LICENSE` for terms. 

GeoNames data is used under [Creative Commons Attribution 4.0](https://creativecommons.org/licenses/by/4.0/).

---

## Author

Andrew Johnson — [andrew_johnson@djwrenhouse.com](mailto:andrew_johnson@djwrenhouse.com)

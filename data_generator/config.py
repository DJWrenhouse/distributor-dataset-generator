"""Default configuration values for dataset generation.

This module defines global defaults such as entity counts, date boundaries,
and pricing ranges. Runtime CLI flags in generator entrypoints can override a
subset of these values for a single execution.
"""

from datetime import datetime

from data_generator.constants import (  # noqa: F401
    REGIONS,
    SHIPMENT_TYPES,
    TIME_ZONES,
)

START_DATE = datetime(2023, 7, 1)
END_DATE = datetime(2025, 12, 31)

N_VENDORS = 2000
N_DCS = 35
N_CUSTOMERS = 10000
N_CARRIERS = 20
N_ORDERS = 6000000  # adjust as needed
LOG_INTERVAL = 500000

# Chunk processing settings for backorders and invoices
CHUNK_SIZE_BACKORDERS = 100000
CHUNK_SIZE_INVOICES = 100000

# Log interval for order-level processing (order_lines)
# Shows progress every N orders processed
LOG_INTERVAL_ORDERS = 100000

# GeoNames US postal codes dataset (CC BY 4.0)
GEONAMES_URL = "https://download.geonames.org/export/zip/US.zip"
GEONAMES_CACHE_DIR = "data/geonames"
GEONAMES_US_PATH = "data/geonames/US.txt"

# External item catalog
ITEM_CATALOG_PATH = "data/items/items_with_dimensions.csv"

# Customer discount percentage range (0-5 by default)
DISCOUNT_PCT_MIN = 0.0
DISCOUNT_PCT_MAX = 5.0

FUEL_BASE_DIESEL = (3.50, 5.00)
FUEL_BASE_GAS = (2.80, 4.20)

DRIVER_HOURLY_RANGE = (18.0, 35.0)
DRIVER_PER_MILE_RANGE = (0.30, 0.70)
DRIVER_PER_STOP_RANGE = (8.0, 25.0)

# `SHIPMENT_TYPES` is provided from data_generator.constants

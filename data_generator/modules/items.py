"""
Generate item master data and item-attribute data from the external catalog.

This module enriches catalog records with synthetic IDs, vendor assignments,
category links, lead-time values, and realistic logistics attributes including
NMFC class with category overrides and mild stochastic variation.
"""

import logging

import numpy as np
import pandas as pd

import data_generator.config as config

_RNG = np.random.default_rng()


def compute_cube_ft(length_in, width_in, height_in):
    """Convert box dimensions from inches to cubic feet."""
    return (length_in * width_in * height_in) / 1728.0


def _compute_density(weight_lb, cube_ft):
    """Compute density in lb per cubic foot, guarding against zero or null
    cube."""
    if cube_ft is None or cube_ft == 0:
        return None
    return float(weight_lb) / float(cube_ft)


def _compute_dim_weight_lb(length_in, width_in, height_in, divisor=139.0):
    """Compute dimensional weight in pounds using a standard divisor."""
    if any(v is None for v in (length_in, width_in, height_in)):
        return None
    dim = (float(length_in) * float(width_in) * float(height_in)) / divisor
    return dim


# Category driven NMFC overrides tailored to your taxonomy
CATEGORY_NMFC_OVERRIDES = {
    # Furniture
    "Furniture": "125",
    # Office Supplies
    "Office Supplies": "150",
    "Paper": "70",
    "Envelopes": "150",
    "Labels": "150",
    "Binders": "150",
    "Fasteners": "150",
    "Storage": "150",
    "Art": "150",
    "Appliances": "125",
    # Technology
    "Technology": "100",
    "Phones": "100",
    "Machines": "100",
    "Copiers": "92.5",
    "Accessories": "100",
}


def _density_based_nmfc(density):
    """Density based fallback NMFC class assignment."""
    if density is None:
        return "150"
    if density < 1:
        return "300"
    elif density < 2:
        return "250"
    elif density < 4:
        return "150"
    elif density < 6:
        return "125"
    elif density < 8:
        return "100"
    elif density < 10:
        return "92.5"
    else:
        return "70"


def _apply_nmfc_variation(base_class):
    """Apply mild stochastic variation to NMFC class for realism."""
    variations = {
        "300": ["300", "250"],
        "250": ["250", "300"],
        "150": ["150", "125"],
        "125": ["125", "150"],
        "100": ["100", "92.5"],
        "92.5": ["92.5", "100"],
        "70": ["70"],  # stable
    }
    choices = variations.get(str(base_class), [str(base_class)])
    return _RNG.choice(choices)


def _assign_nmfc_class(row):
    """Assign NMFC class using category overrides first, then density
    fallback."""
    cat = row.get("product_category")
    subcat = row.get("product_subcategory")
    density = row.get("density_lb_per_cuft")

    # Subcategory override
    if subcat in CATEGORY_NMFC_OVERRIDES:
        base = CATEGORY_NMFC_OVERRIDES[subcat]
        return _apply_nmfc_variation(base)

    # Category override
    if cat in CATEGORY_NMFC_OVERRIDES:
        base = CATEGORY_NMFC_OVERRIDES[cat]
        return _apply_nmfc_variation(base)

    # Density fallback
    base = _density_based_nmfc(density)
    return _apply_nmfc_variation(base)


def generate_items(vendors_df):
    """Build the item table from catalog data."""
    logger = logging.getLogger(__name__)
    catalog = pd.read_csv(config.ITEM_CATALOG_PATH)

    # Normalize column names
    catalog = catalog.rename(
        columns={
            "weight_lbs": "weight_lb",
            "cubic_ft": "cube_ft",
            "produc_subcategory": "product_subcategory",
        }
    )

    # Compute cube_ft if missing
    if "cube_ft" not in catalog.columns:
        catalog["cube_ft"] = compute_cube_ft(
            catalog["length_in"],
            catalog["width_in"],
            catalog["height_in"],
        )
    else:
        mask = (
            catalog["cube_ft"].isna()
            & catalog["length_in"].notna()
            & catalog["width_in"].notna()
            & catalog["height_in"].notna()
        )
        catalog.loc[mask, "cube_ft"] = catalog.loc[mask].apply(
            lambda r: compute_cube_ft(r["length_in"], r["width_in"], r["height_in"]),
            axis=1,
        )

    # Assign IDs
    catalog = catalog.reset_index(drop=True)
    catalog["item_id"] = np.arange(1, len(catalog) + 1)

    # Assign vendors
    vendor_ids = vendors_df["vendor_id"].to_numpy()
    catalog["vendor_id"] = _RNG.choice(vendor_ids, size=len(catalog))

    # Lead times
    catalog["lead_time_days"] = _RNG.integers(1, 31, size=len(catalog))

    # Logistics attributes
    catalog["density_lb_per_cuft"] = catalog.apply(
        lambda r: _compute_density(r.get("weight_lb"), r.get("cube_ft")),
        axis=1,
    )

    catalog["dimensional_weight_lb"] = catalog.apply(
        lambda r: _compute_dim_weight_lb(
            r.get("length_in"),
            r.get("width_in"),
            r.get("height_in"),
        ),
        axis=1,
    )

    catalog["stackability_flag"] = True
    catalog["hazmat_flag"] = False

    # NMFC class assignment with variation
    catalog["nmfc_class"] = catalog.apply(_assign_nmfc_class, axis=1)

    # Items table
    items_df = pd.DataFrame(
        {
            "item_id": catalog["item_id"].to_numpy(),
            "item_sku": catalog["product_code"].astype(str),
            "item_description": catalog["description"].astype(str),
            "category": catalog["product_category"].astype(str),
            "subcategory": catalog["product_subcategory"].fillna("").astype(str),
            "vendor_id": catalog["vendor_id"].to_numpy(),
            "lead_time_days": catalog["lead_time_days"].astype(int),
            "length_in": catalog["length_in"].astype(float).round(2),
            "width_in": catalog["width_in"].astype(float).round(2),
            "height_in": catalog["height_in"].astype(float).round(2),
            "weight_lb": catalog["weight_lb"].astype(float).round(3),
            "cube_ft": catalog["cube_ft"].astype(float).round(4),
            "density_lb_per_cuft": catalog["density_lb_per_cuft"].astype(float).round(4),
            "dimensional_weight_lb": catalog["dimensional_weight_lb"].astype(float).round(4),
            "stackability_flag": catalog["stackability_flag"].astype(bool),
            "hazmat_flag": catalog["hazmat_flag"].astype(bool),
            "nmfc_class": catalog["nmfc_class"],
        }
    )

    logger.info("Loaded %d items from catalog", len(items_df))
    return items_df

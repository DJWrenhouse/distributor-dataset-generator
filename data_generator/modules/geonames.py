"""Load and cache GeoNames postal-code data for US location sampling.

The module ensures `US.txt` is available locally, downloading it on demand,
then returns normalized city/state/zip tuples for address generation.
"""

from __future__ import annotations

import csv
import logging
import os
import tempfile
import zipfile
from pathlib import Path
from urllib.parse import urlparse

import requests

import data_generator.config as config

_GEONAMES_CACHE = None


def _download_geonames_zip(url: str, dest_dir: Path) -> Path:
    """Download the GeoNames ZIP to a temporary file in the cache directory.

    Args:
        url: Source URL for the GeoNames ZIP archive.
        dest_dir: Directory where the temporary ZIP should be created.

    Returns:
        Path: Path to the downloaded temporary ZIP file.
    """
    # Validate URL scheme to prevent file:// attacks
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        raise ValueError(f"Invalid URL scheme: {parsed.scheme}. Only http and https are allowed.")

    dest_dir.mkdir(parents=True, exist_ok=True)
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".zip", dir=str(dest_dir))
    tmp.close()

    response = requests.get(url, timeout=30)
    response.raise_for_status()
    with open(tmp.name, "wb") as f:
        f.write(response.content)

    return Path(tmp.name)


def _ensure_geonames_us_file() -> Path:
    """Ensure `US.txt` exists locally and return its path.

    Returns:
        Path: Local filesystem path to the extracted GeoNames `US.txt` file.

    Raises:
        RuntimeError: If downloading or extraction fails.
    """
    path = Path(config.GEONAMES_US_PATH)
    if path.exists():
        return path

    url = config.GEONAMES_URL
    logger = logging.getLogger(__name__)
    logger.info("Downloading GeoNames dataset from %s", url)

    zip_path = None
    try:
        zip_path = _download_geonames_zip(url, Path(config.GEONAMES_CACHE_DIR))
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extract("US.txt", path.parent)
    except Exception as exc:
        raise RuntimeError(
            "Failed to download GeoNames dataset. "
            "Download US.zip from https://download.geonames.org/export/zip/ "
            "and place US.txt in data/geonames/"
        ) from exc
    finally:
        try:
            if zip_path is not None and zip_path.exists():
                os.remove(zip_path)
        except Exception:
            pass

    return path


def load_us_zip_records():
    """Load and cache GeoNames records as `(city, state, zip)` tuples.

    Returns:
        list[tuple[str, str, str]]: Normalized location tuples.

    Raises:
        RuntimeError: If the dataset loads but contains no usable records.
    """
    global _GEONAMES_CACHE
    if _GEONAMES_CACHE is not None:
        return _GEONAMES_CACHE

    path = _ensure_geonames_us_file()
    records = []

    with path.open("r", encoding="utf-8") as handle:
        reader = csv.reader(handle, delimiter="\t")
        for row in reader:
            if len(row) < 5:
                continue
            zip_code = row[1].strip()
            city = row[2].strip()
            state = row[4].strip()
            if not zip_code or not city or not state:
                continue
            records.append((city, state, zip_code))

    if not records:
        raise RuntimeError("GeoNames dataset loaded but no usable records found.")

    _GEONAMES_CACHE = records
    return records

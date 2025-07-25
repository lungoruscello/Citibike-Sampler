import os
from datetime import datetime
from multiprocessing import cpu_count

from .global_paths import *

__all__ = [
    "ASSET_DIR",
    "FIRST_SUPPORTED_YEAR",
    "NOW_YEAR",
    "NOW_MONTH",
    "LAST_BUNDLED_YEAR",
    "get_cache_dir",
    "get_max_concurrency",
]

DEFAULT_MAX_CONCURRENCY = max(1, cpu_count() - 2)

FIRST_SUPPORTED_YEAR = 2020
LAST_BUNDLED_YEAR = 2023
NOW_YEAR = datetime.now().year
NOW_MONTH = datetime.now().month


def get_cache_dir():
    """
    Return the local cache directory for downloaded CitiBike source data.

    Note
    ----
    You can override the default cache directory by setting the "CITIBIKE_CACHE_DIR"
    environment variable.
    """
    cache_dir = os.getenv("CITIBIKE_CACHE_DIR", str(DEFAULT_CACHE_DIR))
    cache_dir = Path(cache_dir)
    return cache_dir


def get_max_concurrency():
    """
    Return the maximum concurrency for parallel data processing.

    Note
    ----
    You can override the default concurrency limit by setting the "CITIBIKE_MAX_CONCURRENCY"
    environment variable.
    """
    num_threads = os.getenv("CITIBIKE_MAX_CONCURRENCY", str(DEFAULT_MAX_CONCURRENCY))
    return int(num_threads)

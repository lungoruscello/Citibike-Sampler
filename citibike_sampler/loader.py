"""
loader.py

A simple data loader for the raw, unthinned Citi Bike trip-data.

TODO: Parallelise the data loader
"""
import logging
from calendar import monthrange

import pandas as pd
from tqdm.autonotebook import tqdm

from citibike_sampler.downloader import glob_csv_paths_month, is_month_fully_cached, download
from citibike_sampler.misc import normalise_time_range, month_list

logger = logging.getLogger(__name__)


def load_all(start, end=None):
    """
    Load the full, unthinned Citi Bike trip-data for a specific time period.

    Beware when trying to ingest data for many months as the full trip-data
    is large.

    start : str, int or tuple[int, int]
        The start year or month of the period for which to load the full
        trip-data (inclusive).
        Accepted formats:
        - Tuple: (2020, 1)
        - Integer: 2020 → (2020, 1)
        - String: "2020" → (2020, 1)
        - String: "2020-5" → (2020, 5)
        - String: "2020-05" → (2020, 5)
    end : str, int or tuple[int, int], optional
        The end year or month of the period for which to load the full
        trip-data (inclusive).
        Accepted formats:
        - Tuple: (2020, 12)
        - Integer: 2020 → (2020, 12)
        - String: "2020" → (2020, 1)
        - String: "2020-5" → (2020, 5)
        - String: "2020-05" → (2020, 5)
        If `None` is provided, data will be loaded only for the single year
        or single month specified in `start`.
    """
    start, end = normalise_time_range(start, end)

    # trigger Citi Bike data downloads from S3 when needed
    download(start, end, skip_if_exists=True, warn=False)  # will also validate the cache

    # load months
    monthly_dfs = []
    pbar = tqdm(
        month_list(start, end),
        desc=f'Loading months',
        leave=False
    )
    for year, month in pbar:
        monthly_dfs.append(_load_full_month(year, month))

    # concatenate
    return pd.concat(monthly_dfs, ignore_index=True)


def _load_full_month(year, month):
    assert is_month_fully_cached(year, month)

    # get paths to all trip-data shards for this month
    csv_paths = glob_csv_paths_month(year, month)

    # load all shards
    total_obs = 0
    shards = []
    pbar = tqdm(
        csv_paths,
        desc=f'Loading shards for month {year}-{month}',
        leave=False
    )
    for path in pbar:
        shard = _load_csv_shard(path)
        total_obs += len(shard)
        shards.append(shard)

    # concatenate and sort
    df = pd.concat(shards, ignore_index=True)
    df.sort_values('ended_at', inplace=True)

    # validate the concatenated data
    assert df.duplicated('ride_id').sum() == 0
    assert len(df) == total_obs

    expected_days = monthrange(year, month)[1]
    actual_days = df.ended_at.dt.date.nunique()
    if expected_days != actual_days:
        logger.warning(
            f"{year}-{month}: Expected {expected_days} days worth of "
            f"trip-data, but got data for only {actual_days} days."
        )

    return df


def _load_csv_shard(csv_path):
    return pd.read_csv(
        csv_path,
        low_memory=False,
        parse_dates=["started_at", "ended_at"],
    )

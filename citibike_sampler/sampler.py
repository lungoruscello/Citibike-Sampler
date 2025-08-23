"""
sampler.py

Provides logic to produce a single pandas DataFrame containing a random
sample of Citi Bike records for periods spanning several months or years.

The full Citi Bike data is massive, with millions of rides recorded each
single month -- and recorded in a large number of smaller data shards.
Having access to a random sample of the Citi Bike data from several years
in one single dataframe can greatly simplify data analysis, including the
development and testing of ML models.
"""

import logging
import random
from concurrent.futures import as_completed, ProcessPoolExecutor
from dataclasses import dataclass
from hashlib import sha256
from typing import Any, Optional

import pandas as pd
from tqdm.auto import tqdm

from citibike_sampler.config import *
from citibike_sampler.downloader import glob_csv_paths, download
from citibike_sampler.loader import _load_csv_shard
from citibike_sampler.misc import normalise_time_range

logger = logging.getLogger(__name__)


@dataclass
class SampingResult:
    """
    Represents the outcome of a  sampling operation at the trip-data shard level.

    Attributes
    ----------
    success : bool
        Indicates whether the operation completed successfully.
    num_orig: int
        Number of records in the original trip-data shards. None if not applicable.
    df : Any or None
        Dataframe with sampled records. None if not applicable.
    error_msg : str or None
        The message of an exception raised during the operation. None if not applicable.
    """

    success: bool
    num_orig: Optional[int] = None
    df: Optional[Any] = None
    error_msg: Optional[str] = None


class ProcessingError(Exception):
    """Raised when one or more trip-data shards fail to process properly."""

    pass


def sample(
        start,
        end=None,
        fraction=0.01,
        seed=None,
        max_workers=None,
        verbose=True

):
    """
    Randomly sample a fraction of all Citi Bike trip records for a
    specified time period and return the result as one DataFrame.

    Triggers automatic downloads of Citi Bike data for months/years that
    have not yet been locally cached.

    Parameters
    ----------
    start : str, int or tuple[int, int]
        The start year or month of the sampling period (inclusive).
        Accepted formats:
        - Tuple: (2020, 1)
        - Integer: 2020 → (2020, 1)
        - String: "2020" → (2020, 1)
        - String: "2020-5" → (2020, 5)
        - String: "2020-05" → (2020, 5)
    end : str, int or tuple[int, int], optional
        The end year or month of the sampling period (inclusive).
        Accepted formats:
        - Tuple: (2020, 12)
        - Integer: 2020 → (2020, 12)
        - String: "2020" → (2020, 1)
        - String: "2020-5" → (2020, 5)
        - String: "2020-05" → (2020, 5)
        If `None` is provided, data will be sampled only for the single year
        or single month specified in `start`.
    fraction : float, optional
        Fraction of records to retain (between 0 and 1). Default is 0.01 or 1 percent.
    seed : int, optional
        Random seed for reproducibility. Default is None.
    max_workers : int, optional
        Max number of threads to use for parallel data ingestion and sampling.
        Default is None, in which case the concurrency level is set to whatever
        `citibike_sampler.config.get_max_concurrency()` returns.
    verbose : bool, default=True
        If False, all progress bars and print outputs are silenced.

    Returns
    -------
    pandas.DataFrame
        A concatenated DataFrame with trip records that were randomly sampled
        from all data shards during the requested time range.
    """
    if not (0 < fraction < 1):
        raise ValueError("Sampling fraction must be between 0 and 1")

    start, end = normalise_time_range(start, end)
    max_workers = get_max_concurrency() if max_workers is None else max_workers

    if seed is not None:
        random.seed(seed)

    # trigger Citi Bike data downloads from S3 when needed
    download(start, end, skip_if_exists=True, warn=False)  # will also validate the cache

    # sample in parallel from trip-data shards
    sampled_frames = []
    errors = []
    num_all_records = 0
    csv_paths = glob_csv_paths(start, end)

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(_process_csv, p, i, fraction, seed)
            for i, p in enumerate(csv_paths)
        ]
        pbar = tqdm(
            as_completed(futures),
            total=len(futures),
            desc="Sampling from trip-data shards",
            disable=not verbose
        )
        for future in pbar:
            result = future.result()
            if result.success:
                num_all_records += result.num_orig
                if result.df is not None:
                    sampled_frames.append(result.df)
            else:
                errors.append(result.error_msg)

    # issue warning and return empty frame if no job returned any samples
    if not sampled_frames:
        logger.warning(
            "All sampled dataframes were empty. Unless your sampling "
            "fraction was very low, the cached data may be corrupted."
        )
        return pd.DataFrame()

    # raise an informative error if any job failed
    # > will include the full error message and path to
    #   the offending CSV file for each failed job
    if errors := [e for e in errors if e is not None]:
        formatted = '\n'.join(f"- {e} [" for e in errors)
        raise ProcessingError(
            f"{len(errors)} Sampling operation(s) failed. Details:\n{formatted}"
        )

    # clean up
    concat_df = pd.concat(sampled_frames, ignore_index=True)
    concat_df.sort_values("started_at", inplace=True)

    if verbose:
        frac = len(concat_df) / num_all_records * 100
        print(f"Sampled records: {len(concat_df):,}")
        print(f"Empirical sampling fraction: {frac:.2f}%")

    return concat_df


def _process_csv(csv_path, job_id, sampling_fraction, master_seed):
    try:
        # ingest trip data
        df = _load_csv_shard(csv_path)

        num_orig = len(df)
        if df.empty:
            return SampingResult(True, num_orig=0, df=None)

        # sample records at random
        seed = _job_seed(master_seed, job_id)
        sampled_df = df.sample(
            frac=sampling_fraction,
            random_state=seed,
            replace=False
        )

        sampled_df.sort_values("started_at", inplace=True)
        if sampled_df.empty:
            sampled_df = None
        return SampingResult(True, num_orig=num_orig, df=sampled_df)

    except Exception as e:
        msg = (
            f"{type(e).__name__}: {e}\n[Error occurred "
            f"during processing of file at {csv_path}]"
        )
        return SampingResult(False, num_orig=None, df=None, error_msg=msg)


def _job_seed(master_seed, idx):
    cryptic_str = f"xx_{idx}_yy_{master_seed}_zz_{idx}"
    digest = sha256(cryptic_str.encode()).hexdigest()
    int_64 = int(digest[:16], 16)  # max. 20 digits
    int_trunc = int(str(int_64)[:9])  # pandas has upper limit on seeds
    return int_trunc

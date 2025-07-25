"""
download.py

This module provides logic to download, unpack, and cache Citi Bike data
from AWS, where archives of trip records are hosted in a public S3 bucket.
The module supports legacy annual data archives for years before 2024 and
modern monthly archives (2024+).

Extracted trip-data is stored in a local cache, with `.manifest.json` files
used to track completeness and avoid redundant downloads.
"""

import json
import shutil
from zipfile import ZipFile

import requests
from tqdm.auto import tqdm, trange

from citibike_sampler.config import *

BASE_URL = "https://s3.amazonaws.com/tripdata"


class DownloadError(Exception):
    """Raised when one or more files fail to download."""

    pass


class ExtractionError(Exception):
    """Raised when one or more files fail to extract from an archive."""

    pass


def fetch_data(
        year,
        month=None,
        skip_if_exists=True,
        remove_archives=True,
        verbose=True
):
    """
    Download and extract archived trip-data from NYC's S3 bucket for a
    single year or month.

    Depending on the year, this function fetches either a single monthly
    archive (=modern data) or an annual bundle (=legacy data). Extracted CSV
    files holding individual trip-data shards are stored in a local cache,
    along with a manifest file for subsequent cache validation.

    Parameters
    ----------
    year : int
        The year for which to fetch data.
    month : int, optional, default: None
        The month-of-year for which to fetch data. Must be None for legacy data (pre-2024).
    skip_if_exists : bool, default=True
        If True, skip downloading and extraction if cached data already exists and is complete.
    remove_archives : bool, default=True
        Whether to delete downloaded `.zip` files and temporary directories after extraction.
        Setting this to False may help during debugging,
    verbose : bool, default=True
        If False, all progress bars are silenced.

    Raises
    ------
    RuntimeError
        If a download or extraction fails, or the time arguments are invalid.
    """
    _validate_download_time(year, month)

    try:
        if year <= LAST_BUNDLED_YEAR:
            assert month is None
            _fetch_legacy_year(year, skip_if_exists, verbose)
        else:
            if year == NOW_YEAR and month is None:
                assert month is not None

            months = range(1, 13) if month is None else [month]
            for month in months:
                _fetch_new_month(year, month, skip_if_exists, verbose)

    except Exception as e:
        if remove_archives:
            _clean_cache_dir()
        raise RuntimeError(f"An error occurred: {e}") from e

    if remove_archives:
        _clean_cache_dir()


def year_bundle_cache_dir(year):
    """
    Return the local path to the directory holding one year's worth
    of cached trip-data records.

    Parameters
    ----------
    year : int
        The year for which the cache directory is requested.

    Returns
    -------
    pathlib.Path
        Path to the year-level cache directory.
    """
    return get_cache_dir() / _year_bundle_name(year)


def month_bundle_cache_dir(year, month):
    """
    Return the local path to the directory holding one month's worth
    of cached trip-data records.

    Parameters
    ----------
    year : int
        The year of the data.
    month : int
        The month-of-year of the data (1–12).

    Returns
    -------
    pathlib.Path
        Path to the month-level cache directory.
    """
    return year_bundle_cache_dir(year) / f"{year}{month:02d}"


def glob_monthly_csv_paths(year, month, to_str=False):
    """
    Return a list of all cached trip-data shards for a given month.

    Parameters
    ----------
    year : int
        The year of the data.
    month : int
        The month-of-year of the data (1–12).
    to_str : bool, default=False
        If True, return file paths as strings; otherwise return Path objects.

    Returns
    -------
    list
        List of CSV file paths (or strings).
    """
    dir_ = month_bundle_cache_dir(year, month)
    if not dir_.is_dir():
        return []

    csv_prefix = _month_bundle_name(year, month)
    csv_paths = sorted(dir_.glob(f'{csv_prefix}_*.csv'))

    if to_str:
        # make JSON-serialisable
        csv_paths = [str(x) for x in csv_paths]

    return csv_paths


def is_month_fully_cached(year, month):
    """
    Return True if cached Citi Bike data is available for a given month.
    Return False otherwise.

    Parameters
    ----------
     year : int
        The year of the data.
    month : int
        The month-of-year of the data (1–12).

    Returns
    -------
    bool
    """
    try:
        manifest_data = _read_month_manifest(year, month)
    except FileNotFoundError:
        # without manifest, we cannot tell
        return False
    else:
        expected = manifest_data["csv_files"]
        actual = glob_monthly_csv_paths(year, month, to_str=True)
        missing = set(expected) - set(actual)

        if missing:
            return False

    return True


def purge_cache(dry_run=True):
    """
    Delete all trip-data shards in the local cache directory, incl. that
    directory itself.

    Parameters
    ----------
    dry_run : bool, optional
        If True (default), do not delete anything and simply report how
        many trip-data shards would be deleted without the `dry_run` flag.

    Returns
    -------
    dict
        Summary of how many trip-data shards would be or were deleted, incl.
        their total size (bytes) .
    """
    cache_dir = get_cache_dir()

    csv_paths = cache_dir.glob('**/*.csv')
    total_size = num_matched = num_deleted = 0

    for path in csv_paths:
        num_matched += 1
        total_size += path.stat().st_size
        if not dry_run:
            path.unlink()
            num_deleted += 1

    if not dry_run:
        shutil.rmtree(cache_dir)

    return {
        "dry_run": dry_run,
        "matched_csv_files": num_matched,
        "deleted_csv_files": num_deleted,
        "total_size_mb": round(total_size / 1e6, 2),
    }


def _fetch_legacy_year(year, skip_if_exists=True, verbose=True):
    """
    Fetch and extract trip-data for one legacy year (pre-2024).

    Parameters
    ----------
    year : int
        The year to fetch (must be <= LAST_BUNDLED_YEAR).
    skip_if_exists : bool, default=True
        If True, skip the download and extraction if all months for this year
        are already fully cached.
    verbose : bool, default=True
        If False, all progress bars are silenced.
    """
    assert FIRST_SUPPORTED_YEAR <= year <= LAST_BUNDLED_YEAR

    if skip_if_exists:
        if all(is_month_fully_cached(year, m) for m in range(1, 13)):
            return  # nothing to do

    _download_archive(year)
    _unpack_legacy_year_bundle(year)

    pbar = trange(
        1, 13,
        leave=False,
        desc=f"Extracting trip CSVs for {year}",
        disable=not verbose,
    )
    for month in pbar:
        _extract_monthly_csv_files(year, month)


def _fetch_new_month(year, month, skip_if_exists=True, verbose=True):
    """
    Fetch and extract modern trip-data for one month (2024+).

    Parameters
    ----------
    year : int
        The year to fetch (must be > LAST_BUNDLED_YEAR).
    month : int
        The month-of-year to fetch (1–12).
    skip_if_exists : bool, default=True
        If True, skip the download and extraction if the month is already fully cached.
    verbose : bool, default=True
        If False, all progress bars are silenced.
    """
    assert year > LAST_BUNDLED_YEAR

    # check local cache for this month
    if skip_if_exists and is_month_fully_cached(year, month):
        return  # nothing to do

    _download_archive(year, month, verbose)
    _extract_monthly_csv_files(year, month)


def _download_archive(year, month=None, verbose=True, **get_kwargs):
    """
    Download a single Citi Bike archive (either annual or monthly)
    from NYC's public S3 bucket.

    Parameters
    ----------
    year : int
        The year corresponding to the archive.
    month : int, optional, default: None
        The month-of-year corresponding to the archive. If None, a legacy annual
        archive is downloaded.
    verbose : bool, default=True
        If False, all progress bars are silenced.
    **get_kwargs : dict
        Additional keyword arguments passed to `requests.get`.

    Raises
    ------
    DownloadError
        If the download fails or the HTTP request is invalid.
    """
    if month is None:
        url = _build_s3_url(year)
    else:
        url = _build_s3_url(year, month)

    remote_fname = url.split("/")[-1]
    cache_dir = get_cache_dir()
    final_zip_path = cache_dir / remote_fname
    tmp_zip_path = final_zip_path.with_suffix(final_zip_path.suffix + ".download")

    try:
        # download archive to the temporary path
        with requests.get(url, stream=True, **get_kwargs) as r:
            r.raise_for_status()
            len_by_header = r.headers.get("Content-Length")
            total_len = int(len_by_header) if len_by_header is not None else None

            cache_dir.mkdir(parents=True, exist_ok=True)
            pbar = tqdm.wrapattr(
                r.raw,
                "read",
                total=total_len,
                desc=f"Downloading {url}",
                leave=False,
                disable=not verbose,
            )
            with pbar as raw:
                with open(tmp_zip_path, "wb") as out:
                    shutil.copyfileobj(raw, out)  # noqa
    except Exception as e:
        if tmp_zip_path.exists():
            tmp_zip_path.unlink()
        raise DownloadError(
            f"Download for {url} failed. Details:\n{e}"
        )
    else:
        # rename the downloaded file to its final name
        tmp_zip_path.rename(final_zip_path)


def _unpack_legacy_year_bundle(year, verbose=True):
    """
    Unpack monthly ZIP files from a legacy annual archive.

    Parameters
    ----------
    year : int
        The year corresponding to the annual archive.
    verbose : bool, default=True
        If False, all progress bars are silenced.

    Raises
    ------
    ExtractionError
        If the annual ZIP file is corrupted or missing expected contents.
    """
    cache_dir = get_cache_dir()

    tmp_dir = cache_dir / 'tmp'
    tmp_dir.mkdir(exist_ok=True)

    y_bundle_name = _year_bundle_name(year)
    y_bundle_zip_path = cache_dir / f"{y_bundle_name}.zip"

    tmp_bundle_dir = tmp_dir / y_bundle_name
    final_bundle_dir = cache_dir / y_bundle_name

    try:
        with ZipFile(y_bundle_zip_path) as zipf:
            # check that 12 monthly zip files exist inside the annual archive
            expected_members = [
                f'{y_bundle_name}/{_month_bundle_name(year, month)}.zip'
                for month in range(1, 13)
            ]
            if missing := set(expected_members) - set(zipf.namelist()):
                raise RuntimeError(
                    f"Expected annual data bundle to contain 12 monthly "
                    f"zip-files with path-naming convention "
                    f"<yyyy-citibike-tripdata/yyyymm-citibike-tripdata.zip>."
                    f"\n Missing members: {missing}."
                )

            # Extract all monthly zip files to the temporary directory.
            # Note that relative paths are retained!
            pbar = tqdm(
                expected_members,
                desc=f'Unpacking annual data bundle for {year}',
                leave=False,
                disable=not verbose,
            )
            for member in pbar:
                zipf.extract(member, path=tmp_dir)
    except Exception as e:
        shutil.rmtree(tmp_dir, ignore_errors=True)

        raise ExtractionError(
            f"Unpacking of annual data bundle '{y_bundle_zip_path}' failed. "
            f"Details:\n{e}"
        )
    else:
        # make sure we delete any corrupted, dangling cache
        _remove_cached_year_bundle(year)

        # use an atomic rename to move the unzipped annual directory
        # to the final location.
        tmp_bundle_dir.rename(final_bundle_dir)
        tmp_dir.rmdir()


def _extract_monthly_csv_files(year, month):
    """
    Extract all trip-data shards from a monthly ZIP archive into the cache.

    All extracted shards (=CSV files) are stored under a month-specific
    directory. A manifest file listing the extracted shards is also created.

    Parameters
    ----------
    year : int
        The year of the data.
    month : int
        The month-of-year of the data (1–12).

    Raises
    ------
    ExtractionError
        If the monthly ZIP file is corrupted or CSV extraction fails.
    """
    cache_dir = get_cache_dir()

    y_bundle_dir = cache_dir / _year_bundle_name(year)
    month_zip_name = _month_bundle_name(year, month) + '.zip'
    month_zip_dir = y_bundle_dir if year <= LAST_BUNDLED_YEAR else cache_dir
    month_zip_path = month_zip_dir / month_zip_name

    final_month_dir = y_bundle_dir / f"{year}{month:02d}"
    tmp_dir = final_month_dir / 'tmp'
    tmp_dir.mkdir(exist_ok=True, parents=True)

    try:
        # extract CSV files from the unzipped annual bundle
        with ZipFile(month_zip_path) as zipf:
            csv_members = [x for x in zipf.namelist() if x.endswith(".csv")]
            for member in csv_members:
                zipf.extract(member, path=tmp_dir)
    except Exception as e:
        shutil.rmtree(tmp_dir, ignore_errors=True)

        raise ExtractionError(
            f"Extraction of trip-data CSVs for '{year}-{month:02d} failed. "
            f"Details:\n{e}"
        )
    else:
        # move all extracted CSV files to the final location
        for member in csv_members:
            csv_name = member.split('/')[-1]
            tmp_path = tmp_dir / member
            tmp_path.rename(final_month_dir / csv_name)
        shutil.rmtree(tmp_dir)  # still contains an empty sub-dir

        _write_month_manifest(year, month)


def _write_month_manifest(year, month):
    """
    Create a JSON file listing all trip-data shards for a given month.

    When executing this function after a fresh data download, it produces
    a manifest file that can later be used for cache validation.

    Parameters
    ----------
    year : int
        The year of the data.
    month : int
        The month-of-year of the data (1–12).
    """

    manifest_path = _manifest_path(year, month)
    csv_paths = glob_monthly_csv_paths(year, month, to_str=True)

    manifest_data = {
        "csv_files": csv_paths,
    }

    with open(manifest_path, "w") as f:
        json.dump(manifest_data, f, indent=2)


def _year_bundle_name(year):
    return f'{year}-citibike-tripdata'


def _month_bundle_name(year, month):
    ym = f"{year}{month:02d}"
    return f'{ym}-citibike-tripdata'


def _read_month_manifest(year, month):
    manifest_path = _manifest_path(year, month)

    with open(manifest_path, "r") as f:
        manifest_data = json.load(f)
    return manifest_data


def _manifest_path(year, month):
    dir_ = month_bundle_cache_dir(year, month)
    return dir_ / ".manifest.json"


def _is_year_fully_cached(year):
    for month in range(1, 13):
        if not is_month_fully_cached(year, month):
            return False
    return True


def _build_s3_url(year, month=None):
    if month is None:
        fname = _year_bundle_name(year)
    else:
        fname = _month_bundle_name(year, month)

    return f"{BASE_URL}/{fname}.zip"


def _validate_download_time(year, month):
    # check year in range
    if year < FIRST_SUPPORTED_YEAR:
        raise RuntimeError(
            f"Citi Bike data is only available from {FIRST_SUPPORTED_YEAR} "
            f"onwards. You requested year {year}."
        )
    elif year > NOW_YEAR:
        raise RuntimeError(f"Requested year {year} is in the future.")

    # check month in range
    if month is not None:
        msg = "`month` must be an integer between 1 and 12."
        if not isinstance(month, int):
            raise RuntimeError(msg)
        if month < 1 or month > 12:
            raise RuntimeError(msg)

        if year == NOW_YEAR:
            if month >= NOW_MONTH:
                raise RuntimeError(
                    f"Can only download data for months in the past. "
                    f"You requested {year}-{month}."
                )

    # check year-month combination
    if year <= LAST_BUNDLED_YEAR:
        if month is not None:
            raise RuntimeError(
                f"Isolated monthly downloads are only possible from "
                f"{LAST_BUNDLED_YEAR + 1} onwards. Before that, you "
                f"can only request an entire year at once (legacy "
                f"data format)."
            )
    else:
        if year == NOW_YEAR and month is None:
            raise RuntimeError(
                f"Please specify a month when downloading data "
                f"for the current year."
            )


def _remove_cached_year_bundle(year):
    dir_ = year_bundle_cache_dir(year)
    if dir_.is_dir():
        shutil.rmtree(dir_)


def _clean_cache_dir():
    cache_dir = get_cache_dir()

    if cache_dir.is_dir():
        # delete files that are neither CSVs nor manifests
        for file_path in cache_dir.rglob('*'):
            if file_path.is_file():
                fname = file_path.name
                if not fname.endswith(".csv") and not fname.endswith(".manifest.json"):
                    file_path.unlink()

        # remove empty subdirectories (bottom-up)
        for dir_path in sorted(cache_dir.rglob('*'), reverse=True):
            if dir_path.is_dir():
                try:
                    dir_path.rmdir()
                except OSError:
                    pass  # directory not empty

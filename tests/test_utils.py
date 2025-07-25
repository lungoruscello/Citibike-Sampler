"""
Testing utilities, including logic for the generation of mock Citi Bike data.
"""

import csv
import json
import os
import random
import shutil
import string
from contextlib import contextmanager
from datetime import datetime, timedelta
from hashlib import sha256
from zipfile import ZipFile

from citibike_sampler.global_paths import ASSET_DIR, DEFAULT_CACHE_DIR

FIRST_MOCK_YEAR = 2020
LAST_MOCK_YEAR = 2024
LAST_BUNDLED_YEAR = 2023  # full year bundles up to this year
ARCHIVE_TEST_DATA_DIR = ASSET_DIR / 'test_data' / 'compressed'
CACHE_TEST_DATA_DIR = ASSET_DIR / 'test_data' / 'unpacked'


def prepare_mock_data():
    """
    Prepare the mock data needed for both file extraction tests and
    data-processing tests.

    Mock data is stored for re-use at `<ASSET_DIR/test_data>`

    Note:
        Many unit tests will fail unless you have at least once executed
        this function before.
    """
    generate_mock_data(mock_source=True)
    generate_mock_data(mock_source=False)


@contextmanager
def switched_cache_dir(sub_dir):
    """
    Temporarily switch the CITIBIKE_CACHE_DIR to a named subdirectory within the
    default cache location.

    Upon exit, the original cache directory is restored and the temporary one deleted.

    Parameters
    ----------
    sub_dir : str or Path
        Name of the temporary subdirectory within the default cache location
        to switch the cache to.
    """
    test_cache_dir = DEFAULT_CACHE_DIR / sub_dir
    os.environ["CITIBIKE_CACHE_DIR"] = str(test_cache_dir)

    try:
        yield
    finally:
        os.environ["CITIBIKE_CACHE_DIR"] = str(DEFAULT_CACHE_DIR)

        if test_cache_dir.is_dir():
            shutil.rmtree(test_cache_dir)  # tear down


def mock_download(year, month=None, verbose=None):
    """
    Mock the download of a Citi Bike data archive. This is done by simply
    copying over previously generated mock data from `ARCHIVE_TEST_DATA_DIR`
    to the currently set cache directory.

    Parameters
    ----------
    year : int
        Year of the data archive.
    month : int, optional
        Month-of-year of the data archive. If omitted, assumes legacy full-year bundle.
    verbose : bool, optional
        Not used. Needed only to match signature of mocked function.

    Raises
    ------
    AssertionError
        If the expected mock archive zip file does not exist.
    """
    from citibike_sampler.config import get_cache_dir
    from citibike_sampler.download import _year_bundle_name, _month_bundle_name  # noqa

    cache_dir = get_cache_dir()
    cache_dir.mkdir(parents=True, exist_ok=True)

    if month is None:
        name = _year_bundle_name(year)
    else:
        name = _month_bundle_name(year, month)

    fname = f'{name}.zip'

    test_src_fpath = ARCHIVE_TEST_DATA_DIR / fname
    mock_download_fpath = cache_dir / fname

    assert test_src_fpath.is_file(), f'Test file does not exist {test_src_fpath}'
    shutil.copy(test_src_fpath, mock_download_fpath)


def generate_mock_data(mock_source=True, seed=5432):
    """
    Generate mock Citi Bike data, either as compressed source archives (for
    mock download), or as a fully extracted local cache layout (for sampling
    tests).

    Parameters
    ----------
    mock_source : bool
        If True, create zipped archive files (mocking downloaded source).
        If False, create unpacked cache-style data with .manifest.json files.
    seed : int
        Random seed for reproducibility.
    """
    random.seed(seed)

    for year in range(FIRST_MOCK_YEAR, LAST_MOCK_YEAR + 1):
        for month in range(1, 13):
            if mock_source:
                if year <= LAST_BUNDLED_YEAR:
                    _write_legacy_bundle(year)
                    break  # only one bundle per legacy year needed
                else:
                    _write_month_archive(year, month)
            else:
                _write_extracted_month(year, month)


def _write_legacy_bundle(year):
    yname = f"{year}-citibike-tripdata"
    zip_path = ARCHIVE_TEST_DATA_DIR / f"{yname}.zip"
    tmp_dir = ARCHIVE_TEST_DATA_DIR / f"tmp_{yname}"
    bundle_dir = tmp_dir / yname
    bundle_dir.mkdir(parents=True, exist_ok=True)

    for month in range(1, 13):
        ym = f"{year}{month:02d}"
        month_zip_path = bundle_dir / f"{ym}-citibike-tripdata.zip"
        tmp_month = tmp_dir / f"tmp_{ym}"
        tmp_month.mkdir(exist_ok=True)

        csv_paths = _generate_monthly_shards(year, month, tmp_month)
        with ZipFile(month_zip_path, "w") as zipf:
            for path in csv_paths:
                zipf.write(path, arcname=path.name)

        shutil.rmtree(tmp_month)

    with ZipFile(zip_path, "w") as zipf:
        for inner_zip in bundle_dir.glob("*.zip"):
            zipf.write(inner_zip, arcname=f"{yname}/{inner_zip.name}")

    shutil.rmtree(tmp_dir)


def _write_month_archive(year, month):
    ym = f"{year}{month:02d}"
    zip_path = ARCHIVE_TEST_DATA_DIR / f"{ym}-citibike-tripdata.zip"
    tmp_dir = ARCHIVE_TEST_DATA_DIR / f"tmp_{ym}"
    tmp_dir.mkdir(exist_ok=True)

    csv_paths = _generate_monthly_shards(year, month, tmp_dir)

    with ZipFile(zip_path, "w") as zipf:
        for path in csv_paths:
            zipf.write(path, arcname=path.name)

    shutil.rmtree(tmp_dir)


def _write_extracted_month(year, month):
    out_dir = CACHE_TEST_DATA_DIR / f'{year}-citibike-tripdata' / f"{year}{month:02d}"
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_paths = _generate_monthly_shards(year, month, out_dir)
    _write_manifest(out_dir, year, month, csv_paths)


def _generate_monthly_shards(year, month, dest_dir):
    ym = f"{year}{month:02d}"
    num_shards = random.randint(1, 3)
    csv_paths = []

    for idx in range(1, num_shards + 1):
        fname = f"{ym}-citibike-tripdata_{idx}.csv"
        fpath = dest_dir / fname

        with open(fpath, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["ride_id", "started_at", "ended_at"])
            for _ in range(random.randint(5, 10)):
                ride_id = _random_ride_hash()
                started_at, ended_at = _random_ride_times(year, month)
                writer.writerow([ride_id, started_at, ended_at])

        csv_paths.append(fpath)

    return csv_paths


def _random_ride_hash():
    random_input = random.choices(string.ascii_letters + string.digits, k=20)
    raw = ''.join(random_input)
    return sha256(raw.encode()).hexdigest()


def _random_ride_times(year, month):
    min_duration, max_duration = 5 * 60, 60 * 60

    # find the month's duration in seconds
    month_start = datetime(year, month, 1)
    if month == 12:
        next_month_start = datetime(year + 1, 1, 1)
    else:
        next_month_start = datetime(year, month + 1, 1)
    delta_seconds = int((next_month_start - month_start).total_seconds())

    # sample a start time
    start_offset = random.randint(0, delta_seconds - 1)
    started = month_start + timedelta(seconds=start_offset)

    # sample a ride duration
    duration = random.randint(min_duration, max_duration)
    ended = started + timedelta(seconds=duration)

    assert month_start <= started <= ended
    return started.isoformat(), ended.isoformat()


def _write_manifest(cache_dir, year, month, csv_paths):
    manifest = {
        "year": year,
        "month": month,
        "csv_files": [str(p.resolve()) for p in csv_paths],
    }
    with open(cache_dir / ".manifest.json", "w") as f:
        json.dump(manifest, f, indent=2)

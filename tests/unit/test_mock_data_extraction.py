"""
Tests for data extraction logic (both annual and monthly) with mocked data
downloads.

Specifically, downloads of Citi Bike archives are mocked using synthetic data
that mimics the structure of real Citi Bike archives, but with a tiny
disk footprint. (See `test_utils.generate_mock_tripdata_archives` for details).
"""

from pathlib import Path
from unittest import mock

from tests.test_utils import switched_cache_dir, mock_download

test_year1 = 2020  # it's synthetic data, so the exact choice does not matter
test_year2 = 2024
test_month2 = 1


@mock.patch('citibike_sampler.download._download_archive', side_effect=mock_download)
def test_fresh_legacy_data_extraction(mocked_download):
    from citibike_sampler.download import fetch_data, _is_year_fully_cached

    with switched_cache_dir('extraction_test'):
        fetch_data(test_year1)
        assert _is_year_fully_cached(test_year1)


@mock.patch('citibike_sampler.download._download_archive', side_effect=mock_download)
def test_fresh_monthly_data_extraction(mocked_download):
    from citibike_sampler.download import fetch_data, is_month_fully_cached

    with switched_cache_dir('extraction_test'):
        fetch_data(test_year2, test_month2)
        assert is_month_fully_cached(test_year2, test_month2)


@mock.patch('citibike_sampler.download._download_archive', side_effect=mock_download)
def test_corrupted_cache_for_legacy_year(mocked_download):
    from citibike_sampler.download import fetch_data, _is_year_fully_cached

    with switched_cache_dir('extraction_test'):
        fetch_data(test_year1)

        # corrupt the cache by deleting one trip-data shard
        _delete_first_monthly_shard(test_year1, 1)

        # trigger the data extraction again
        fetch_data(test_year1)
        assert _is_year_fully_cached(test_year1)


@mock.patch('citibike_sampler.download._download_archive', side_effect=mock_download)
def test_corrupted_cache_for_new_month(mocked_download):
    from citibike_sampler.download import fetch_data
    from citibike_sampler.download import month_bundle_cache_dir, is_month_fully_cached

    with switched_cache_dir('extraction_test'):
        fetch_data(test_year2, test_month2)

        # corrupt the cache by deleting one trip-data shard
        _delete_first_monthly_shard(test_year2, test_month2)

        # trigger the data download yet again
        fetch_data(test_year2, test_month2)

        expected_dir = month_bundle_cache_dir(test_year2, test_month2)
        assert expected_dir.is_dir()
        assert is_month_fully_cached(test_year2, test_month2)


@mock.patch('citibike_sampler.download._download_archive', side_effect=mock_download)
def test_forced_dual_extraction_for_legacy_year(mocked_download):
    from citibike_sampler.download import fetch_data, _is_year_fully_cached

    with switched_cache_dir('extraction_test'):
        fetch_data(test_year1)
        fetch_data(test_year1, skip_if_exists=False)
        assert _is_year_fully_cached(test_year1)


@mock.patch('citibike_sampler.download._download_archive', side_effect=mock_download)
def test_forced_dual_extraction_for_new_month(mocked_download):
    from citibike_sampler.download import fetch_data, is_month_fully_cached

    with switched_cache_dir('extraction_test'):
        fetch_data(test_year2, test_month2)
        fetch_data(test_year2, test_month2, skip_if_exists=False)
        assert is_month_fully_cached(test_year2, test_month2)


def _delete_first_monthly_shard(year, month):
    from citibike_sampler.download import _read_month_manifest  # noqa

    manifest = _read_month_manifest(year, month)
    csv_paths = [Path(x) for x in manifest["csv_files"]]

    assert len(csv_paths) >= 1
    csv_paths[0].unlink()

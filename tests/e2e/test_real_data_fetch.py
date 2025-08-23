"""
End-to-end tests for real Citi Bike data download and cache validation.
"""

import pytest

from tests.test_utils import switched_cache_dir


@pytest.mark.e2e
def test_real_2022_download(monkeypatch):
    from citibike_sampler.downloader import download, _is_year_fully_cached

    with switched_cache_dir('e2e_test'):
        download(2022)
        assert _is_year_fully_cached(2022)


@pytest.mark.e2e
def test_real_2024_download(monkeypatch):
    from citibike_sampler.downloader import download, _is_year_fully_cached

    with switched_cache_dir('e2e_test'):
        download('2024-1')
        download('2024-2', '2024-12')
        assert _is_year_fully_cached(2024)

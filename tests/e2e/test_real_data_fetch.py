"""
End-to-end tests for full Citi Bike data fetch and cache validation.
"""

import pytest

from tests.test_utils import switched_cache_dir


@pytest.mark.e2e
def test_fetch_2022_data(monkeypatch):
    from citibike_sampler.download import fetch_data, _is_year_fully_cached

    with switched_cache_dir('e2e_test'):
        fetch_data(2022)
        assert _is_year_fully_cached(2022)


@pytest.mark.e2e
def test_fetch_2024_data(monkeypatch):
    from citibike_sampler.download import fetch_data, _is_year_fully_cached

    with switched_cache_dir('e2e_test'):
        fetch_data(2024)
        assert _is_year_fully_cached(2024)

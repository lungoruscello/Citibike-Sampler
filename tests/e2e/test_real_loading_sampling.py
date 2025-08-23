"""
End-to-end tests for real Citi Bike data download, ingestion, and sampling.
"""

import pytest

from tests.test_utils import switched_cache_dir
import math


@pytest.mark.e2e
def test_real_loading_sampling(monkeypatch):
    from citibike_sampler.loader import load_all
    from citibike_sampler.sampler import sample

    with switched_cache_dir('e2e_test'):
        sample_df = sample(start='2025-5', end='2025-6', fraction=0.01, seed=42)
        full_df = load_all(start='2025-5', end='2025-6')

        assert set(sample_df.ride_id).issubset(set(full_df.ride_id))
        assert math.isclose(len(sample_df) / len(full_df), 0.01, abs_tol=0.0001)

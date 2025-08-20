"""
Tests the data sampling logic, based on mock Citi Bike data.
(See `test_utils.generate_mock_data` for details)
"""

from unittest import mock
import logging
import numpy as np
import pandas as pd

from tests.test_utils import switched_cache_dir, mock_download


def test_derived_seeds():
    from citibike_sampler.sampling import _job_seed

    assert _job_seed(1, 0) != _job_seed(0, 1) != _job_seed(1, 1)
    assert _job_seed(23, 5) == _job_seed(23, 5)


@mock.patch("citibike_sampler.download._download_archive", side_effect=mock_download)
def test_data_sampling(mocked_download):
    from citibike_sampler.sampling import sample

    with switched_cache_dir("sampling_test"):
        shared_args = dict(
            start=2020, end=2024, fraction=0.5, verbose=False
        )
        df1 = sample(seed=42, **shared_args)
        df2 = sample(seed=42, **shared_args)
        df3 = sample(seed=23, **shared_args)

    for df in [df1, df2, df3]:
        _assert_no_duplicates(df)

    assert _are_frames_identical(df1, df2)  # same seed
    assert not _are_frames_identical(df1, df3)  # different seeds


@mock.patch("citibike_sampler.download._download_archive", side_effect=mock_download)
def test_empty_samples_triggers_warning(mocked_download, caplog):
    from citibike_sampler.sampling import sample

    with switched_cache_dir("sampling_test"):
        with caplog.at_level(logging.WARNING):
            df = sample(2020, 2020, fraction=1e-20, verbose=False)
            assert df.empty

    # check if expected warning is in the log
    assert "All sampled dataframes were empty." in caplog.text
    assert len(caplog.records) == 1  # only one warning should have been logged
    assert caplog.records[0].levelname == "WARNING"  # verify log level


def _assert_no_duplicates(df):
    assert isinstance(df, pd.DataFrame)
    assert df.ride_id.duplicated().sum() == 0


def _are_frames_identical(df_a, df_b):
    try:
        for var in ["ride_id", "started_at", "ended_at"]:
            assert np.all(df_a[var].values == df_b[var].values)
            # TODO: Figure out why `df1.equals(df2)` fails on identical frames
    except AssertionError:
        return False
    else:
        return True

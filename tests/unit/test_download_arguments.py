import pytest

from citibike_sampler.config import *


def test_module_import():
    import citibike_sampler.download as download
    assert download is not None


def test_time_validation_works():
    from citibike_sampler.download import _validate_download_time

    # check valid cases
    _validate_download_time(FIRST_SUPPORTED_YEAR, month=None)
    _validate_download_time(NOW_YEAR - 1, month=None)
    _validate_download_time(NOW_YEAR - 1, month=NOW_MONTH - 1)

    # check invalid cases
    _test_time_validation_raises(FIRST_SUPPORTED_YEAR-1)  # too early
    _test_time_validation_raises(NOW_YEAR + 1)  # future
    _test_time_validation_raises(FIRST_SUPPORTED_YEAR, month=13)  # impossible month
    _test_time_validation_raises(NOW_YEAR, month=NOW_MONTH)  # current month (still ongoing)
    _test_time_validation_raises(NOW_YEAR, month=None)  # month mandatory for current year
    _test_time_validation_raises(LAST_BUNDLED_YEAR, month=1)  # month impossible for legacy years


def _test_time_validation_raises(year, month=None):
    from citibike_sampler.download import _validate_download_time  # noqa

    with pytest.raises(RuntimeError):
        _validate_download_time(year, month=month)

import pytest


def test_time_normalisation_works():
    from citibike_sampler.misc import normalise_monthly_time_range

    # check tuple pass-thru
    start = (2020, 1)
    end = (2024, 12)
    assert normalise_monthly_time_range(start, end) == (start, end)  # noqa

    (start_y, start_m), (end_y, end_m) = normalise_monthly_time_range("2020-6")
    assert start_y == end_y == 2020
    assert start_m == end_m == 6

    (start_y, start_m), (end_y, end_m) = normalise_monthly_time_range("2021", "2021")
    assert start_y == 2021
    assert start_m == 1
    assert end_y == 2021
    assert end_m == 12

    (start_y, start_m), (end_y, end_m) = normalise_monthly_time_range("2022-4", "2023-9")
    assert start_y == 2022
    assert start_m == 4
    assert end_y == 2023
    assert end_m == 9

    (start_y, start_m), (end_y, end_m) = normalise_monthly_time_range("2024-04", "2025-09")
    assert start_y == 2024
    assert start_m == 4
    assert end_y == 2025
    assert end_m == 9

    # start date after end date
    with pytest.raises(ValueError):
        normalise_monthly_time_range("2021", "2020")
    with pytest.raises(ValueError):
        normalise_monthly_time_range("2022-2", "2022-1")


def test_month_range_works():
    from citibike_sampler.misc import month_list

    assert month_list((2020, 1), (2020, 1)) == [(2020, 1)]
    assert month_list((2023, 12), (2024, 2)) == [(2023, 12), (2024, 1), (2024, 2)]

    with pytest.raises(ValueError):
        # start date after end date
        month_list((2020, 2), (2020, 1))


def test_download_range_validation_works():
    from citibike_sampler.download import _validate_download_range
    from citibike_sampler.config import FIRST_SUPPORTED_YEAR, NOW_YEAR, NOW_MONTH, LAST_BUNDLED_YEAR

    # check valid cases
    _validate_download_range((FIRST_SUPPORTED_YEAR, 1), (NOW_YEAR, NOW_MONTH -1))
    _validate_download_range((2024, 4), (2024, 4))

    # check invalid cases
    _test_time_validation_raises((2024, 1), (2023, 12))  # start after end
    _test_time_validation_raises((FIRST_SUPPORTED_YEAR -1, 1), (NOW_YEAR, NOW_MONTH -1))  # start too early
    _test_time_validation_raises((NOW_YEAR + 1, 1), (NOW_YEAR + 1, 1))  # start in future
    _test_time_validation_raises((FIRST_SUPPORTED_YEAR, 1), (NOW_YEAR + 1, 1))  # end in future
    _test_time_validation_raises((2020, 13), (2024, 4))  # impossible month
    _test_time_validation_raises((FIRST_SUPPORTED_YEAR, 1), (LAST_BUNDLED_YEAR, 2))  # month impossible for legacy years


def _test_time_validation_raises(year, month):
    from citibike_sampler.download import _validate_download_range  # noqa

    with pytest.raises(ValueError):
        _validate_download_range(year, month)

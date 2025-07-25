import pytest

def test_time_normalisation_works():
    from citibike_sampler.misc import normalise_time_range

    (start_y, start_m), (end_y, end_m) = normalise_time_range("2020", "2020")
    assert start_y == 2020
    assert start_m == 1
    assert end_y == 2020
    assert end_m == 12

    (start_y, start_m), (end_y, end_m) = normalise_time_range("2022-7", "2023-11")
    assert start_y == 2022
    assert start_m == 7
    assert end_y == 2023
    assert end_m == 11

    (start_y, start_m), (end_y, end_m) = normalise_time_range("2024-04", "2025-09")
    assert start_y == 2024
    assert start_m == 4
    assert end_y == 2025
    assert end_m == 9

    with pytest.raises(ValueError):
        normalise_time_range("2021", "2020")
        normalise_time_range("2022-2", "2022-1")

import pytest
import requests

from citibike_sampler.config import *


@pytest.mark.integration
@pytest.mark.parametrize("year", [FIRST_SUPPORTED_YEAR, LAST_BUNDLED_YEAR])
def test_legacy_annual_s3_archive_exists(year):
    from citibike_sampler.download import _build_s3_url

    url = _build_s3_url(year)
    response = requests.head(url)
    assert response.status_code == 200, f"Expected {url} to exist"


@pytest.mark.integration
@pytest.mark.parametrize("year,month", [[LAST_BUNDLED_YEAR+1, 1], [2025, 4]])
def test_new_monthly_s3_archive_exists(year, month):
    from citibike_sampler.download import _build_s3_url

    url = _build_s3_url(year, month)
    response = requests.head(url)
    assert response.status_code == 200, f"Expected {url} to exist"


@pytest.mark.integration
@pytest.mark.parametrize("year,month", [[LAST_BUNDLED_YEAR, 12]])
def test_new_monthly_s3_archive_missing(year, month):
    from citibike_sampler.download import _build_s3_url

    url = _build_s3_url(year, month)
    response = requests.head(url)
    assert response.status_code == 404, f"Expected {url} to NOT exist"

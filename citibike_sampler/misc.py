import re


def normalise_monthly_time_range(start, end=None):
    """
    Normalise flexible time range inputs (for either data download or sampling)
    into ((start_year, start_month), (end_year, end_month)) tuples.

    Parameters
    ----------
    start : str, int or tuple[int, int]
        The start of the desired time range. Accepted formats:
        - Tuple: (2020, 1)
        - Integer: 2020 → (2020, 1)
        - String: "2020" → (2020, 1)
        - String: "2020-5" → (2020, 5)
        - String: "2020-05" → (2020, 5)
    end : str, int or tuple[int, int], optional
        The end of the desired time range. Accepted formats:
        - Tuple: (2020, 12)
        - Integer: 2020 → (2020, 12)
        - String: "2020" → (2020, 1)
        - String: "2020-5" → (2020, 5)
        - String: "2020-05" → (2020, 5)
        If `None` is provided, end will be equal to start.

    Returns
    -------
    (start_year, start_month) : tuple[int, int]
        The year and month of the start.

    (end_year, end_month) : tuple[int, int]
        The year and month of the end.

    Raises
    ------
    ValueError
        If the input strings are not in a recognised format, contain invalid months,
        or if the start date is after the end date.

    TypeError
        If inputs are not strings or integers.
    """
    end = start if end is None else end

    def to_tuple(value, is_start):
        err_msg = f"Invalid date format: {value!r}"

        if isinstance(value, int):
            year = value
            month = 1 if is_start else 12
        elif isinstance(value, str):
            match = re.match(r"^(\d{4})(?:-(\d{1,2}))?$", value)
            if not match:
                raise ValueError(err_msg)
            year = int(match.group(1))
            month = int(match.group(2)) if match.group(2) else (1 if is_start else 12)
        else:
            raise ValueError(err_msg)

        if not (1 <= month <= 12):
            raise ValueError(f"Invalid month {month} in {value!r}")

        return year, month

    if not isinstance(start, tuple):
        start = to_tuple(start, is_start=True)

    if not isinstance(end, tuple):
        end = to_tuple(end, is_start=False)

    if start > end:
        raise ValueError(f"Start date {start} is after end date {end}")

    return start, end


def month_list(start, end):
    """
    Return a list of (year, month) tuples between start and end inclusive.
    """
    start_year, start_month = start
    end_year, end_month = end

    if (start_year, start_month) > (end_year, end_month):
        raise ValueError("Start date must not be after end date.")

    months = []
    year, month = start_year, start_month
    while (year, month) <= (end_year, end_month):
        months.append((year, month))
        month += 1
        if month > 12:
            month = 1
            year += 1
    return months

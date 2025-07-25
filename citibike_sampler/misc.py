import re


def normalise_time_range(start, end):
    """
    Normalise flexible time range inputs (for either data download or sampling)
    into ((start_year, start_month), (end_year, end_month)) tuples.

    Parameters
    ----------
    start : str or int
        The start of the desired time range. Accepted formats:
        - Integer: 2020 → (2020, 1)
        - String: "2020" → (2020, 1)
        - String: "2020-05" → (2020, 5)

    end : str or int
        The end of the desired time range. Accepted formats:
        - Integer: 2020 → (2020, 12)
        - String: "2020" → (2020, 12)
        - String: "2020-05" → (2020, 5)

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
    def to_tuple(value, is_start):
        if isinstance(value, int):
            year = value
            month = 1 if is_start else 12
        elif isinstance(value, str):
            match = re.match(r"^(\d{4})(?:-(\d{1,2}))?$", value)
            if not match:
                raise ValueError(f"Invalid date format: {value!r}")
            year = int(match.group(1))
            month = int(match.group(2)) if match.group(2) else (1 if is_start else 12)
        else:
            raise TypeError(
                f"Date must be an integer or string in 'YYYY' or "
                f"'YYYY-MM' format, got {type(value)}"
            )

        if not (1 <= month <= 12):
            raise ValueError(f"Invalid month {month} in {value!r}")

        return year, month

    start_tuple = to_tuple(start, is_start=True)
    end_tuple = to_tuple(end, is_start=False)

    if start_tuple > end_tuple:
        raise ValueError(f"Start date {start_tuple} is after end date {end_tuple}")

    return start_tuple, end_tuple

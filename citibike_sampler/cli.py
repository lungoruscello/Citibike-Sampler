"""
A command-line tool to download and sample trip data from NYC's Citi Bike network

Author: S. Langenbach (ETHZ)
Licence: MIT
"""

# TODO: Test and debug the CLI, especially the time handling and cache use

import argparse
import logging
import sys
from pathlib import Path

from citibike_sampler.download import fetch_data
from citibike_sampler.misc import normalise_time_range
from citibike_sampler.sampling import thinned_dataset

ROOT_DIR = Path(__file__).parent

logging.basicConfig(level=logging.INFO)


def main():
    parser = argparse.ArgumentParser(
        prog="cbike",
        description="Command-line tool for downloading and sampling Citi Bike trip data.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # --- Download command ---
    dl = subparsers.add_parser(
        "download",
        help="Download Citi Bike trip-data from one or more months or years.",
        description=(
            "Download and extract raw trip-data. Before 2024, trip-data can only be downloaded "
            "in annual chunks. Modern data (2024 onwards) can either be downloaded for an entire "
            "year or for a single month only. "
            "Accepted date formats: 2020, '2021', '2021-05', etc."
        )
    )
    dl.add_argument(
        "--start", type=str, required=True,
        help="Start of time range (e.g., '2020' or '2020-05')"
    )
    dl.add_argument(
        "--end", type=str, default=None,
        help="End of time range (e.g., '2023-12'). If omitted, same as start."
    )
    dl.add_argument(
        "--force", action="store_true",
        help="Force re-download even if files exist in cache."
    )
    dl.add_argument(
        "--quiet", action="store_true",
        help="Suppress progress output."
    )

    # --- Sample command ---
    sm = subparsers.add_parser(
        "sample",
        help="Randomly sample trip-data across months or years.",
        description=(
            "Extract a random sample from Citi Bike trip-data for the specified time range. "
            "Automatically triggers download if required data is missing. "
            "Accepted date formats: 2020, '2021', '2021-05', etc."
        )
    )
    sm.add_argument("--start", type=str, required=True, help="Start of time range (e.g., '2020-01')")
    sm.add_argument("--end", type=str, required=True, help="End of time range (e.g., '2023-12')")
    sm.add_argument("--fraction", type=float, default=0.01, help="Sampling fraction (0 < f < 1)")
    sm.add_argument("--output", type=str, required=True, help="Output CSV file path")
    sm.add_argument("--seed", type=int, default=None, help="Random seed for reproducibility")
    sm.add_argument("--quiet", action="store_true", help="Suppress progress output")

    args = parser.parse_args()
    start_arg = args.start
    end_arg = args.end
    end_arg = start_arg if end_arg is None else end_arg
    skip_if_exists = not args.force
    verbose = not args.quiet

    if args.command == "download":
        download(
            start_arg, end_arg,
            skip_if_exists,
            verbose
        )
    elif args.command == "sample":
        sample(
            start_arg, end_arg,
            args.fraction,
            args.seed,
            args.outout,
            verbose
        )
    else:
        parser.print_help()
        sys.exit(1)


def download(start_arg, end_arg, skip_if_exists, verbose):
    (start_y, start_m), (end_y, end_m) = normalise_time_range(start_arg, end_arg)

    # legacy data only supports full years
    if start_y <= 2023 or end_y <= 2023:
        if (start_y < 2024 and start_m != 1) or (end_y < 2024 and end_m != 12):
            raise ValueError(
                "Legacy data before 2024 can only be downloaded in annual chunks. "
                "Please use full years like '--start 2020' and '--end 2022'."
            )

    for year, month in _expand_months((start_y, start_m), (end_y, end_m)):
        fetch_data(
            year=year,
            month=month if year >= 2024 else None,
            skip_if_exists=skip_if_exists,
            verbose=verbose,
        )


def sample(start_arg, end_arg, fraction, seed, output, verbose):
    # TODO: Infer storage format (CSV, feather, parquet) and raise error
    #  when `pyarrow` is not available.

    df = thinned_dataset(
        start_arg,
        end_arg,
        fraction=fraction,
        seed=seed,
        verbose=verbose,
    )
    df.to_csv(output, index=False)

    if verbose:
        print(f"Sample saved to: {output}")


def _expand_months(start_tup, end_tup):
    """Yield (year, month) tuples from start to end inclusive."""
    start_y, end_m = start_tup
    end_y, end_m = end_tup
    while (start_y, end_m) <= (end_y, end_m):
        yield start_y, end_m
        if end_m == 12:
            start_y += 1
            end_m = 1
        else:
            end_m += 1


if __name__ == "__main__":
    main()

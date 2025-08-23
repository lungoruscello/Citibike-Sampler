"""
A command-line tool to generate a thinned subset of NYC's Citi Bike dataset,
with support for multi-year or multi-month coverage.

Author: S. Langenbach (ETHZ)
Licence: MIT
"""

import argparse
import logging
from pathlib import Path

from .sampler import sample

ROOT_DIR = Path(__file__).parent

logging.basicConfig(level=logging.INFO)


def main():
    parser = argparse.ArgumentParser(
        prog="cbike_sampler",
        description=(
            "Generate a random sample from the full Citi Bike data for the "
            "specified time range. Automatically triggers downloads if the "
            "required source data is missing from the local cache. "
            "Accepts time formats like: 2020, '2021', '2021-5', '2021-05'."
        )
    )
    parser.add_argument(
        "-s", "--start", type=str, required=True,
        help="Start of the sampling period, inclusive (e.g., 2020 or '2020-05') "
    )
    parser.add_argument(
        "-e", "--end", type=str, default=None,
        help="End of the sampling period, inclusive (e.g., 2024 or '2024-05'). "
             "If omitted, this is the same as `start` thereby resulting "
             "in sampled data for a single year or month only."
    )
    parser.add_argument(
        "-f", "--fraction", type=float, default=0.01,
        help="Sampling fraction (0 < f < 1). Default is 0.01 (=1%)."
    )
    parser.add_argument(
        "-o", "--output", type=str, required=True,
        help="Output CSV/feather/parquet file path"
    )
    parser.add_argument(
        "--seed", type=int, default=None,
        help="Optional seed for reproducible sampling"
    )
    parser.add_argument(
        "--workers", type=int, default=None,
        help="Optional limit on the maximum number of workers used for parallel "
             "sampling from individual trip-data shards"
    )
    parser.add_argument(
        "--quiet", action="store_true",
        help="Suppress progress output"
    )

    args = parser.parse_args()
    verbose = not args.quiet

    start = args.start
    end = args.end
    end = start if end is None else end

    sample_with_export(
        start, end,
        args.fraction,
        args.seed,
        args.workers,
        args.output,
        verbose
    )


def sample_with_export(
        start_arg, end_arg,
        fraction,
        seed,
        max_workers,
        output,
        verbose
):
    # infer output file type
    supported = ['csv', 'feather', 'parquet']
    suffix = output.split('.')[-1].lower()
    if suffix not in supported:
        print('WARNING: Output file format not recognised. Defaulting to csv.')
        output = f'{output}.csv'
        suffix = 'csv'

    if suffix != 'csv':
        if not _pyarrow_available():
            raise ValueError(
                f"To use output file formats 'feather' or 'parquet', please "
                f"first install 'pyarrow' via pip, conda, or mamba."
            )

    # sample
    df = sample(
        start_arg,
        end_arg,
        fraction=fraction,
        seed=seed,
        max_workers=max_workers,
        verbose=verbose,
    )

    # export to disk
    if suffix == 'csv':
        df.to_csv(output, index=False)
    else:
        # avoid `ArrowTypeError`
        df.start_station_id = df.start_station_id.astype('str')
        df.end_station_id = df.end_station_id.astype('str')
        if suffix == 'feather':
            df.to_feather(output)
        else:
            df.to_parquet(output)

    if verbose:
        print(f"Sample saved to: {output}")


def _pyarrow_available():
    try:
        import pyarrow
    except ModuleNotFoundError:
        return False
    else:
        return True


if __name__ == "__main__":
    main()

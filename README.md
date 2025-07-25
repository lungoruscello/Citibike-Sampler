# citibike_sampler

*A lightweight command-line tool to download and sample NYC Citi Bike trip-data.*  

---

## Why Use This?

The full [Citi Bike dataset](https://citibikenyc.com/system-data) contains **millions of trip records per month**.
[Public data archives](https://s3.amazonaws.com/tripdata/index.html) are also organised differently over time and 
analysts looking to work with trip records for several years will find them organised in hundreds of compressed CSV 
files. Working with this raw data can be time-consuming and resource-intensive.

This tool helps you:
- Download only the months or yeats you care about
- Extract a **random sample of trip data across multiple months or years**
- Output the result as a **single, clean DataFrame or file** for easy use in data analysis or machine learning workflows

---

## Installation

Installation is best done using [`pipx`](https://pipx.pypa.io/stable/). 
This allows you to run the script as a command-line tool without polluting your global Python environment or 
having to manage virtual environments manually.

```bash
pipx install git+https://github.com/lungoruscello/citibike_sampler.git
```

---

## Example Usage 

```bash
# Download trip data from June 2024 to June 2025 
cbike download --year 2023 --month 3

# Sample 1% of trips and save to CSV
cbike sample --start 2022-01 --end 2022-12 --fraction 0.01 --output sample.csv
```

---

## Pre-sampled data

If you are just looking to explore or prototype with Citi Bike data, you also use the pre-sampled dataset
included in this repository.

Specifically, [`citibike_202001-202506_sampled_0.001.parquet`](citibike_202001-202506_sampled_0.001.parquet) (≈12 MB compressed). 
random sample of **0.1% of all Citi Bike trip records from Jan 2020 to June 2025** has been precomputed and is available in this repository under 

This file includes:
- Cleaned, standardized column names
- All trips across all months in a single file
- CSV format (gzip-compressed)

Trip data is provided by Citi Bike NYC under the [Open Data Commons Public Domain Dedication and License (PDDL)](https://opendatacommons.org/licenses/pddl/1-0/).

## Requirements

* Python 3.9 or higher
* pandas
* tqdm
* pyarrow

## Licence

MIT Licence. See [LICENSE.txt](https://github.com/lungoruscello/citibike_sampler/blob/master/LICENSE.txt) for details.
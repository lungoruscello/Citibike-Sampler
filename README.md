# Citibike-Sampler

*A Python tool to facilitate work with data from NYC's Citi Bike network.*  

<!-- 
Keywords: Citi Bike data, Python package, download and combine Citi Bike records
-->

[![PyPI Latest Release](https://img.shields.io/pypi/v/citibike-sampler.svg)](https://pypi.org/project/citibike-sampler/)
[![MIT License](https://img.shields.io/badge/license-MIT-green.svg)](https://github.com/lungoruscello/Citibike-Sampler/blob/master/LICENSE.txt)


## Why use this?
Data from the ['Citi Bike' system in NYC](https://citibikenyc.com/system-data) captures real-world patterns of urban mobility at very high resolution. 
As such, the data is widely used in research and practical applications.

However, working with the [raw source data](https://s3.amazonaws.com/tripdata/index.html) can be tedious. In a single year, the Citi Bike system records tens of 
millions of bike rides, equating to several GB worth of data. Furthermore, historical trip records are spread over hundreds 
of CSV files that use an inconsistent archive layout over time (annual bundles before 2024, monthly archives 
after).

**Citibike-Sampler** streamlines your workflow by providing:

* a convenient **data downloader** with consistent local caching;
* a **data loader** for accessing the full trip records; and
* a **random sampler** to draw representative subsets of the full Citi Bike data spanning multiple months
or years. 

Random sampling allows you to quickly explore multi-year trends in the Citi Bike data, without having to load 
hundreds of millions of records into memory.


## Installation

### pip
**Citibike-Sampler** is available on [PyPI](https://pypi.org/project/citibike-sampler/) and can be
installed using `pip`:  

```bash
pip install citibike-sampler
```

### pipx (for CLI use)

If you only need data sampling from the command-line, installation is best done using 
[`pipx`](https://pipx.pypa.io/stable/):

```bash
pipx install git+https://github.com/lungoruscello/Citibike-Sampler.git
```
  
## Usage

### Python API

```python
from citibike_sampler import sample, load_all, get_cache_dir

# Randomly sample 1% of all trip records from the first half of 2025.
# (Will automatically download data from AWS if not already cached.)
sample_df = sample(start='2025-1', end='2025-6', fraction=0.01, seed=42)

# Plot daily aggregates of sampled trips (assumes matplotlib is available)
sample_df.set_index('ended_at').resample('1D').ride_id.count().plot()

# Load the full dataset (be careful: millions of rides per month!)
full_df = load_all(start='2025-1', end='2025-6') 

print(len(sample_df) / len(full_df))  # check the sampling fraction

print(get_cache_dir())  # inspect the local cache location  
```

### CLI

Generate a random sample of Citi Bike data directly from the terminal:

```bash
cbike_sampler --start 2025-1 --end 2025-6 --fraction 0.01 --seed 42 --output sampled.csv
```

This will create a *sampled.csv* file containing roughly 1% of all trip records from the first half of 2025. To store the 
sampling result as a Feather or Parquet file, simply change the suffix of the output filename accordingly (e.g., 
*sampled.parquet*).

## Requirements

* Python 3.9 or higher
* requests
* pandas
* tqdm
* pyarrow (optional, for Parquet/Feather export)

## Licence

MIT Licence. See [LICENSE.txt](https://github.com/lungoruscello/Citibike-Sampler/blob/master/LICENSE.txt) for details.
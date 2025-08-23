# CitibikeSampler

*A lightweight Python tool to download, ingest, and sample trip-data from NYC's Citi Bike network.*  

---

## Why use this?
Data from the ['Citi Bike' system in NYC](https://citibikenyc.com/system-data) captures real-world patterns of urban mobility at very high resolution. 
As such, the data is widely used in research and practical applications.

However, working with the [raw source data](https://s3.amazonaws.com/tripdata/index.html) can be tedious. In a single year, the Citi Bike system records tens of 
millions of rides, equating to several GB worth of data. Furthermore, historical trip records are spread over hundreds 
of CSV files that use an inconsistent archive layout over time (annual bundles before 2024, monthly archives 
after).

**CitibikeSampler** streamlines your workflow by providing:

* a convenient **data downloader** with consistent local caching;
* a **data loader** for accessing the full trip records; and
* a **random sampler** to draw representative subsets of the full Citi Bike dataset spanning multiple months
or years. 

Random sampling allows you to quickly explore multi-year trends in the data, without having to load hundreds of millions 
of records into memory.

---

## Installation

### pip
**CitibikeSampler** is available on [PyPI](https://pypi.org/project/CitibikeSampler/) and can be
installed using `pip`:  

```bash
pip install citibike_sampler
```

### pipx (for CLI use)

If you only need data sampling from the command-line, installation is best done using 
[`pipx`](https://pipx.pypa.io/stable/):

```bash
pipx install git+https://github.com/lungoruscello/citibike_sampler.git
```
---

  
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



### Command Line

Generate a random sample directly from the terminal:
```bash
cbike_sampler --start 2025-1 --end 2025-6 --fraction 0.01 --seed 42 --output sampled.csv
```

This will create a *sampled.csv* containing roughly 1% of all trip records from the first half of 2025. To store the 
sampling result as a Feather or Parquet file, simply change the suffix of the output filename accordingly (e.g., 
*sampled.parquet*)

## Requirements

* Python 3.9 or higher
* requests
* pandas
* tqdm
* pyarrow (optional, for Parquet/Feather export)

## Licence

MIT Licence. See [LICENSE.txt](https://github.com/lungoruscello/citibike_sampler/blob/master/LICENSE.txt) for details.
# DANDI Cache: `valid-nwb-file-to-number-of-datasets`

A one-to-one mapping from content IDs to the number of datasets in their NWB file, restricted to the files that are valid according to [`content-id-to-valid-nwb-file`](https://github.com/dandi-cache/content-id-to-valid-nwb-file).

The upstream cache maps each content ID to whether its NWB file is valid; this cache spans only the `true` entries. For each such content ID, the corresponding asset is streamed directly from the public DANDI Archive S3 bucket — the content ID is itself the S3 object identifier, so its URL is reconstructed without any API lookup — and its datasets are counted:

- HDF5 NWB files (stored as a single blob) are streamed with [`remfile`](https://github.com/flatironinstitute/remfile) and opened with [`h5py`](https://github.com/h5py/h5py); the count is the number of HDF5 datasets.
- Zarr NWB stores (`.nwb.zarr`) are read anonymously from S3 with [`s3fs`](https://github.com/fsspec/s3fs) and opened with [`zarr`](https://github.com/zarr-developers/zarr-python); the count is the number of Zarr arrays (the Zarr analogue of a dataset).

Because a content ID is content-addressed, each file's dataset count is immutable, so the cache is incremental: every run counts only content IDs it has not recorded yet, catching up over successive daily runs.

Updated frequently.

Primarily for use by developers.



## One-time use

If you only plan to use this cache infrequently or from disparate locations, you can directly download the latest version of the cache as a compressed [JSON Lines](https://jsonlines.org/) file from the `dist` branch:

### Python API (recommended)

```python
import gzip
import json

import requests

url = "https://raw.githubusercontent.com/dandi-cache/valid-nwb-file-to-number-of-datasets/refs/heads/dist/derivatives/valid_nwb_file_to_number_of_datasets.jsonl.gz"
response = requests.get(url)
lines = gzip.decompress(data=response.content).decode("utf-8").splitlines()
valid_nwb_file_to_number_of_datasets = [json.loads(line) for line in lines]
```

Each line is a single-entry mapping of `{"<content_id>": <number_of_datasets>}`.

### Save to file

```bash
curl https://raw.githubusercontent.com/dandi-cache/valid-nwb-file-to-number-of-datasets/refs/heads/dist/derivatives/valid_nwb_file_to_number_of_datasets.jsonl.gz -o valid_nwb_file_to_number_of_datasets.jsonl.gz
```



## Repeated use

If you plan on using this cache regularly, clone the `derivatives` branch of this repository:

```bash
git clone --branch derivatives https://github.com/dandi-cache/valid-nwb-file-to-number-of-datasets.git
```

Or, if you prefer [DataLad](https://www.datalad.org/):

```bash
datalad clone https://github.com/dandi-cache/valid-nwb-file-to-number-of-datasets.git --branch derivatives
```

Then set up a CRON on your system to pull the latest version of the cache at your desired frequency.

For example, through `crontab -e`, add:

```bash
0 0 * * * git -C /path/to/valid-nwb-file-to-number-of-datasets pull
```

This will minimize data overhead by only loading the most recent changes.

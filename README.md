# DANDI Cache: `valid-nwb-file-to-number-of-datasets`

A mapping from the content ID of every valid NWB file on the DANDI archive to the total number of datasets inside that file.

The set of valid NWB files is taken from the [`content-id-to-valid-nwb-file`](https://github.com/dandi-cache/content-id-to-valid-nwb-file) cache, restricted to the entries it marked `true`. Each such file is streamed directly from the public DANDI S3 bucket and read with [h5py](https://www.h5py.org/) (HDF5 assets) or [zarr](https://zarr.readthedocs.io/) (Zarr assets), and its datasets are counted.

Each line of the derivatives is a JSON object of the form:

```json
{"<content_id>": <number_of_datasets>}
```

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

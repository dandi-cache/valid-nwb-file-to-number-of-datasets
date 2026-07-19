import argparse
import itertools
import json
import pathlib

import h5py
import remfile
import s3fs
import zarr

# Testing mode processes only this many items and writes to its own designated file
# (`derivatives/testing.jsonl`), leaving the real cache untouched.
_TESTING_LIMIT = 10
_CACHE_FILE_NAME = "valid_nwb_file_to_number_of_datasets.jsonl"
_TESTING_FILE_NAME = "testing.jsonl"

# The input is the `content-id-to-valid-nwb-file` cache, registered as an input subdataset.
_INPUT_FILE_PATH = (
    pathlib.Path("sourcedata") / "content-id-to-valid-nwb-file" / "derivatives" / "content_id_to_valid_nwb_file.jsonl"
)

# The public DANDI archive S3 bucket. Every asset is content-addressed, so each valid NWB
# file is reachable directly from its content ID without consulting the DANDI API:
#   - HDF5 assets are stored as a single blob at `blobs/<c[:3]>/<c[3:6]>/<content_id>`.
#   - Zarr assets are stored as a directory store under `zarr/<content_id>/`.
# The content ID alone does not say which layout an entry uses, so the blob key is probed
# first and the entry is treated as Zarr when no such blob exists.
_BUCKET = "dandiarchive"
_BLOB_URL_TEMPLATE = "https://dandiarchive.s3.amazonaws.com/blobs/{prefix}/{infix}/{content_id}"


def _load_content_id_to_validity(file_path: pathlib.Path) -> dict:
    """Load the `{content_id: bool}` mapping from the input JSONL, or an empty dict if missing."""
    records: dict = {}
    if not file_path.exists():
        return records
    with file_path.open(mode="r") as file_stream:
        for line in file_stream:
            if line.strip():
                records.update(json.loads(line))
    return records


def _load_previous_cache(file_path: pathlib.Path) -> dict:
    """Load the previously computed `{content_id: number_of_datasets}` mapping (empty on bootstrap)."""
    records: dict = {}
    if not file_path.exists():
        return records
    with file_path.open(mode="r") as file_stream:
        for line in file_stream:
            if line.strip():
                records.update(json.loads(line))
    return records


def _write_cache(file_path: pathlib.Path, records: dict) -> None:
    """Write the `{content_id: number_of_datasets}` mapping, one sorted content ID per line."""
    with file_path.open(mode="w") as file_stream:
        file_stream.writelines(f"{json.dumps({content_id: records[content_id]})}\n" for content_id in sorted(records))


def _count_hdf5_datasets(content_id: str) -> int:
    """Stream an HDF5 asset and count its datasets."""
    blob_url = _BLOB_URL_TEMPLATE.format(prefix=content_id[:3], infix=content_id[3:6], content_id=content_id)
    rem_file = remfile.File(url=blob_url)
    with h5py.File(name=rem_file, mode="r") as h5py_file:
        number_of_datasets = 0  # The root `/` is a group, not a dataset, so the count starts at zero.

        def _visit(_name: str, obj: object) -> None:
            nonlocal number_of_datasets
            if isinstance(obj, h5py.Dataset):
                number_of_datasets += 1

        h5py_file.visititems(_visit)
    return number_of_datasets


def _count_zarr_datasets(s3_filesystem: s3fs.S3FileSystem, content_id: str) -> int:
    """Stream a Zarr asset's metadata and count its arrays, the Zarr analogue of a dataset."""
    store = s3fs.S3Map(root=f"{_BUCKET}/zarr/{content_id}", s3=s3_filesystem, check=False)
    # DANDI writes consolidated metadata (`.zmetadata`) for every Zarr asset, so the whole
    # hierarchy loads in a single request and the walk below never touches the network again.
    # Fall back to the plain store for the rare asset that lacks it.
    try:
        root_group = zarr.open_consolidated(store=store, mode="r")
    except KeyError:
        root_group = zarr.open_group(store=store, mode="r")

    number_of_datasets = 0

    def _walk(group: zarr.hierarchy.Group) -> None:
        nonlocal number_of_datasets
        number_of_datasets += sum(1 for _ in group.arrays())
        for _name, subgroup in group.groups():
            _walk(subgroup)

    _walk(root_group)
    return number_of_datasets


def _count_datasets(s3_filesystem: s3fs.S3FileSystem, content_id: str) -> int:
    """Count the total number of datasets in the valid NWB file identified by `content_id`."""
    blob_key = f"{_BUCKET}/blobs/{content_id[:3]}/{content_id[3:6]}/{content_id}"
    if s3_filesystem.exists(blob_key):
        return _count_hdf5_datasets(content_id=content_id)
    return _count_zarr_datasets(s3_filesystem=s3_filesystem, content_id=content_id)


def _run(base_directory: pathlib.Path, testing: bool, limit: int | None) -> None:
    content_id_to_validity = _load_content_id_to_validity(file_path=base_directory / _INPUT_FILE_PATH)
    # Only the assets the upstream cache marked valid ('true') are counted.
    valid_content_ids = {content_id for content_id, is_valid in content_id_to_validity.items() if is_valid is True}

    derivatives_directory = base_directory / "derivatives"
    derivatives_directory.mkdir(parents=True, exist_ok=True)
    cache_file_path = derivatives_directory / (_TESTING_FILE_NAME if testing else _CACHE_FILE_NAME)
    valid_nwb_file_to_number_of_datasets = _load_previous_cache(file_path=cache_file_path)

    # Already-counted content IDs are exactly the keys already in the output, so re-runs skip
    # them and only pick up content IDs newly marked valid upstream.
    content_ids_to_process = sorted(valid_content_ids - valid_nwb_file_to_number_of_datasets.keys())

    # A testing run caps the batch tightly; otherwise the optional `--limit` bounds a single
    # run because streaming and walking each file is heavy.
    effective_limit = _TESTING_LIMIT if testing else limit
    content_ids_to_process = list(itertools.islice(content_ids_to_process, effective_limit))

    s3_filesystem = s3fs.S3FileSystem(anon=True)
    for content_id in content_ids_to_process:
        try:
            number_of_datasets = _count_datasets(s3_filesystem=s3_filesystem, content_id=content_id)
        except Exception as exception:
            # These files were already opened successfully upstream, so a failure here is
            # almost always transient (network). Skip it and leave it for a later run to retry
            # rather than recording a wrong count.
            print(f"Skipping `{content_id}`: {type(exception).__name__}: {exception}", flush=True)
            continue
        valid_nwb_file_to_number_of_datasets[content_id] = number_of_datasets

    _write_cache(file_path=cache_file_path, records=valid_nwb_file_to_number_of_datasets)


if __name__ == "__main__":
    default_base_directory = pathlib.Path(__file__).parent.parent

    parser = argparse.ArgumentParser(description="Update the valid-nwb-file-to-number-of-datasets DANDI cache.")
    parser.add_argument(
        "--base-directory",
        type=pathlib.Path,
        default=default_base_directory,
        help=(
            "The directory containing the `sourcedata` and `derivatives` directories. "
            "Set to the mounted dataset path when run inside the pipeline container; "
            "defaults to the repository root."
        ),
    )
    parser.add_argument(
        "--testing",
        action="store_true",
        help=(
            f"Run in testing mode: process only the first {_TESTING_LIMIT} items and write "
            f"`derivatives/{_TESTING_FILE_NAME}` instead of the real cache, leaving it "
            "untouched. Omit for a complete update."
        ),
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional cap on the number of newly valid content IDs to process in this run.",
    )
    args = parser.parse_args()

    _run(base_directory=args.base_directory, testing=args.testing, limit=args.limit)

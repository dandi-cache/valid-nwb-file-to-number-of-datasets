import argparse
import json
import pathlib
import traceback

import h5py
import remfile
import requests
import s3fs
import zarr

# Testing mode processes only this many valid content IDs and writes to its own designated
# file (`derivatives/testing.jsonl`), leaving the real cache untouched.
_TESTING_LIMIT = 10
_CACHE_FILE_NAME = "valid_nwb_file_to_number_of_datasets.jsonl"
_TESTING_FILE_NAME = "testing.jsonl"

# The DANDI archive publishes every asset in the public `dandiarchive` S3 bucket, and each
# asset's content ID is the S3 object identifier itself: the blob UUID for HDF5 assets (stored
# under `blobs/`), the Zarr store ID for Zarr assets (stored under `zarr/`). Both URLs are
# therefore reconstructable from the content ID alone, with no DANDI API lookup. A blob is laid
# out at `blobs/<id[:3]>/<id[3:6]>/<id>`; probing that URL distinguishes the two kinds (a Zarr
# content ID has no blob object and returns 404).
_BLOB_URL = "https://dandiarchive.s3.amazonaws.com/blobs/{prefix_1}/{prefix_2}/{content_id}"
_ZARR_STORE_ROOT = "dandiarchive/zarr/{content_id}"
_HEAD_TIMEOUT_SECONDS = 30


def _load_records(file_path: pathlib.Path) -> dict:
    """Load a `{content_id: value}` mapping from a JSONL file, or an empty dict if missing."""
    if not file_path.exists():
        return {}

    records: dict = {}
    with file_path.open(mode="r") as file_stream:
        for line in file_stream:
            if line.strip():
                records.update(json.loads(line))
    return records


def _write_records(file_path: pathlib.Path, records: dict) -> None:
    """Write a `{content_id: value}` mapping to a JSONL file, one sorted content ID per line."""
    with file_path.open(mode="w") as file_stream:
        file_stream.writelines(f"{json.dumps({content_id: records[content_id]})}\n" for content_id in sorted(records))


def _count_hdf5_datasets(s3_url: str) -> int:
    """Stream an HDF5 NWB file and count every dataset in it, reading only metadata."""
    rem_file = remfile.File(url=s3_url)
    with h5py.File(name=rem_file, mode="r") as h5py_file:
        count = 0

        def _visit(_name: str, obj: object) -> None:
            nonlocal count
            if isinstance(obj, h5py.Dataset):
                count += 1

        h5py_file.visititems(_visit)
    return count


def _count_zarr_arrays(store_root: str, s3_filesystem: s3fs.S3FileSystem) -> int:
    """Open a Zarr NWB store and count every array in it (the Zarr analogue of a dataset)."""
    store = s3fs.S3Map(root=store_root, s3=s3_filesystem, check=False)
    # DANDI's Zarr NWB stores carry consolidated metadata, so the whole hierarchy is read from a
    # single object; fall back to a plain open for any store that predates consolidation.
    try:
        group = zarr.open_consolidated(store=store, mode="r")
    except KeyError:
        group = zarr.open_group(store=store, mode="r")

    count = 0

    def _visit(obj: object) -> None:
        nonlocal count
        if isinstance(obj, zarr.core.Array):
            count += 1

    group.visitvalues(_visit)
    return count


def _number_of_datasets(content_id: str, s3_filesystem: s3fs.S3FileSystem) -> int:
    """Count the datasets in the NWB file for `content_id`, dispatching on its storage kind."""
    blob_url = _BLOB_URL.format(prefix_1=content_id[:3], prefix_2=content_id[3:6], content_id=content_id)
    head_response = requests.head(url=blob_url, timeout=_HEAD_TIMEOUT_SECONDS)
    if head_response.status_code == 200:
        return _count_hdf5_datasets(s3_url=blob_url)
    if head_response.status_code == 404:
        return _count_zarr_arrays(
            store_root=_ZARR_STORE_ROOT.format(content_id=content_id), s3_filesystem=s3_filesystem
        )
    # Any other status (e.g. an embargoed asset denying anonymous access) is unexpected for a
    # content ID drawn from the public valid set; surface it so the caller logs and skips it.
    head_response.raise_for_status()
    raise RuntimeError(f"Unexpected status {head_response.status_code} for `{blob_url}`.")


def _run(base_directory: pathlib.Path, testing: bool, limit: int | None) -> None:
    # Source: the `content-id-to-valid-nwb-file` cache, registered as an input subdataset. Its
    # main file maps each content ID to whether its NWB file is valid; this cache spans only the
    # `true` entries and records how many datasets each of those files contains.
    validity_file_path = (
        base_directory
        / "sourcedata"
        / "content-id-to-valid-nwb-file"
        / "derivatives"
        / "content_id_to_valid_nwb_file.jsonl"
    )
    content_id_to_validity = _load_records(file_path=validity_file_path)
    valid_content_ids = sorted(content_id for content_id, valid in content_id_to_validity.items() if valid)

    derivatives_directory = base_directory / "derivatives"
    derivatives_directory.mkdir(parents=True, exist_ok=True)

    # A real run resumes from the existing cache and processes only content IDs not yet recorded;
    # content IDs are content-addressed, so each file's dataset count is immutable and never needs
    # recomputing. A testing run starts fresh in its own designated file.
    output_file_path = derivatives_directory / (_TESTING_FILE_NAME if testing else _CACHE_FILE_NAME)
    content_id_to_count = {} if testing else _load_records(file_path=output_file_path)

    content_ids_to_process = [content_id for content_id in valid_content_ids if content_id not in content_id_to_count]
    if testing:
        # Testing run: keep only the first few content IDs, so the run is fast but still exercises
        # the real streaming and counting logic end to end.
        content_ids_to_process = content_ids_to_process[:_TESTING_LIMIT]
    elif limit is not None:
        content_ids_to_process = content_ids_to_process[:limit]

    logs_directory = derivatives_directory / "logs"
    logs_directory.mkdir(parents=True, exist_ok=True)
    errors_log_file_path = logs_directory / ("testing_errors.txt" if testing else "errors.txt")

    # `dandiarchive` is a public bucket, so it is read anonymously.
    s3_filesystem = s3fs.S3FileSystem(anon=True)
    errors: list[str] = []
    for content_id in content_ids_to_process:
        try:
            content_id_to_count[content_id] = _number_of_datasets(content_id=content_id, s3_filesystem=s3_filesystem)
        except Exception as exception:
            # A file may have become unreadable since it was validated (e.g. deleted upstream, or a
            # transient network error). Skip it with a logged reason rather than failing the whole
            # run; unrecorded content IDs are retried on the next run.
            errors.append(
                f"Error counting datasets for `content_id={content_id!r}`!\n\n"
                f"{type(exception).__name__}: {exception}\n\n"
                f"{traceback.format_exc()}"
            )

    _write_records(file_path=output_file_path, records=content_id_to_count)
    # The error log is rewritten in full each run, so it always reflects this run's batch.
    with errors_log_file_path.open(mode="w") as file_stream:
        file_stream.writelines(f"{error}\n\n" for error in errors)


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
            f"Run in testing mode: process only the first {_TESTING_LIMIT} valid content IDs and "
            f"write `derivatives/{_TESTING_FILE_NAME}` instead of the real cache, leaving it "
            "untouched. Omit for a complete update."
        ),
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help=(
            "Optional cap on the number of new content IDs to count in this run. Streaming each "
            "NWB file over the network is moderately heavy, so a batch size keeps a single run "
            "bounded; the incremental cache catches up over successive runs. Ignored under "
            "--testing."
        ),
    )
    args = parser.parse_args()

    _run(base_directory=args.base_directory, testing=args.testing, limit=args.limit)

from .pipelines import invenio_v1
from .pipelines import invenio_v3

import logging
import subprocess
from fs import open_fs
from .version import __version__

my_fs = open_fs(".")

try:
    commit_hash = subprocess.check_output(
        ["git", "rev-parse", "--short", "HEAD"]
    ).decode("utf-8")
except CalledProcessError:
    commit_hash = ""


def process(
    recid,
    source,
    loglevel,
    ark_json,
    ark_json_rel,
    skip_downloads=False,
    bibdoc=False,
    bd_ssh_host=None,
    timestamp=0,
):
    # Setup logging
    # DEBUG, INFO, WARNING, ERROR logging levels
    loglevels = [10, 20, 30, 40]
    logging.basicConfig(level=loglevels[loglevel], format="%(message)s")
    logging.info(f"BagIt Create tool {__version__} {commit_hash}")
    logging.info(f"Starting job. recid: {recid}, source: {source}")
    logging.debug(f"Set log level: {loglevels[loglevel]}")

    # Initialize the pipeline
    if source == "cds":
        pipeline = invenio_v1.InvenioV1Pipeline("https://cds.cern.ch/record/")
    elif source == "ilcdoc":
        pipeline = invenio_v1.InvenioV1Pipeline("http://ilcdoc.linearcollider.org/")
    elif source == "cod":
        pipeline = opendata.OpenDataPipeline()
    elif source == "zenodo" or source == "inveniordm":
        pipeline = invenio_v3.InvenioV3Pipeline(source)

    # Prepare folders
    base_path, temp_files_path = pipeline.prepare_folders(source, recid)

    # Create bagit.txt
    pipeline.add_bagit_txt(f"{base_path}/bagit.txt")

    # Create AIC
    aic_path, aic_name = pipeline.prepare_AIC(base_path, recid, timestamp)

    # Get metadata
    metadata, metadata_url, status_code, metadata_filename = pipeline.get_metadata(recid)

    # Save metadata file in the AIC
    pipeline.write_file(metadata, f"{aic_path}/{metadata_filename}")

    # Parse metadata for files
    files = pipeline.parse_metadata(f"{aic_path}/{metadata_filename}")

    # Create fetch.txt
    pipeline.create_fetch_txt(files, f"{base_path}/fetch.txt")

    # Download files
    pipeline.download_files(files, temp_files_path)

    # Copy files to final locations (AIUs)
    files = pipeline.move_files_to_aius(files, base_path, temp_files_path, recid)
    # `localpath` gets added here to files

    # Save bic-meta.json in the AIC
    files = pipeline.create_bic_meta(
        files, metadata_filename, metadata_url, aic_path, aic_name, base_path
    )
    # an entry for "bic-meta.json" gets added to files

    # Create manifest files
    pipeline.create_manifests(files, base_path, temp_files_path)

    # Verify created Bag
    pipeline.verify_bag(base_path)

    pipeline.delete_folder(temp_files_path)

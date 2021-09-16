from .pipelines import invenio_v1
from .pipelines import invenio_v3
from .pipelines import opendata

import logging
import subprocess
from fs import open_fs
from .version import __version__

my_fs = open_fs(".")

try:
    commit_hash = subprocess.check_output(
        ["git", "rev-parse", "--short", "HEAD"]
    ).decode("utf-8")
except:
    commit_hash = ""


def process(
    recid,
    source,
    loglevel,
    target,
    dry_run=False,
    alternate_uri=False,
    bibdoc=False,
    bd_ssh_host=None,
    timestamp=0,

):
    # Setup logging
    # DEBUG, INFO, WARNING, ERROR logging levels
    loglevels = [10, 20, 30, 40]
    logging.basicConfig(level=loglevels[loglevel], format="%(message)s")
    logging.info(f"BagIt Create tool {__version__} {commit_hash}")
    logging.info(f"Starting job.. Resource ID: {recid}. Source: {source}")
    logging.debug(f"Set log level: {loglevels[loglevel]}")

    if dry_run:
        logging.warning(
            f"This will be a DRY RUN. A 'light' bag will be created, not downloading \
            or moving any payload file, but checksums *must* be available from the \
            metadata, or no valid CERN AIP will be created."
        )
    try:
        # Initialize the pipeline
        if source == "cds":
            pipeline = invenio_v1.InvenioV1Pipeline("https://cds.cern.ch/record/")
        elif source == "ilcdoc":
            pipeline = invenio_v1.InvenioV1Pipeline("http://ilcdoc.linearcollider.org/")
        elif source == "cod":
            pipeline = opendata.OpenDataPipeline("http://opendata.cern.ch")
        elif source == "zenodo" or source == "inveniordm":
            pipeline = invenio_v3.InvenioV3Pipeline(source)

        # Save job details (Audit step 0)
        audit = [
            {
                "tool": f"BagIt Create tool {__version__}",
                "param": {"recid": recid, "source": source},
            }
        ]

        # Prepare folders
        base_path, temp_files_path, name = pipeline.prepare_folders(source, recid)

        # Create bagit.txt
        pipeline.add_bagit_txt(f"{base_path}/bagit.txt")

        # Get metadata
        metadata, metadata_url, status_code, metadata_filename = pipeline.get_metadata(
            recid
        )

        # Save metadata file in the meta folder
        pipeline.write_file(metadata, f"{base_path}/data/meta/{metadata_filename}")

        # Parse metadata for files
        files = pipeline.parse_metadata(f"{base_path}/data/meta/{metadata_filename}")

        if dry_run is True:
            # Create fetch.txt
            pipeline.create_fetch_txt(files, f"{base_path}/fetch.txt", alternate_uri)
        else:
            # Download files
            pipeline.download_files(files, f"{base_path}/data/content")

        # Create sip.json
        files = pipeline.create_bic_meta(
            files, audit, metadata_filename, metadata_url, base_path
        )
        # an entry for "sip.json" gets added to files

        # Create manifest files
        pipeline.create_manifests(files, base_path)

        pipeline.add_bag_info(base_path, f"{base_path}/bag-info.txt")

        # Verify created Bag
        pipeline.verify_bag(base_path)

        # move folder to specified path
        if (target):
            # Catch an exception so if the move folder fails, the original folder is deleted
            try:
                pipeline.move_folders(base_path, name, target)
                pipeline.delete_folder(base_path)
            except FileExistsError as e:
                logging.error(f"Job failed with error: {e}")
                pipeline.delete_folder(temp_files_path)
                pipeline.delete_folder(base_path)

                return {"status": 1, "errormsg": e}

        pipeline.delete_folder(temp_files_path)

        logging.info("SUCCESS")

        return {"status": 0, "errormsg": None}
    except FileExistsError as e:
        # Folder exists, gracefully stop.
        logging.error(f"Job failed with error: {e}")

        return {"status": 1, "errormsg": e}
    except Exception as e:
        # For any other error, print details and clean up
        #  any folder created
        logging.error(f"Job failed with error: {e}")
        pipeline.delete_folder(temp_files_path)
        pipeline.delete_folder(base_path)

        return {"status": 1, "errormsg": e}

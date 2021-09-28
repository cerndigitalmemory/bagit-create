from .pipelines import invenio_v1
from .pipelines import invenio_v3
from .pipelines import opendata
from .pipelines import indico
from .pipelines import local

import logging
import fs
from fs import open_fs
from .version import __version__
import time

my_fs = open_fs(".")


def process(
    recid,
    source,
    loglevel,
    target,
    localsource,
    dry_run=False,
    alternate_uri=False,
    bibdoc=False,
    bd_ssh_host=None,
    timestamp=0,
):
    ## Setup log

    # DEBUG, INFO, WARNING, ERROR log levels
    loglevels = [10, 20, 30, 40]
    log = logging.getLogger("basic-logger")
    log.setLevel(logging.DEBUG)

    log.propagate = False

    ## Console Handler
    # create console handler logging to the shell
    # what has been requested by the user (-v or -vv)
    ch_formatter = logging.Formatter("%(message)s")
    ch = logging.StreamHandler()
    ch.setLevel(loglevels[loglevel])
    ch.setFormatter(ch_formatter)
    log.addHandler(ch)

    ## File Handler
    # create file handler logging everything
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    fh = logging.FileHandler("bagitcreate.tmp")
    fh.setLevel(10)
    fh.setFormatter(formatter)
    log.addHandler(fh)

    log.info(f"BagIt Create tool {__version__}")
    log.info(f"Starting job.. Resource ID: {recid}. Source: {source}")
    log.debug(f"Set log level: {loglevels[loglevel]}")

    # Save timestamp
    timestamp = int(time.time())

    if dry_run:
        log.warning(
            "This will be a DRY RUN. A 'light' bag will be created, not downloading"
            "or moving any payload file, but checksums *must* be available from"
            "the metadata, or no valid CERN SIP will be created."
        )
    try:
        # Initialize the pipeline
        if source == "cds":
            pipeline = invenio_v1.InvenioV1Pipeline("https://cds.cern.ch/record/")
        elif source == "ilcdoc":
            pipeline = invenio_v1.InvenioV1Pipeline(
                "http://ilcdoc.linearcollider.org/record/"
            )
        elif source == "cod":
            pipeline = opendata.OpenDataPipeline("http://opendata.cern.ch")
        elif source == "zenodo" or source == "inveniordm":
            pipeline = invenio_v3.InvenioV3Pipeline(source)
        elif source == "indico":
            pipeline = indico.IndicoV1Pipeline("https://indico.cern.ch/")
        elif source == "local":
            pipeline = local.LocalV1Pipeline(localsource)

        if source == "local":
            recid = pipeline.get_folder_checksum(localsource)

        # Save job details (Audit step 0)
        audit = [
            {
                "tool": f"BagIt Create tool {__version__}",
                "param": {"recid": recid, "source": source},
            }
        ]

        base_path, name = pipeline.prepare_folders(source, recid)

        # Create bagit.txt
        pipeline.add_bagit_txt(f"{base_path}/bagit.txt")

        if source == "local":
            # Look for files in the source folder and prepare the files object
            files = pipeline.scan_files(localsource)
            metadata_url = None
        else:
            # Get metadata from upstream
            (
                metadata,
                metadata_url,
                status_code,
                metadata_filename,
            ) = pipeline.get_metadata(recid)

            # Save metadata file in the meta folder
            pipeline.write_file(metadata, f"{base_path}/data/meta/{metadata_filename}")

            # Parse metadata for files
            files = pipeline.parse_metadata(f"{base_path}/data/meta/{metadata_filename}")

        if dry_run is True:
            # Create fetch.txt
            pipeline.create_fetch_txt(files, f"{base_path}/fetch.txt", alternate_uri)
        else:

            if source == "local":
                files = pipeline.copy_files(
                    files, localsource, f"{base_path}/data/content"
                )

            else:
                # Download files
                pipeline.download_files(files, f"{base_path}/data/content")

        # Create sip.json
        files = pipeline.create_sip_meta(
            files, audit, timestamp, base_path, metadata_url
        )

        # To allow consistency and hashing of the attached log,
        # No events after this point will be logged to the file

        # Close the stream and release the lock on the file
        log.handlers[1].stream.close()
        # Remove the FileHandler (this allows to keep logging to the shell)
        log.removeHandler(log.handlers[1])

        # Move log file inside the meta folder
        fs.move.move_file(
            ".",
            "bagitcreate.tmp",
            f"{base_path}/data/meta",
            "bagitcreate.log",
        )

        pipeline.create_manifests(files, base_path)
        # file entries for "sip.json" and "bagitcreate.log" get added there

        # Create manifest files
        pipeline.add_bag_info(base_path, f"{base_path}/bag-info.txt")

        # Verify created Bag
        pipeline.verify_bag(base_path)

        # If a target folder is specified, move the created Bag there
        if target:
            # If the move fails, the original folder is deleted
            try:
                pipeline.move_folders(base_path, name, target)
                pipeline.delete_folder(base_path)
            except FileExistsError as e:
                log.error(f"Job failed with error: {e}")
                pipeline.delete_folder(base_path)

                return {"status": 1, "errormsg": e}

        log.info("SIP successfully created")

        return {"status": 0, "errormsg": None}

    # Folder exists, gracefully stop
    except FileExistsError as e:

        log.error(f"Job failed with error: {e}")

        return {"status": 1, "errormsg": e}
    # For any other error, print details about what happened and clean up
    #  any created file and folder
    except Exception as e:
        log.error(f"Job failed with error: {e}")
        pipeline.delete_folder(base_path)

        return {"status": 1, "errormsg": e}

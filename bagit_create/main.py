import logging
import time

import fs
from fs import open_fs

from . import utils
from .pipelines import base, codimd, indico, invenio_v1, invenio_v3, local, opendata
from .pipelines.base import WrongInputException
from .version import __version__

my_fs = open_fs(".")


def process(
    recid,
    source,
    loglevel,
    target=None,
    source_path=None,
    author=None,
    source_base_path=None,
    dry_run=False,
    bibdoc=False,
    bd_ssh_host=None,
    timestamp=0,
    cert=None,
    token=None,
    skipssl=False,
    url=None,
):
    # Save timestamp
    timestamp = int(time.time())

    # Save parameters with which bagit-create was called
    params = {
        "recid": recid,
        "url": url,
        "source": source,
        "loglevel": loglevel,
        "target": target,
        "source_path": source_path,
        "author": author,
        "source_base_path": source_base_path,
        "dry_run": dry_run,
        "bibdoc": bibdoc,
        "bd_ssh_host": bd_ssh_host,
        "timestamp": timestamp,
        "cert": cert,
    }

    try:
        base.BasePipeline.check_parameters_input(
            recid,
            url,
            source,
            source_path,
            author,
            source_base_path,
            bibdoc,
            bd_ssh_host,
            loglevel,
        )
    except WrongInputException as e:
        return {"status": 1, "errormsg": e}

    ## Setup log

    # DEBUG, INFO, WARNING, ERROR log levels
    loglevels = [10, 20, 30, 40]
    log = logging.getLogger("bic-basic-logger")
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
    logfilename = f"biclog::{recid}::{source}.tmp"
    fh = logging.FileHandler(logfilename)
    fh.setLevel(10)
    fh.setFormatter(formatter)
    log.addHandler(fh)

    log.info(f"BagIt Create tool {__version__}")
    log.info(f"Starting job.. Resource ID: {recid}. Source: {source}")
    log.debug(f"Set log level: {loglevels[loglevel]}")
    log.debug(f"Parametrs: {params}")

    if url:
        # If an URL is provided, parse it to get Source and Record ID
        source, recid = utils.parse_url(url)
        log.info(f"Got record {recid} from {source} from parsing URL")

    if dry_run:
        log.warning(
            """This will be a DRY RUN. A 'light' bag will be created, not downloading
            or moving any payload file, but checksums *must* be available from
            the metadata, or no valid CERN SIP will be created."""
        )
    try:
        # Initialize the pipeline
        if source == "cds":
            pipeline = invenio_v1.InvenioV1Pipeline(
                "https://cds.cern.ch/record/",
                cert_path=cert,
                recid=recid,
                token=token,
                skipssl=skipssl,
            )
        elif source == "ilcdoc":
            pipeline = invenio_v1.InvenioV1Pipeline(
                "http://ilcdoc.linearcollider.org/record/",
                cert_path=cert,
                recid=recid,
                token=token,
                skipssl=skipssl,
            )
        elif source == "codimd":
            pipeline = codimd.CodimdPipeline(token=token, recid=recid)
        elif source == "cod":
            pipeline = opendata.OpenDataPipeline("http://opendata.cern.ch")
        elif source == "zenodo" or source == "inveniordm":
            pipeline = invenio_v3.InvenioV3Pipeline(source)
        elif source == "indico":
            pipeline = indico.IndicoV1Pipeline("https://indico.cern.ch/", token=token)
        elif source == "ilcagenda":
            pipeline = indico.IndicoV1Pipeline("https://agenda.linearcollider.org/")
        elif source == "local":
            pipeline = local.LocalV1Pipeline(source_path)
            source_path = pipeline.get_abs_path(source_path)
            recid = pipeline.get_local_recid(source_path, author)
            params["recid"] = recid

        # Save job details (as audit step 0)
        audit = [
            {
                "tool": {
                    "name": "CERN BagIt Create",
                    "version": __version__,
                    "website": "https://gitlab.cern.ch/digitalmemory/bagit-create",
                    "params": params,
                },
                "action": "sip_create",
                "timestamp": timestamp,
                "message": "",
            }
        ]

        base_path, name = pipeline.prepare_folders(source, recid, timestamp)

        # Create bagit.txt
        pipeline.add_bagit_txt(f"{base_path}/bagit.txt")

        if source == "local":
            # Look for files in the source folder and prepare the files object
            files = pipeline.scan_files(source_path)
            metadata_url = None
        else:
            # Get metadata from upstream
            (
                metadata,
                metadata_url,
                status_code,
                metadata_filename,
            ) = pipeline.get_metadata(recid, source)

            # Save metadata file in the meta folder
            pipeline.write_file(
                metadata, f"{base_path}/data/content/{metadata_filename}"
            )

            # Parse metadata for files
            files, meta_file_entry = pipeline.parse_metadata(
                f"{base_path}/data/content/{metadata_filename}"
            )

            if bibdoc:
                # Get files metadata from bibdocfile
                output, files = pipeline.run_bibdoc(files, recid, bd_ssh_host)
                # Save bibdoc output
                pipeline.write_file(output, f"{base_path}/data/meta/bibdoc.txt")
                files.append(meta_file_entry)

        if dry_run is True:
            # Create fetch.txt
            pipeline.create_fetch_txt(files, source, f"{base_path}/fetch.txt")
        else:

            if source == "local":
                files = pipeline.copy_files(
                    files, source_path, f"{base_path}/data/content"
                )

            else:
                # Download files
                files = pipeline.download_files(files, base_path)

        # To allow consistency and hashing of the attached log,
        # no events after this point will be logged to the file

        # Close the stream and release the lock on the file
        log.handlers[1].stream.close()
        # Remove the FileHandler (this allows to keep logging to the shell)
        log.removeHandler(log.handlers[1])

        # Move log file inside the meta folder
        fs.move.move_file(
            ".",
            logfilename,
            f"{base_path}/data/meta",
            "bagitcreate.log",
        )

        # Create manifest files, according to the specified algorithms in the pipeline
        #  Uses the checksums from the parsed metadata if available
        #  Newly computed checksum get added to the SIP metadata, too.
        files = pipeline.create_manifests(files, base_path)

        # Create sip.json
        #  File entries for sip.json and the log files will be added here
        files = pipeline.create_sip_meta(
            files, audit, timestamp, base_path, metadata_url
        )

        # Compute checksums just for the last 2 added files (the sip.json and the log file)
        #  and *append* them to the already created manifests
        files = pipeline.create_manifests(files[-2:], base_path)

        # Add bag-info.txt file
        #  containing the final payload size and number of files
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

        # Clear up logging handlers so subsequent executions in the same python thread
        #  don't stack up
        if log.hasHandlers():
            log.handlers.clear()

        return {"status": 0, "errormsg": None, "foldername": name}

    # Folder exists, gracefully stop
    except FileExistsError as e:

        log.error(f"Job failed with error: {e}")

        # Clear up logging handlers so subsequent executions in the same python thread
        #  don't stack up
        if log.hasHandlers():
            log.handlers.clear()

        return {"status": 1, "errormsg": e}

    # For any other error, print details about what happened and clean up
    #  any created file and folder
    except Exception as e:
        log.error(f"Job failed with error: {e}")
        pipeline.delete_folder(base_path)

        # Clear up logging handlers so subsequent executions in the same python thread
        #  don't stack up
        if log.hasHandlers():
            log.handlers.clear()

        return {"status": 1, "errormsg": e}

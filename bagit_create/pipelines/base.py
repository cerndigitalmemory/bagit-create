import requests
import json
import os
import logging
import time
import re
import fs
from fs import open_fs
from ..version import __version__
import bagit
import shutil
from itertools import chain
from zlib import adler32
from datetime import date
from pathlib import Path
from jsonschema import validate

my_fs = open_fs("/")

log = logging.getLogger("basic-logger")


class BasePipeline:
    def __init__(self) -> None:
        pass

    def add_bag_info(self, path, dest):
        """
        The "bag-info.txt" file is a tag file that contains metadata elements
        describing the bag and the payload. The metadata elements contained
        in the "bag-info.txt" file are intended primarily for human use.
        """

        payload_path = Path(f"{path}/data")
        today = date.today()
        d = today.strftime("%Y-%m-%d")

        file_count = sum(len(files) for _, _, files in os.walk(payload_path))
        size = sum(f.stat().st_size for f in payload_path.glob("**/*") if f.is_file())

        baginfo = (
            f"Bag-Software-Agent: bagit-create {__version__}"
            " <https://github.com/cerndigitalmemory/bagit-create>\nBagging-Date:"
            f" {d}\nPayload-Oxum: {size}.{file_count}\n"
        )
        self.write_file(baginfo, dest)

    def validate_sip_json(self, schema_path, file_path):
        log.info("Validating sip.json..")
        try:
            with open(file_path) as json_file:
                data = json.load(json_file)

            with open(schema_path) as json_schema:
                schema = json.load(json_schema)

            validate(instance=data, schema=schema)
            log.info(f"Valid against {schema_path}")

        except Exception as err:
            log.error("sip.json validation failed with error", err)

    def downloadRemoteFile(self, src, dest):
        r = requests.get(src)
        with open(dest, "wb") as f:
            f.write(r.content)
        return True

    def downloadEOSfile(self, src, dest):
        try:
            my_fs.copy(src, dest)
        except (FileNotFoundError, fs.errors.ResourceNotFound):
            log.debug(f"  Path '{src}' not found. Skipping file. ")
            return False

    def adler32sum(self, filepath):
        """
        Compute adler32 of given file
        """
        BLOCKSIZE = 256 * 1024 * 1024
        asum = 1
        with open(filepath, "rb") as f:
            while True:
                data = f.read(BLOCKSIZE)
                if not data:
                    break
                asum = adler32(data, asum)
        return hex(asum)[2:10].zfill(8).lower()

    def merge_lists(self, a, b, keyname):
        output = []
        c = dict()
        for e in chain(a, b):
            key = e[keyname]
            c[key] = True
        for e in chain(a, b):
            key = e[keyname]
            if c[key]:
                c[key] = False
                output.append(e)
        return output

    def get_metadata(self, recid):
        return None

    def delete_folder(self, path):
        log.info(f"Deleted {path}")
        shutil.rmtree(path)

    def write_file(self, content, dest):
        if type(content) is bytes:
            open(f"{dest}", "ab").write(content)
        elif type(content) is dict:
            open(f"{dest}", "a").write(json.dumps(content, indent=4))
        else:
            open(f"{dest}", "a").write(content)
        log.info(f"Wrote {os.path.basename(dest)}")
        log.debug(f"({dest})")

    def download_file(self, sourcefile, dest):
        with requests.get(sourcefile, stream=True) as r:
            r.raise_for_status()
            with open(dest, "wb") as f:
                for chunk in r.iter_content(chunk_size=512 * 1024):
                    if chunk:  # filter out keep-alive new chunks
                        f.write(chunk)
        return True

    def add_bagit_txt(self, dest, version="0.97", encoding="UTF-8"):
        """
        Creates "bagit.txt", the Bag Declaration file (BagIt specification)
        """
        bagittxt = f"BagIt-Version: {version}\nTag-File-Character-Encoding: {encoding}\n"
        self.write_file(bagittxt, dest)
        return bagittxt

    def validate_bag(base_path):
        bag = bagit.Bag(base_path)
        return bag.is_valid()

    def compute_hash(self, filename, alg="md5"):
        """
        Compute hash of a given file
        """
        if alg == "adler32":
            computedhash = self.adler32sum(filename)
        else:
            computedhash = my_fs.hash(filename, alg)

        return computedhash

    def generate_manifest(self, files, algorithm, basepath):
        """
        Given an array of File objects (with `filename` and optionally `checksum`
        key), generate a manifest (BagIt specification) file listing every file
        and their checksums.

        <CHECKSUM> <FILENAME>
        <CHECKSUM> <FILENAME>
        ...

        If the requested algorithm is not available (or the `checksum` key is not
        there at all), compute the checksums on the downloaded files (found
        appending the filaname to the given base path) and add them to the SIP metadata.
        """
        contents = ""

        for idx, file in enumerate(files):
            path = f"{basepath}/{file['bagpath']}"
            checksum = None
            # Check if there's the "checksum" value in the File
            if "checksum" in file:
                # If it's a string create a single element list out of it
                if type(file["checksum"]) == str:
                    file["checksum"] = [file["checksum"]]
                # For each available checksum
                for avail_checksum in file["checksum"]:
                    p = re.compile(r"([A-z0-9]*):([A-z0-9]*)")
                    m = p.match(avail_checksum)
                    alg = m.groups()[0].lower()
                    matched_checksum = m.groups()[1]
                    # Check if it's the required one
                    if alg == algorithm:
                        checksum = matched_checksum

            # If we didn't find the required checksum but the file has been downloaded,
            #  compute it
            if not checksum and file["downloaded"]:
                checksum = self.compute_hash(path, algorithm)
                # Add the newly computed checksum to the SIP metadata
                if "checksum" in files[idx]:
                    files[idx]["checksum"].append(f"{algorithm}:{checksum}")
                else:
                    files[idx]["checksum"] = [f"{algorithm}:{checksum}"]

            # If there's no checksum and it's not possibile to compute it from disk, throw an error
            if not checksum and not file["downloaded"]:
                pass

            line = f"{checksum} {file['bagpath']}\n"
            contents += line

        return contents, files

    def generate_fetch_txt(self, files):
        """
        Given an array of "files" dictionaries (containing the `url`, `size` and `path` keys)
        generate the contents for the fetch.txt file (BagIt specification)

        <URL> <LENGTH_IN_BYTES> <FILENAME>
        <URL> <LENGTH_IN_BYTES> <FILENAME>
        <URL> <LENGTH_IN_BYTES> <FILENAME>
        ...

        """
        contents = ""
        for file in files:
            if type(file["origin"]["url"]) is list:
                url = file["origin"]["url"][0]
            else:
                url = file["origin"]["url"]

            # Workaround to get a valid fetch.txt (/eos/ is a malformed URL)
            if url[:5] == "/eos/":
                url = f"eos:/{url}"
            line = f'{url} {file["size"]} {file["origin"]["path"]}{file["origin"]["filename"]}\n'
            contents += line
        return contents

    def create_fetch_txt(self, files, dest):
        content = self.generate_fetch_txt(files)
        self.write_file(content, dest)

    def prepare_folders(self, source, recid, timestamp, delimiter_str="::"):
        # Get current path
        path = os.getcwd()

        # Prepare the base folder for the BagIt export
        #  e.g. "bagitexport::cds::42"
        base_name = (
            f"sip{delimiter_str}{source}{delimiter_str}{recid}{delimiter_str}{timestamp}"
        )
        base_path = f"{path}/{base_name}"

        os.mkdir(base_path)

        # Create data/ subfolder (bagit payload)
        os.mkdir(f"{base_path}/data")
        os.mkdir(f"{base_path}/data/meta")
        os.mkdir(f"{base_path}/data/content")

        self.base_path = base_path

        log.debug(f"Bag folder: {base_name}")

        return base_path, base_name

    def prepare_AIC(self, base_path, recid, timestamp=0, delimiter_str="::"):
        log.info("Creating AIC..")
        # Set timestamp to now if 0 is passed
        if timestamp == 0:
            log.debug("No timestamp provided. Using 'now'")
            timestamp = int(time.time())
        aic_name = f"{recid}{delimiter_str}{str(timestamp)}"
        aic_path = f"{base_path}/data/{aic_name}"
        os.mkdir(aic_path)
        self.aic_name = aic_name
        return aic_path, aic_name

    def move_files_to_aius(
        self, files, base_path, temp_relpath, recid, delimiter_str="::"
    ):
        # Copy each file from the temp folder to the AIU folders
        for idx, file in enumerate(files):
            if file["downloaded"] and file["metadata"] == False:
                filehash = self.compute_hash(f"{temp_relpath}/{file['filename']}")
                aiufoldername = f"{base_path}/data/{recid}{delimiter_str}{filehash}"
                try:
                    os.mkdir(aiufoldername)
                except:
                    log.warning(
                        "Trying to create an already existing AIU. Duplicate files or"
                        " colliding checksums?"
                    )
                files[idx][
                    "localpath"
                ] = f"data/{recid}{delimiter_str}{filehash}/{file['filename']}"
                try:
                    my_fs.copy(
                        f"{temp_relpath}/{file['filename']}",
                        f"{base_path}/data/{recid}{delimiter_str}{filehash}/{file['filename']}",
                    )
                except:
                    log.warning(f"{temp_relpath}/{file['filename']} already exists")

            if file["downloaded"] == False and file["metadata"] == False:
                if "checksum" in file:
                    p = re.compile(r"([A-z0-9]*):([A-z0-9]*)")
                    m = p.match(file["checksum"])
                    alg = m.groups()[0].lower()
                    matched_checksum = m.groups()[1]
                    files[idx][
                        "localpath"
                    ] = f"data/{recid}{delimiter_str}{matched_checksum}/{file['filename']}"
                else:
                    log.error(
                        "File was not downloaded and there's not checksum available from"
                        " metadata."
                    )

        return files

    def create_sip_meta(self, files, audit, timestamp, base_path, metadata_url=None):
        source = audit[0]["tool"]["params"]["source"]
        bic_meta = {
            "$schema": "https://gitlab.cern.ch/digitalmemory/sip-spec/-/blob/master/sip-schema-d1.json",
            "created_by": f"bagit-create {__version__}",
            "audit": audit,
            "source": audit[0]["tool"]["params"]["source"],
            "recid": audit[0]["tool"]["params"]["recid"],
            "metadataFile_upstream": metadata_url,
            "contentFiles": files,
            "sip_creation_timestamp": timestamp,
        }
        if source == "local":
            bic_meta.update(
                {
                    "source_details": {
                        "source_path": os.path.abspath(
                            audit[0]["tool"]["params"]["source_path"]
                        ),
                        "source_base_path": audit[0]["tool"]["params"][
                            "source_base_path"
                        ],
                    },
                    "author": audit[0]["tool"]["params"]["author"],
                }
            )

        bic_log_file_entry = {
            "origin": {
                "filename": "bagitcreate.log",
                "path": "",
            },
            "metadata": False,
            "downloaded": True,
            "bagpath": f"data/meta/bagitcreate.log",
        }

        files.append(bic_log_file_entry)

        bic_meta_file_entry = {
            "origin": {
                "filename": "sip.json",
                "path": "",
            },
            "metadata": False,
            "downloaded": True,
            "bagpath": f"data/meta/sip.json",
        }

        files.append(bic_meta_file_entry)

        self.write_file(
            json.dumps(bic_meta, indent=4), f"{base_path}/data/meta/sip.json"
        )

        return files

    def verify_bag(self, path):
        log.info(f"\n--\nValidating created Bag {path} ..")
        bag = bagit.Bag(path)
        valid = False
        try:
            valid = bag.validate()
        except bagit.BagValidationError as err:
            log.error(f"Bag validation failed: {err}")
        if valid:
            log.info(f"Bag successfully validated")
        log.info("--\n")
        return valid

    def move_folders(self, base_path, name, target):
        log.info(f"Moving files to {target} ..")

        # Check if destination folder exists
        if not os.path.isdir(target):
            os.mkdir(target)

        # make a new folder at the target folder with the original name
        new_path = f"{target}/{name}"
        os.mkdir(new_path)

        # move folder to the target location
        fs.move.move_fs(base_path, new_path)

    # Checks the input from the cli and raises error if there is a mistake
    def check_parameters_input(
        recid,
        source,
        source_path,
        author,
        source_base_path,
        bibdoc,
        bd_ssh_host,
        loglevel,
    ):
        """
        Checks if the combination of the parameters for the job make up for
        a valid operation
        """

        if (bibdoc or bd_ssh_host) and (source != "cds" and source != "ilcdoc"):
            raise WrongInputException(
                "bibdoc and bd_ssh_host parameters are only accepted when selecting CDS\
                as a source."
            )

        if bd_ssh_host and not bibdoc:
            raise WrongInputException(
                "bd_ssh_host is a setting supported only when using bibdoc."
            )

        if recid and source == "local":
            raise WrongInputException("The local pipeline is not expecting a recid.")

        if source != "local" and not recid:
            raise WrongInputException("Recid is missing.")
        if source_path and source != "local":
            raise WrongInputException("This pipeline is not expecting a source_path.")
        if source == "local" and not author:
            raise WrongInputException("Author is missing")
        if source == "local" and not source_path:
            raise WrongInputException("source_path is missing")
        if (source_base_path or source_path or author) and (source != "local"):
            raise WrongInputException(
                "source_base_path, source_path and author are parameters used only when source is local."
            )
        if source_base_path:
            if not source_base_path in os.path.abspath(source_path):
                raise WrongInputException("source_base_path should include source_path")


class WrongInputException(Exception):
    # This exception handles wrong cli commands
    pass

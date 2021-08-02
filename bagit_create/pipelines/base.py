import requests
import json
import os
import logging
import time
import re
from fs import open_fs
from ..version import __version__
import bagit
import shutil
from itertools import chain
from zlib import adler32

my_fs = open_fs("/")


class BasePipeline:
    def __init__(self) -> None:
        pass

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
        logging.info(f"Deleted {path}")
        shutil.rmtree(path)

    def write_file(self, content, dest):
        if type(content) is bytes:
            open(f"{dest}", "wb").write(content)
        elif type(content) is dict:
            open(f"{dest}", "w").write(json.dumps(content, indent=4))
        else:
            open(f"{dest}", "w").write(content)
        logging.info(f"Wrote {dest}")

    def download_file(self, sourcefile, dest):
        r = requests.get(sourcefile["url"])

        with open(dest, "wb+") as file:
            file.write(r.content)
        return True

    def add_bagit_txt(self, dest, version="0.97", encoding="UTF-8"):
        """
        Creates "bagit.txt", the Bag Declaration file (BagIt specification)
        """
        bagittxt = (
            f"BagIt-Version: {version}\n" f"Tag-File-Character-Encoding: {encoding}"
        )
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

    def generate_manifest(self, files, algorithm, temp_relpath=""):
        """
        Given an array of File objects (with `filename` and optionally `checksum`
        key), generate a manifest (BagIt specification) file listing every file
        and their checksums.

        <CHECKSUM> <FILENAME>
        <CHECKSUM> <FILENAME>
        ...

        If the requested algorithm is not available (or the `checksum` key is not
        there at all), compute the checksums on the downloaded files (found
        appending the filaname to the given base path)
        """
        contents = ""
        for file in files:
            if "localsavepath" in file:
                path = file["localsavepath"]
            else:
                path = temp_relpath
            if "checksum" in file:
                p = re.compile(r"([A-z0-9]*):([A-z0-9]*)")
                m = p.match(file["checksum"])
                alg = m.groups()[0].lower()
                matched_checksum = m.groups()[1]
                if alg == algorithm:
                    checksum = matched_checksum
                else:
                    logging.info(
                        f"Checksum {alg} found for {file['filename']} \
                        but {algorithm} was requested."
                    )
                    logging.debug(f"Computing {algorithm} of {file['filename']}")
                    checksum = self.compute_hash(f"{path}/{file['filename']}", algorithm)

            else:
                logging.debug(f"No checksum available for {file['filename']}")
                logging.debug(f"Computing {algorithm} of {file['filename']}")
                checksum = self.compute_hash(f"{path}/{file['filename']}", algorithm)

            line = f'{checksum} {file["localpath"]}\n'
            contents += line
        return contents

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
            line = f'{file["url"]} {file["size"]} {file["path"]}\n'
            contents += line
        return contents

    def create_fetch_txt(self, files, dest):
        content = self.generate_fetch_txt(files)
        self.write_file(content, dest)

    def prepare_folders(self, source, recid, delimiter_str="::"):
        # Get current path
        path = os.getcwd()

        # Prepare the base folder for the BagIt export
        #  e.g. "bagitexport::cds::42"
        base_name = f"bagitexport{delimiter_str}{source}{delimiter_str}{recid}"
        base_path = f"{path}/{base_name}"

        try:
            os.mkdir(base_path)
            # Create data/ subfolder (bagit payload)
            os.mkdir(f"{base_path}/data")
        except FileExistsError:
            logging.error("Directory exists")
            return {"status": "1", "errormsg": "Directory Exists"}

        # Create temporary folder to download the resource content
        temp_path = f"{path}/temp_{source}_{recid}"
        temp_relpath = f"temp_{source}_{recid}"
        os.mkdir(temp_path)
        # Create subfolder for saving upstream resource contents
        temp_file_path = f"{temp_path}/payload"
        os.mkdir(temp_file_path)

        logging.debug(f"Bag folder: {base_name}")
        return base_path, temp_file_path

    def prepare_AIC(self, base_path, recid, timestamp=0, delimiter_str="::"):
        logging.info("Creating AIC..")
        # Set timestamp to now if 0 is passed
        if timestamp == 0:
            logging.debug("No timestamp provided. Using 'now'")
            timestamp = int(time.time())
        aic_name = f"{recid}{delimiter_str}{str(timestamp)}"
        aic_path = f"{base_path}/data/{aic_name}"
        os.mkdir(aic_path)
        return aic_path, aic_name

    def move_files_to_aius(
        self, files, base_path, temp_relpath, recid, delimiter_str="::"
    ):
        # Copy each file from the temp folder to the AIU folders
        for idx, file in enumerate(files):
            if file["downloaded"] and file["metadata"] == False:
                filehash = self.compute_hash(f"{temp_relpath}/{file['filename']}")
                aiufoldername = f"{base_path}/data/{recid}{delimiter_str}{filehash}"
                os.mkdir(aiufoldername)
                files[idx][
                    "localpath"
                ] = f"data/{recid}{delimiter_str}{filehash}/{file['filename']}"

                my_fs.copy(
                    f"{temp_relpath}/{file['filename']}",
                    f"{base_path}/data/{recid}{delimiter_str}{filehash}/{file['filename']}",
                )
            if file["downloaded"] == False and file["metadata"] == False:
                if file["checksum"]:
                    p = re.compile(r"([A-z0-9]*):([A-z0-9]*)")
                    m = p.match(file["checksum"])
                    alg = m.groups()[0].lower()
                    matched_checksum = m.groups()[1]

                    files[idx][
                        "localpath"
                    ] = f"data/{recid}{delimiter_str}{matched_checksum}/{file['filename']}"

        return files

    def create_bic_meta(
        self, files, metadata_filename, metadata_url, aic_path, aic_name, base_path
    ):
        bic_meta = {
            "created_by": f"bagit-create {__version__}",
            "metadataFile_upstream": metadata_url,
            "contentFiles": files,
        }

        self.write_file(
            json.dumps(bic_meta, indent=4),
            f"{aic_path}/bic-meta.json",
        )

        bic_meta_file_entry = {
            "filename": "bic-meta.json",
            "path": "bic-meta.json",
            "metadata": False,
            "downloaded": True,
            "localpath": f"data/{aic_name}/bic-meta.json",
            "localsavepath": f"{base_path}/data/{aic_name}",
        }
        files.append(bic_meta_file_entry)

        return files

    def verify_bag(self, path):
        logging.info(f"Validating created Bag {path} ..")
        bag = bagit.Bag(path)
        valid = bag.is_valid()
        if valid:
            logging.info(f"Bag successfully validated")
        return valid

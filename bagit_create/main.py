#!/usr/bin/python3
"""
docstring
"""

import os
import time
import random
import string
import json
import copy
import shutil
import logging
import subprocess
from itertools import chain
import fs
from fs import open_fs
import requests
from .version import __version__
from . import cds
from . import cod
from . import bibdocfile

my_fs = open_fs(".")

try:
    commit_hash = subprocess.check_output(
        ["git", "rev-parse", "--short", "HEAD"]
    ).decode("utf-8")
except CalledProcessError:
    commit_hash = ""


def merge_lists(a, b, keyname):
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


def get_random_string(length):
    """
    Get a random string of the desired length
    """
    letters = string.ascii_lowercase
    result_str = "".join(random.choice(letters) for i in range(length))
    return result_str


def generate_bagit_txt(version="0.97", encoding="UTF-8"):
    """
    Creates the Bag Declaration file, as specified by the RFC:
    https://tools.ietf.org/html/rfc8493#section-2.1.1
    """
    bagittxt = f"BagIt-Version: {version}\n" f"Tag-File-Character-Encoding: {encoding}"
    return bagittxt


def checkunique(id):
    """
    Check if the given ID is unique in our system
    """
    # STUB
    logging.debug("ID is unique")
    return True


def generate_fetch_txt(files):
    """
    Given an array of "files" dictionaries (containing the `url`, `size` and `path` keys)
    generate the contents for the fetch.txt
    """
    contents = ""
    for file in files:
        line = f'{file["url"]} {file["size"]} {file["path"]}\n'
        contents += line
    contents += "\n"
    return contents


def generate_manifest(files, alg):
    """
    Given an array of "files" dictionaries (containing the `path` and `hash`)
    If a path is provided
    """
    contents = ""
    for file in files:
        line = f'{file["hash"]} {file["path"]}\n'
        contents += line
    contents += "\n"
    return contents


def write_file(contents, dest):
    """
    Write the given contents to the given destination
    """
    open(dest, "w").write(contents)


def generateReferences(filepathslist):
    """
    Given a list of file paths, compute hashes and generate a
    HASH FILEPATH
    ...
    file
    """
    references = ""
    for filename in filepathslist:
        filehash = compute_hash(filename)
        line = filehash + " " + filename
        references += line
        references += "\n"
    return references


def compute_hash(filename, alg="md5"):
    """
    Compute hash of a given file
    """
    computedhash = my_fs.hash(filename, alg)
    return computedhash


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

    result = {}

    # Setup logging
    # DEBUG, INFO, WARNING, ERROR logging levels
    loglevels = [10, 20, 30, 40]
    logging.basicConfig(level=loglevels[loglevel], format="%(message)s")
    logging.info(f"BagIt Create tool {__version__} {commit_hash}")
    logging.info(f"Starting job. recid: {recid}, source: {source}")
    logging.debug(f"Set log level: {loglevels[loglevel]}")

    # Set timestamp to now if 0 is passed
    if timestamp == 0:
        logging.debug("No timestamp provided. Using 'now'")
        timestamp = int(time.time())

    # Check if the given configuration makes sense
    if bibdoc is True and source != "cds":
        logging.error("Incompatible job configuration")
        return {
            "status": 1,
            "errormsg": "Incompatible job configuration",
            "details": None,
        }

    ## Step 0: preparation

    # Delimiter string
    delimiter_str = "::"

    # Save the plain resource ID (??)
    resid = copy.copy(recid)

    # Prepare the Arkivum JSON metadata object
    metadata_obj = {
        "system": source,
        "recid": recid,
        "metadataFile": None,
        "metadataFile_upstream": None,
        "contentFile": [],
        "timestamp": timestamp,
    }

    # Prepend the system name and the delimiter
    # recid = f"{source}{delimiter_str}{recid}"

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
    os.mkdir(f"{temp_path}/payload")

    # AIC will contain all the metadata related to resource
    # AIUs will contain the resource files

    # AIC
    aic_name = f"{recid}{delimiter_str}{str(timestamp)}"
    aic_path = f"{base_path}/data/{aic_name}"
    os.mkdir(aic_path)

    # Arkivum JSON file name
    arkjson_name = f"{source}_{resid}_files.json"

    # Create bagit.txt
    with open(f"{base_path}/bagit.txt", "w") as f:
        f.write(generate_bagit_txt())

    logging.debug(f"Prepared export with aic {aic_name}")

    logging.debug(f"Fetching the {source} Resource {resid}")

    ## Step 1: pipelines

    # CERN CDS Pipeline
    # consider refactoring the common parts to "invenio-vN" and setting
    # a more general flag
    if source == "cds" or source == "ilcdoc":

        ## Step 1.1: META
        # Get and save metadata
        if source == "cds":
            metadata, metadata_url, status_code = cds.getMetadata(
                resid, baseEndpoint="http://cds.cern.ch/record/"
            )
        elif source == "ilcdoc":
            metadata, metadata_url, status_code = cds.getMetadata(
                resid, baseEndpoint="http://ilcdoc.linearcollider.org/record/"
            )

        if status_code != 200:
            logging.error(
                f"Got HTTP {status_code}, a non 200 status code from the metadata \
                endpoint. Giving up."
            )
            return {
                "status": "1",
                "errormsg": "Metadata endpoint returned a non 200 http status code.",
            }

        # Save metadata upstream endpoint in the ark metadata
        metadata_obj["metadataFile_upstream"] = metadata_url

        # Save metadata.xml the AIC
        open(f"{aic_path}/metadata.xml", "wb").write(metadata)

        ## Step 1.2: PAYLOAD

        logging.debug("Getting source files locations")

        # From the metadata, extract info about the upstream file sources
        files = cds.getRawFilesLocs(f"{aic_path}/metadata.xml")

        # Append every file's URI to the ark metadata
        for sourcefile in files:
            # Check if the files are from Digital Memory and replace the paths with the
            #  full EOS one
            if "https://cern.ch/digital-memory/media-archive/" in sourcefile["uri"]:
                sourcefile["remote"] = "EOS"
                sourcefile["fullpath"] = sourcefile["uri"].replace(
                    "https://cern.ch/digital-memory/media-archive/",
                    "/eos/media/cds/public/www/digital-memory/media-archive/",
                )
            metadata_obj["contentFile"].append(sourcefile)

        logging.warning(f"Starting download of {len(files)} files")

        for sourcefile in files[0:10]:
            if not skip_downloads:
                destination = f'{temp_path}/payload/{sourcefile["filename"]}'

                logging.debug(
                    f'Downloading {sourcefile["filename"]} from {sourcefile["uri"]}..'
                )

                if sourcefile["remote"] == "HTTP":
                    cds.downloadRemoteFile(
                        sourcefile["uri"],
                        destination,
                    )
                elif sourcefile["remote"] == "EOS":
                    cds.downloadEOSfile(
                        sourcefile["uri"],
                        destination,
                    )
            else:
                logging.debug(
                    f'Skipped downloading of {sourcefile["filename"]} from \
                    {sourcefile["uri"]}..'
                )

        logging.warning("Finished downloading")

    # CERN Open Data pipeline
    if source == "cod":

        # Get and save metadata about the requested resource
        metadata = cod.getMetadata(resid)
        open(path + "/" + recid + "/" + "metadata.json", "w").write(json.dumps(metadata))
        files = metadata["metadata"]["files"]

        # Save metadata upstream endpoint in the ark metadata
        metadata_obj[
            "metadataFile_upstream"
        ] = f"https://opendata.cern.ch/api/records/{resid}"

        # From the metadata, extract info about the upstream file sources
        logging.debug(f"Got {len(files)} files")

        # Download and "unpack" every file list
        for sourcefile in files:
            # file lists are TXT and JSON, look for the JSON ones
            if sourcefile["key"][-4:] == "json":
                list_endpoint = (
                    f"https://opendata.cern.ch/record/{resid}/files/{sourcefile['key']}"
                )
                # Download the file list
                r = requests.get(list_endpoint)
                logging.debug(f"Unpacking file list {list_endpoint}")
                # For every file in the list
                for el in r.json():
                    # Remove the EOS instance prefix to get the path
                    el["fullpath"] = el["uri"].replace("root://eospublic.cern.ch/", "")
                    # Append the final path
                    metadata_obj["contentFile"].append(el)
            else:
                sourcefile["fullpath"] = sourcefile["uri"].replace(
                    "root://eospublic.cern.ch/", ""
                )
                # Append the final path
                metadata_obj["contentFile"].append(sourcefile)

    # Prepare AIC
    filelist = []

    # For every downloaded file:
    for el in my_fs.scandir(f"{temp_relpath}/payload"):
        if el.is_dir:
            for file in my_fs.listdir(f"{temp_relpath}/payload/{el.name}"):
                # prepare the relative path
                filepath = f"payload/{el.name}/{file}"
                # and append it to the file list
                filelist.append(filepath)
        else:
            filepath = f"payload/{el.name}"
            filelist.append(filepath)

    print(filelist)

    # Look for high-level metadata and copy it into the AIC
    # metadatafilenames = ["metadata.json", "metadata.xml"]

    # for filename in metadatafilenames:
    #    if filename in my_fs.listdir(recid):
    #        my_fs.copy(recid + "/" + filename, aicfoldername + "/" + filename)
    #        filelist.remove(recid + "/" + filename)

    logging.debug(f"Harvested files: {filelist}")

    # Generate and write references.txt
    # references = generateReferences(filelist)
    # my_fs.writetext(aicfoldername + "/" + "references.txt", references)

    # Copy each file from the temp folder to the AIU folders
    for file in filelist:
        filehash = compute_hash(f"{temp_relpath}/{file}")
        aiufoldername = f"{base_path}/data/{recid}{delimiter_str}{filehash}"
        os.mkdir(aiufoldername)
        my_fs.copy(
            f"{temp_relpath}/{file}",
            f"{base_name}/data/{recid}{delimiter_str}{filehash}/{fs.path.basename(file)}",
        )

    return

    # Finalize the Arkivum JSON metadata export (if requested)
    if ark_json:
        if source == "cds":
            # Set the metadata path as the locally downloaded one
            metadata_obj["metadataFile"] = f"{aicfoldername}/metadata.xml"
        if source == "cod":
            # Set the metadata path as the locally downloaded one
            metadata_obj["metadataFile"] = f"{aicfoldername}/metadata.json"
        if bibdoc:
            # Invoke bibdocfile and parse its output
            bd_files = bibdocfile.get_files_metadata(resid, ssh_host=bd_ssh_host)
            if bd_files != [{}]:
                # Naive strategy to merge results from bibdocfile:
                for bibdoc_entry in bd_files:
                    add = True
                    for file in metadata_obj["contentFile"]:
                        # Entries from the XML metadata have priority
                        if bibdoc_entry["filename"] == file["filename"]:
                            add = False
                            break
                    if add is True:
                        metadata_obj["contentFile"].append(bibdoc_entry)

        open(baseexportpath + "/" + arkjson_filename, "w").write(
            json.dumps(metadata_obj, indent=4)
        )

        logging.info(f"Wrote {arkjson_filename}")
        result["ark_json"] = arkjson_filename

    # Remove the temp folder
    shutil.rmtree(path + "/" + recid)

    result["status"] = 0
    result["errormsg"] = None
    result["details"] = baseexportpath

    # Return details about the executed job
    return result

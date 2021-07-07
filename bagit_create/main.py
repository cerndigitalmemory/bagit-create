#!/usr/bin/python3

from fs import open_fs
import fs
import os
import time
import random
import string
from . import cds
from . import cod
from . import bibdocfile
import json
import copy
import shutil
import logging
import subprocess
import requests


my_fs = open_fs(".")

## TODO: get version information from a manifest
version = "0.0.3"
try:
    commit_hash = subprocess.check_output(
        ["git", "rev-parse", "--short", "HEAD"]
    ).decode("utf-8")
except:
    commit_hash = ""


def merge_lists():
    """
    Given two dictionaries, merge them
    """
    output = []
    c = dict()
    for e in chain(a, b):
        key = e['key']
        c[key] = True
    for e in chain(a, b):
        key = e['key']
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


# Stub


def createBagItTxt(path, version="1.0", encoding="UTF-8"):
    """
    Creates the Bag Declaration file, as specified by the RFC:
    https://tools.ietf.org/html/rfc8493#section-2.1.1
    """
    bagittxt = f"BagIt-Version: {version}\n" f"Tag-File-Character-Encoding: {encoding}"
    my_fs.writetext(path + "/" + "bagit.txt", bagittxt)


def checkunique(id):
    """
    Check if the given ID is unique in our system
    """
    # STUB
    logging.debug("ID is unique")
    return True


def generateReferences(filepathslist):
    """
    Given a list of file paths, compute hashes and generate a
    HASH FILEPATH
    ...
    file
    """
    references = ""
    for filename in filepathslist:
        filehash = getHash(filename)
        line = filehash + " " + filename
        references += line
        references += "\n"
    return references


def getHash(filename, alg="md5"):
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

    # DEBUG, INFO, WARNING, ERROR logging levels
    loglevels = [10, 20, 30, 40]

    # Setup logging
    logging.basicConfig(level=loglevels[loglevel], format="%(message)s")
    logging.info(f"BagIt Create tool {version} {commit_hash}")
    logging.info(f"Starting job. recid: {recid}, source: {source}")
    logging.debug(f"Set log level: {loglevels[loglevel]}")

    # Set timestamp to now if 0 is passed
    if timestamp == 0:
        logging.debug("No timestamp provided. Using 'now'")
        timestamp = int(time.time())

    # Check if the given configuration makes sense
    if bibdoc == True and source != "cds":
        logging.error(
            "You asked to get metadata from bibdocfile but the selected upstream source is not CDS."
        )
        return {
            "status": 1,
            "errormsg": "Incompatible job configuration",
            "details": None,
        }

    # Delimiter string
    delimiter_str = "::"

    # Check if the target name is actually unique
    checkunique(recid)

    # Save the plain resource ID
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
    recid = f"{source}{delimiter_str}{recid}"

    # Get current path
    path = os.getcwd()

    # Prepare the high level folder which will contain the AIC and the AIUs
    baseexportpath = f"bagitexport{delimiter_str}{recid}"
    try:
        os.mkdir(path + "/" + baseexportpath)
    except FileExistsError:
        logging.error("Directory exists")
        return {"status": "1", "errormsg": "Directory Exists"}

    # Create temp folder to download all the related files
    os.mkdir(path + "/" + recid)

    # Prepare the base path strings
    # AIC folder name
    aicfoldername_base = f"{recid}{delimiter_str}{str(timestamp)}"
    # Arkivum JSON file name
    arkjson_filename = f"{source}_{resid}_files.json"

    # AIC folder path
    aicfoldername = f"{baseexportpath}/{aicfoldername_base}"

    logging.debug(f"AIC folder name is {aicfoldername}")

    # Create AIC folder
    os.mkdir(path + "/" + aicfoldername)

    logging.debug(f"Fetching the {source} Resource {resid}")

    # CERN CDS Pipeline
    ## consider refactoring the common parts to "invenio-vN" and setting a more general flag
    if source == "cds" or source == "ilcdoc":

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
                f"Got HTTP {status_code}, a non 200 status code from the metadata endpoint. Giving up."
            )
            return {
                "status": "1",
                "errormsg": "Metadata endpoint returned a non 200 http status code.",
            }

        # Save metadata upstream endpoint in the ark metadata
        metadata_obj["metadataFile_upstream"] = metadata_url

        # Write metadata.xml
        open(path + "/" + recid + "/" + "metadata.xml", "wb").write(metadata)
        logging.debug("Getting source files locations")

        # From the metadata, extract info about the upstream file sources
        files = cds.getRawFilesLocs(recid + "/metadata.xml")

        # Append every file's URI to the ark metadata
        for sourcefile in files:
            # Check if the files are from Digital Memory and replace the paths with the full EOS one
            if "https://cern.ch/digital-memory/media-archive/" in sourcefile["uri"]:
                sourcefile["remote"] = "EOS"
                sourcefile["fullpath"] = sourcefile["uri"].replace(
                    "https://cern.ch/digital-memory/media-archive/",
                    "/eos/media/cds/public/www/digital-memory/media-archive/",
                )
            metadata_obj["contentFile"].append(sourcefile)

        logging.warning(f"Starting download of {len(files)} files")
        for sourcefile in files:
            if not skip_downloads:
                destination = path + "/" + recid + "/" + sourcefile["filename"]
                logging.debug(
                    f'Downloading {sourcefile["filename"]} from {sourcefile["uri"]}..'
                )

                if sourcefile["remote"] == "HTTP":
                    filedata = cds.downloadRemoteFile(
                        sourcefile["uri"],
                        destination,
                    )
                elif sourcefile["remote"] == "EOS":
                    filedata = cds.downloadEOSfile(
                        sourcefile["uri"],
                        destination,
                    )
            else:
                logging.debug(
                    f'Skipped downloading of {sourcefile["filename"]} from {sourcefile["uri"]}..'
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
    for el in my_fs.scandir(recid):
        if el.is_dir:
            for file in my_fs.listdir(recid + "/" + el.name):
                # prepare the relative path
                filepath = recid + "/" + el.name + "/" + file
                # and append it to the file list
                filelist.append(filepath)
        else:
            filepath = recid + "/" + el.name
            filelist.append(filepath)

    # Look for high-level metadata and copy it into the AIC
    metadatafilenames = ["metadata.json", "metadata.xml"]

    for filename in metadatafilenames:
        if filename in my_fs.listdir(recid):
            my_fs.copy(recid + "/" + filename, aicfoldername + "/" + filename)
            filelist.remove(recid + "/" + filename)

    logging.debug(f"Harvested files: {filelist}")

    # Generate and write references.txt
    references = generateReferences(filelist)
    my_fs.writetext(aicfoldername + "/" + "references.txt", references)

    # Copy each file from the temp folder to the AIU folders
    for file in filelist:
        filehash = getHash(file)
        aiufoldername = baseexportpath + "/" + recid + delimiter_str + filehash
        os.mkdir(aiufoldername)
        my_fs.copy(file, aiufoldername + "/" + fs.path.basename(file))

    createBagItTxt(baseexportpath)

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
                    if add == True:
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

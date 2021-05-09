#!/usr/bin/python3

from fs import open_fs
import fs
import os
import time
import random
import string
from . import cds
from . import cod
import json
import copy
import shutil
import logging
import subprocess

my_fs = open_fs(".")

## TODO: get version information from a manifest
version = "0.0.2"
try:
    commit_hash = subprocess.check_output(["git", "rev-parse", "--short", "HEAD"]).decode("utf-8") 
except:
    commit_hash = ""

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


def process(recid, source, loglevel, skip_downloads=False, timestamp=0):

    # DEBUG, INFO, WARNING, ERROR
    loglevels = [10, 20, 30, 40]

    # Setup logging
    logging.basicConfig(level=loglevels[loglevel], format="%(message)s")
    logging.info(f"BagIt Create tool {version} {commit_hash}")
    logging.info(f"Starting job. recid: {recid}, source: {source}")
    logging.debug(f"Set log level: {loglevels[loglevel]}")

    # Delimiter string
    delimiter_str = "::"

    # Check if the target name is actually unique
    checkunique(recid)

    # Save the plain resource ID
    resid = copy.copy(recid)

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
        return {'status': '1',
                'errormsg': 'Directory Exists'}

    # Temp folder to pull the raw data
    os.mkdir(path + "/" + recid)

    # Create the AIC folder (ResourceID_timestamp)
    if timestamp == 0:
        logging.debug("No timestamp provided. Using 'now'")
        timestamp = int(time.time())
    aicfoldername = baseexportpath + "/" + recid + delimiter_str + str(timestamp)
    logging.debug(f"AIC folder name is {aicfoldername}")
    os.mkdir(path + "/" + aicfoldername)

    # CERN CDS Pipeline
    ## consider refactoring the common parts to "invenio-vN" and setting a more general flag
    if source == "cds" or source == "ilcdoc":
        logging.debug(f"Fetching the {source} Resource {resid}")

        # Get and save metadata
        if source == "cds":
            metadata = cds.getMetadata(resid, baseEndpoint="http://cds.cern.ch/record/")
        elif source == "ilcdoc":
            metadata = cds.getMeta
            data(resid, baseEndpoint="http://ilcdoc.linearcollider.org/record/")

        open(path + "/" + recid + "/" + "metadata.xml", "wb").write(metadata)
        logging.debug("Getting source files locations")

        # From the metadata, extract info about the upstream file sources
        files = cds.getRawFilesLocs(recid + "/metadata.xml")
        logging.debug(f"Got {len(files)} files")
        for sourcefile in files:
            destination = path + "/" + recid + "/" + sourcefile["filename"]
            logging.debug(f'Downloading {sourcefile["filename"]} from {sourcefile["uri"]}..')
            if skip_downloads:
                filedata = (
                    b"FILEDATA DOWNLOAD SKIPPED. If you need the real payloads, remove the --skipdownloads flag."
                    + get_random_string(5)
                )
                open(destination, "wb").write(filedata)
                logging.debug("skipped download")
            elif sourcefile["remote"] == "HTTP":
                filedata = cds.downloadRemoteFile(
                    sourcefile["uri"],
                    destination,
                )
            elif sourcefile["remote"] == "EOS":
                filedata = cds.downloadEOSfile(
                    sourcefile["uri"],
                    destination,
                )

    # CERN Open Data pipeline
    if source == "cod":
        # Get and save metadata about the requested resource
        metadata = cod.getMetadata(recid)
        open(path + "/" + recid + "/" + "metadata.json", "w").write(json.dumps(metadata))

    # Prepare AIC
    filelist = []

    for el in my_fs.scandir(recid):
        if el.is_dir:
            for file in my_fs.listdir(recid + "/" + el.name):
                filepath = recid + "/" + el.name + "/" + file
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

    references = generateReferences(filelist)

    my_fs.writetext(aicfoldername + "/" + "references.txt", references)

    # AIUs
    for file in filelist:
        filehash = getHash(file)
        aiufoldername = baseexportpath + "/" + recid + delimiter_str + filehash
        os.mkdir(aiufoldername)
        my_fs.copy(file, aiufoldername + "/" + fs.path.basename(file))

    createBagItTxt(baseexportpath)

    # Remove temp folder
    shutil.rmtree(path + "/" + recid)

    return {'status': 0,
            'errormsg': None,
            'details': baseexportpath}

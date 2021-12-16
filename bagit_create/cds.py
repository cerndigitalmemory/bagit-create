# CDS functions and helpers
# Interacts with CDS API to get metadata, parses and extract file informations
# Helper functions to trigger the download of such files from HTTP and EOS

import requests
from pymarc import marcxml
from fs import open_fs
import pprint
import ntpath
import logging
import re
import fs

my_fs = open_fs("/")


def getMetadata(record_id, baseEndpoint, type="xml"):
    """
    Get MARC21 metadata from a CDS record ID
    Returns: [metadata_serialized, metadata_upstream_url, operation_status_code]
    """
    record_url = f"{baseEndpoint}{record_id}"

    if type == "xml":
        of = "xm"
    elif type == "dc":
        of = "xd"

    payload = {"of": of}

    r = requests.get(record_url, params=payload)

    logging.debug(f"Getting {r.url}")

    return r.content, r.url, r.status_code


def getRawFilesLocs(metadata_filename):
    """
    Given a MARC21 metadata file,
    return an array of "Files" objects, containing:
    `filename`
    `uri`
    `remote`
    `hash`
    `size`
    """

    # Parse the XML as MARC21
    #  and get the first record (result should be one record anyway)
    record = marcxml.parse_xml_to_array(metadata_filename)[0]
    # Look for 856 entries
    #  MARC21: 856 - Electronic Location and Access (R)
    files = []
    for f in record.get_fields("856"):
        obj = {}

        # Unknown size fallback
        obj["size"] = 0

        if f["u"]:
            obj["url"] = f["u"]
            obj["remote"] = "HTTP"
        elif f["d"]:
            obj["url"] = f["d"]
            obj["remote"] = "EOS"
        else:
            logging.debug(f'Skipped 856 entry "{f}", no u or d field.')
            continue

        # File checksum
        if f["w"]:
            p = re.compile(r"\([A-Za-z]*:([A-Za-z0-9]*).*;([A-Za-z0-9]*)")
            m = p.match(f["w"])
            alg = m.groups()[0].lower()
            checksum = m.groups()[1]
            obj["checksum"] = f"{alg}:{checksum}"

        # File size
        if f["s"]:
            obj["size"] = int(f["s"])

        # Get basename
        if obj["url"]:
            obj["filename"] = ntpath.basename(obj["url"])
            obj["path"] = obj["filename"]

        obj["metadata"] = False
        obj["downloaded"] = False

        if obj["filename"]:
            files.append(obj)
        else:
            logging.warning(f'Skipped entry "{f}", no basename found (probably an URL?)')

    return files


def downloadRemoteFile(src, dest, verify=True):
    try:
        r = requests.get(src, stream=True, verify=verify)
        with open(dest, "wb") as f:
            for chunk in r.raw.stream(1024, decode_content=False):
                if chunk:
                    f.write(chunk)

        # r = requests.get(src)
        # with open(dest, "wb") as f:
        #    f.write(r.content)

    except Exception as e:
        logging.warning(f"Couldn't not download file {src}. Error {e}")
    return True


def downloadEOSfile(src, dest):
    try:
        my_fs.copy(src, dest)
    except (FileNotFoundError, fs.errors.ResourceNotFound):
        logging.warning(f"  Path '{src}' not found. Skipping file. ")
        return False


def prettyprint(obj, indentsize=4):
    pp = pprint.PrettyPrinter(indent=indentsize)
    pp.pprint(obj)

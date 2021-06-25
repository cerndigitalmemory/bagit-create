# CDS functions and helpers
# Interacts with CDS API to get metadata, parses and extract file informations
# Helper functions to trigger the download of such files from HTTP and EOS

import requests
import xml.etree.ElementTree as ET
from pymarc import MARCReader, marcxml
from fs import open_fs
import pprint
import ntpath
import logging
import os.path
import re

import fs

my_fs = open_fs("/")


def getMetadata(record_id, baseEndpoint, type="xml"):
    """
    Get MARC21 metadata from a CDS record ID
    Returns a string
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
    Given a MARX21 metadata file,
    get source file locations (on EOS or HTTP remotes)
    """

    # Parse the XML as MARC21
    #  and get the first record (result should be one record anyway)
    record = marcxml.parse_xml_to_array(metadata_filename)[0]
    # Look for 856 entries
    #  MARC21: 856 - Electronic Location and Access (R)
    rawData = []
    for f in record.get_fields("856"):
        obj = {}

        if f["u"]:
            obj["uri"] = f["u"]
            obj["remote"] = "HTTP"
        elif f["d"]:
            obj["uri"] = f["d"]
            obj["remote"] = "EOS"
        else:
            logging.debug(f'Skipped 856 entry "{f}", no u or d field.')
            continue
        
        # File checksum
        if f["w"]:
            p = re.compile('\([A-Za-z]*:([A-Za-z0-9]*).*;([A-Za-z0-9]*)')
            m = p.match(f["w"])
            alg = m.groups()[0].lower()
            checksum = m.groups()[1]
            obj["checksum"] = f'{alg}:{checksum}'

        # File size
        if f["s"]:
            obj["size"] = int(f["s"])

        # Get basename
        if obj["uri"]:
            obj["filename"] = ntpath.basename(obj["uri"])

        if obj["filename"]:
            rawData.append(obj)
        else:
            logging.warning(f'Skipped entry "{f}", no basename found (probably an URL?)')

    return rawData


def downloadRemoteFile(src, dest):
    r = requests.get(src)
    with open(dest, "wb") as f:
        f.write(r.content)


def downloadEOSfile(src, dest):
    try:
        my_fs.copy(src, dest)
    except:
        logging.debug(f"  Path '{src}' not found. Skipping file. ")


def prettyprint(obj, indentsize=4):
    pp = pprint.PrettyPrinter(indent=indentsize)
    pp.pprint(obj)

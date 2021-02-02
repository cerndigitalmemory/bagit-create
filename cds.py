import requests
import xml.etree.ElementTree as ET
from pymarc import MARCReader, marcxml
from fs import open_fs
import pprint
import ntpath

import os.path

import fs

my_fs = open_fs("/")


def getMetadata(record_id, type="xml"):
    """
    Get MARC21 metadata from a CDS record ID
    Returns a string
    """
    baseEndpoint = "http://cds.cern.ch/record/" + str(record_id)

    if type == "xml":
        of = "xm"
    elif type == "dc":
        of = "xd"

    payload = {"of": of}

    r = requests.get(baseEndpoint, params=payload)
    # filename = str(record_id) + "_metadata.xml"
    print("Getting", r.url)

    return r.content


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

        # Get basename
        if obj["uri"]:
            obj["filename"] = ntpath.basename(obj["uri"])

        rawData.append(obj)
    return rawData


def downloadRemoteFile(src, dest):
    r = requests.get(src)
    with open(dest, "wb") as f:
        f.write(r.content)


def downloadEOSfile(src, dest):
    try:
        my_fs.copy(src, dest)
    except:
        print(f"  Path '{src}' not found. Skipping file. ")


def prettyprint(obj, indentsize=4):
    pp = pprint.PrettyPrinter(indent=indentsize)
    pp.pprint(obj)

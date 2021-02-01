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
    # Look for 856 fields
    #  MARC21: 856 - Electronic Location and Access (R)
    rawData = []
    for f in record.get_fields("856"):
        obj = {}
        # Get filetype and URL
        if f["q"]:

            obj["filetype"] = f["q"]
            if f["u"]:
                obj["url"] = f["u"]
            elif f["d"]:
                obj["url"] = f["d"]
        # No file format but the URL is there
        elif f["u"]:
            obj["url"] = f["u"]
            obj["filetype"] = os.path.splitext(obj["url"])[1][1:].upper()

        # Get basename
        if obj["url"]:
            obj["filename"] = ntpath.basename(obj["url"])
        print(obj)
        rawData.append(obj)
    return rawData
    # print(f["q"], f["u"], f["d"])


def downloadRemoteFile(src, dest):
    r = requests.get(src)
    with open(dest, "wb") as f:
        f.write(r.content)


def downloadEOSfile(src, dest):
    my_fs.copy(src, dest)


def prettyprint(obj, indentsize=4):
    pp = pprint.PrettyPrinter(indent=indentsize)
    pp.pprint(obj)

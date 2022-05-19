# Getting metadata using CERN Open Data API

## https://github.com/cernopendata/cernopendata-client

import sys

# WORKAROUND to import the git "master" version
sys.path.insert(1, "/home/avivace/cern/cernopendata-client")

from cernopendata_client import downloader, searcher

SERVER_HTTP_URI = "http://opendata.cern.ch"
"""Default CERN Open Data server to query over HTTP protocol."""

# Check if record with the given recid exists
searcher.verify_recid(server=SERVER_HTTP_URI, recid=1)


def getMetadata(recid):
    if searcher.verify_recid(server=SERVER_HTTP_URI, recid=recid):
        return searcher.get_record_as_json(server=SERVER_HTTP_URI, recid=recid)


def test():
    metadata_from_recid = searcher.get_record_as_json(server=SERVER_HTTP_URI, recid=1)

    metadata_from_doi = searcher.get_record_as_json(
        server=SERVER_HTTP_URI, doi=metadata_from_recid["metadata"]["doi"]
    )

    recid_from_doi = metadata_from_doi["metadata"]["recid"]

    # Check for consistent results
    # Not working, from_title gives `More than one record fit this title.This should not happen.`
    # metadata_from_title = searcher.get_record_as_json(server=SERVER_HTTP_URI, title=metadata_from_recid["metadata"]["title"])
    # metadata_from_title == metadata_from_recid == metadata_from_doi

    prettyprint(metadata_from_recid)

    files_list = searcher.get_files_list(
        server=SERVER_HTTP_URI, record_json=metadata_from_recid, protocol="http"
    )

    prettyprint(files_list)


def downloadFile(recid):
    # Get the files list with a checksum, name, size, and URI
    remote_info = searcher.get_file_info_remote(server=SERVER_HTTP_URI, recid=recid)

    return remote_info

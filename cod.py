# Getting metadata using CERN Open Data API

## https://github.com/cernopendata/cernopendata-client

from cernopendata_client import searcher, downloader
from cds import prettyprint

SERVER_HTTP_URI = "http://opendata.cern.ch"
"""Default CERN Open Data server to query over HTTP protocol."""

# Check if record with the given recid exists
searcher.verify_recid(server=SERVER_HTTP_URI, recid=1)

metadata_from_recid = searcher.get_record_as_json(server=SERVER_HTTP_URI, recid=1)

metadata_from_doi = searcher.get_record_as_json(server=SERVER_HTTP_URI, doi=metadata_from_recid["metadata"]["doi"])

recid_from_doi = metadata_from_doi["metadata"]["recid"]

# Check for consistent results
# Not working, from_title gives `More than one record fit this title.This should not happen.`
# metadata_from_title = searcher.get_record_as_json(server=SERVER_HTTP_URI, title=metadata_from_recid["metadata"]["title"])
# metadata_from_title == metadata_from_recid == metadata_from_doi

prettyprint(metadata_from_recid)

files_list = searcher.get_files_list(server=SERVER_HTTP_URI, record_json=metadata_from_recid)

prettyprint(files_list)

# Get the files list with a checksum, name, size, and URI
remote_info = searcher.get_file_info_remote(recid=1)

prettyprint(remote_info)

# Download a file using the xrootd protocol
downloader.download_single_file(path = ".", file_location=remote_info[0]["uri"])
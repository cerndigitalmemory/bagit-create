from pathlib import PurePosixPath
from urllib.parse import unquote, urlparse

from bagit_create.exceptions import WrongInputException

# DEBUG, INFO, WARNING, ERROR log levels
loglevels = [10, 20, 30, 40]


def get_loglevel(index):
    """
    Returns the log level corresponding to the given index.
    Index should be 0 (DEBUG), 1 (INFO), 2 (WARNING), or 3 (ERROR).
    """
    if not type(index) is int or index < 0 or index >= len(loglevels):
        raise ValueError(f"Log level index out of range (0 to {len(loglevels)-1}).")
    return loglevels[index]


def parse_url(url):
    """
    Parses URLs with the pattern
    <HOSTNAME>/record/<RECORD_ID>
    and returns a couple (Source ID, Record ID)
    (CDS, Zenodo, Open Data are supported)
    """
    o = urlparse(url)
    if o.hostname == "cds.cern.ch":
        source = "cds"
    elif o.hostname == "opendata.cern.ch":
        source = "cod"
    elif o.hostname == "zenodo.org":
        source = "zenodo"
    elif o.hostname == "repository.cern":
        source = "cds-rdm"
    else:
        raise WrongInputException(
            "Unable to parse the given URL. Try manually passing the source and the record ID."
        )

    path_parts = PurePosixPath(unquote(urlparse(url).path)).parts

    # Ensures the path is in the form /record/<RECORD_ID>
    if path_parts[0] == "/" and (
        path_parts[1] == "record" or path_parts[1] == "records"
    ):
        # The ID is the second part of the path
        recid = path_parts[2]
    else:
        raise WrongInputException(
            "Unable to parse the given URL. Try manually passing the source and the record ID."
        )

    return (source, recid)

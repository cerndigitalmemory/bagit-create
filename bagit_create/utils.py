import configparser
import os
from pathlib import PurePosixPath
from urllib.parse import unquote, urlparse

from bagit_create.exceptions import WrongInputException


def load_sources(invenio_only=False):
    sources = (
        []
        if invenio_only
        else ["cds", "ilcdoc", "cod", "indico", "local", "ilcagenda", "codimd", "gitlab"]
    )
    config_file = configparser.ConfigParser()
    config_file.read(os.path.join(os.path.dirname(__file__), "pipelines/invenio.ini"))
    sources.extend(config_file.sections())
    return sources


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

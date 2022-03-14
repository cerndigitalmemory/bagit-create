from .. import utils

"""
This file contains unit tests for the URL parsing feature on the currently
supported digital repositories.
"""

cases = [
    {"url": "http://cds.cern.ch/record/2665537", "source": "cds", "recid": "2665537"},
    {"url": "https://cds.cern.ch/record/2665537", "source": "cds", "recid": "2665537"},
    {
        "url": "http://cds.cern.ch/record/2665537/files/cms_160312_03.png",
        "source": "cds",
        "recid": "2665537",
    },
    {"url": "http://opendata.cern.ch/record/8884", "source": "cod", "recid": "8884"},
    {"url": "https://zenodo.org/record/6220704", "source": "zenodo", "recid": "6220704"},
]


def test_url_parsing():
    for case in cases:
        result = source, recid = utils.parse_url(case["url"])
        assert result == (case["source"], case["recid"])

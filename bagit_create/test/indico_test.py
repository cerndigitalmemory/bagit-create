from ..pipelines import indico
from unittest import mock
import json, os, pytest


def test_parse_indico_metadata():
    indico_pipeline = indico.IndicoV1Pipeline("https://indico.cern.ch/")

    # Prepare the mock metadata and expected result from file
    indico_files = [
        {
            'size': 1864393,
            'url': 'https://indico.cern.ch/event/1024767/attachments/2266930/3849110/Documentation_Project_20210615meeting.pdf',
            'filename': 'Documentation_Project_20210615meeting.pdf',
            'path': 'Documentation_Project_20210615meeting.pdf',
            'title': 'The same notes in PDF',
            'content_type': 'application/pdf',
            'metadata': False,
            'downloaded': False,
            'localpath': 'data/content/Documentation_Project_20210615meeting.pdf',
        },
        {
            'size': 154244,
            'url': 'https://indico.cern.ch/event/1024767/contributions/4301928/attachments/2261620/3844353/vCHEP2021.slides',
            'filename': 'vCHEP2021.slides',
            'path': 'vCHEP2021.slides',
            'title': "the vCHEP slides made with the CERN Slides' app",
            'content_type': 'application/octet-stream',
            'metadata': False,
            'downloaded': False,
            'localpath': 'data/content/vCHEP2021.slides',
        },
        {
            'startDate': {
                'date': '2021-06-15',
                'time': '14:30:00',
                'tz': 'Europe/Zurich',
            },
            'endDate': {'date': '2021-06-15', 'time': '16:00:00', 'tz': 'Europe/Zurich'},
            'room': '',
            'metadata': True,
            'downloaded': True,
            'filename' : 'metadata.json',
            'localpath': 'data/meta/metadata.json',
        },
    ]

    # Parsing the metadata to get the list of files
    result_files = indico_pipeline.parse_metadata(
        os.path.join(os.path.dirname(__file__), "files", "indico_mock_meta.txt")
    )

    assert result_files == indico_files


def test_api_response1():
    indico_pipeline = indico.IndicoV1Pipeline("https://indico.cern.ch/")

    # Reading the expected results from file
    with open(
        os.path.join(os.path.dirname(__file__), "files", "indico_mock_meta.txt")
    ) as f:
        indico_metadata = f.read()
        f.close()

    # Get metadata for ID 1024767
    (
        metadata,
        metadata_url,
        status_code,
        metadata_filename,
    ) = indico_pipeline.get_metadata(1024767)

    indico_metadataJSON = json.loads(indico_metadata)
    metadata_JSON = json.loads(metadata)
    metadata_JSON.pop("ts")
    indico_metadataJSON.pop("ts")

    # Getting metadata and checking the results
    assert metadata_JSON == indico_metadataJSON
    assert (
        metadata_url
        == "https://indico.cern.ch/export/event/1024767.json?detail=contributions&occ=yes&pretty=yes"
    )
    assert status_code == 200
    assert metadata_filename == "metadata.json"


def test_api_response2():
    indico_pipeline = indico.IndicoV1Pipeline("https://indico.cern.ch/")

    # Reading the expected results from file
    with open(
        os.path.join(os.path.dirname(__file__), "files", "indico_mock_zero.txt")
    ) as f:
        indico_metadata = f.read()
        f.close()

    # Get metadata for a file that doesn't exist
    with pytest.raises(indico.RecidException) as recid_exc:
        (
            metadata,
            metadata_url,
            status_code,
            metadata_filename,
        ) = indico_pipeline.get_metadata(10797899777)
    assert recid_exc.type is indico.RecidException

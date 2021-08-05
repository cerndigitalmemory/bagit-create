from ..pipelines import invenio_v3
from unittest import mock
import json, os


def test_parse_zenodo_metadata():
    zenodo_pipeline = invenio_v3.InvenioV3Pipeline("zenodo")

    # Reading the mock metadata and expected result from file
    with open(
        os.path.join(os.path.dirname(__file__), "files", "zenodo_mock_data.txt")
    ) as f:
        data = f.read()
        data = data.split("###")

    zenodo_metadata = json.loads(data[0])
    zenodo_files = json.loads(data[1])

    # Assigning necessary instance variables
    zenodo_pipeline.metadata = zenodo_metadata
    zenodo_pipeline.aic_name = "3974864::1"
    zenodo_pipeline.base_path = "base_path"
    zenodo_pipeline.metadata_url = "https://zenodo.org/api/records/3974864"
    zenodo_pipeline.metadata_size = 1

    # Parsing the metadata to get the list of files
    zenodo_result_files = zenodo_pipeline.parse_metadata("")
    assert zenodo_result_files == zenodo_files


def test_parse_inveniordm_metadata():
    rdm_pipeline = invenio_v3.InvenioV3Pipeline("inveniordm")

    # Reading the mock metadata and expected result from file
    with open(
        os.path.join(os.path.dirname(__file__), "files", "inveniordm_mock_data.txt")
    ) as f:
        data = f.read()
        data = data.split("###")

    rdm_metadata = json.loads(data[0])
    rdm_files = json.loads(data[1])

    # Assigning necessary instance variables
    rdm_pipeline.metadata = rdm_metadata
    rdm_pipeline.aic_name = "gjgvm-4mq98::1"
    rdm_pipeline.base_path = "base_path"
    rdm_pipeline.metadata_url = "https://inveniordm.web.cern.ch/api/records/gjgvm-4mq98"
    rdm_pipeline.metadata_size = 1

    # Mocking the request to get the list of files to the record
    with mock.patch(
        "bagit_create.pipelines.invenio_v3.InvenioV3Pipeline.get_fileslist"
    ) as mock_query_get_files:
        mock_query_get_files.return_value = [
            {
                "bucket_id": "fecbe7c9-c751-4ca0-bcfc-7b264f9d207e",
                "checksum": "md5:2c0a8156137877bc84f4962d45e21a45",
                "created": "2021-07-23T13:40:32.675784+00:00",
                "file_id": "8f494ae2-42ee-42fa-9757-77d15e0906e0",
                "key": "1911.00295.pdf",
                "links": {
                    "content": "https://inveniordm.web.cern.ch/api/records/gjgvm-4mq98/files/1911.00295.pdf/content",
                    "self": "https://inveniordm.web.cern.ch/api/records/gjgvm-4mq98/files/1911.00295.pdf",
                },
                "metadata": None,
                "mimetype": "application/pdf",
                "size": 301875.0,
                "status": "completed",
                "storage_class": "S",
                "updated": "2021-07-23T13:40:32.684472+00:00",
                "version_id": "3df10a1e-718c-480a-8990-39206a26a790",
            }
        ]

        # Parsing the metadata to get the list of files
        rdm_result_files = rdm_pipeline.parse_metadata("")
        assert rdm_result_files == rdm_files

from ..pipelines import invenio_v3
import json, os


def test_api_zenodo_metadata():
    zenodo_pipeline = invenio_v3.InvenioV3Pipeline("zenodo")

    # Reading the expected results from file
    with open(
        os.path.join(os.path.dirname(__file__), "files", "zenodo_api_data.txt")
    ) as f:
        data = f.read()
        data = data.split("###")

    zenodo_metadata = json.loads(data[0])
    zenodo_files = json.loads(data[1])

    (
        metadata,
        metadata_url,
        status_code,
        metadata_filename,
    ) = zenodo_pipeline.get_metadata(3974864)

    # Removing statistics from metadata
    metadata.pop("stats")
    zenodo_metadata.pop("stats")

    # Getting metadata and checking the results
    assert metadata == zenodo_metadata
    assert metadata_url == "https://zenodo.org/api/records/3974864"
    assert status_code == 200
    assert metadata_filename == "metadata.json"

    # Assigning some necessary instance variables
    zenodo_pipeline.aic_name = "3974864::1"
    zenodo_pipeline.base_path = "base_path"

    # Parsing the metadata to get the list of files
    files = zenodo_pipeline.parse_metadata("")

    # Removing metadata.json file size from filelist
    for file in zenodo_files:
        if file["filename"] == "metadata.json":
            file.pop("size")

    for file in files:
        if file["filename"] == "metadata.json":
            file.pop("size")

    assert files == zenodo_files


"""
def test_api_inveniordm_metadata():
    rdm_pipeline = invenio_v3.InvenioV3Pipeline("inveniordm")

    # Reading the expected results from file
    with open(
        os.path.join(os.path.dirname(__file__), "files", "inveniordm_api_data.txt")
    ) as f:
        data = f.read()
        data = data.split("###")

    rdm_metadata = json.loads(data[0])
    rdm_files = json.loads(data[1])

    # Getting metadata and checking the results
    metadata, metadata_url, status_code, metadata_filename = rdm_pipeline.get_metadata(
        "gjgvm-4mq98"
    )

    assert metadata == rdm_metadata
    assert metadata_url == "https://inveniordm.web.cern.ch/api/records/gjgvm-4mq98"
    assert status_code == 200
    assert metadata_filename == "metadata.json"

    # Assigning some necessary instance variables
    rdm_pipeline.aic_name = "gjgvm-4mq98::1"
    rdm_pipeline.base_path = "base_path"

    # Parsing the metadata to get the list of files
    files = rdm_pipeline.parse_metadata("")
    assert files == rdm_files
"""

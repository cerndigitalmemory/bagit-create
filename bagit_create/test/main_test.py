from ..pipelines import invenio_v1
from ..pipelines import base
import tempfile
import ntpath, os
from os import walk
import shutil

pipeline = invenio_v1.InvenioV1Pipeline("https://some/invenio/v1/instance", recid=1)

a = [{"filename": "42.txt", "path": "8"}, {"filename": "47.txt", "path": "/opt/47"}]
b = [
    {"filename": "42.txt", "path": "/opt/42", "hash": "0"},
    {"filename": "43.txt", "path": "/opt/43", "hash": "4"},
]

c1 = [
    {"filename": "42.txt", "path": "8"},
    {"filename": "47.txt", "path": "/opt/47"},
    {"filename": "43.txt", "path": "/opt/43", "hash": "4"},
]

c2 = [
    {"filename": "42.txt", "path": "/opt/42", "hash": "0"},
    {"filename": "43.txt", "path": "/opt/43", "hash": "4"},
    {"filename": "47.txt", "path": "/opt/47"},
]


def test_mergelists():
    assert pipeline.merge_lists(a, b, "filename") == c1


def test_mergelists_inverted():
    assert pipeline.merge_lists(b, a, "filename") == c2


def test_target_option():

    src = "/tmp/test_temp_folder_1"
    dest = "/tmp/destination_temp_folder"

    try:
        os.mkdir(src)
    except:
        shutil.rmtree(src)
        os.mkdir(src)
    os.mkdir(f"{src}/test_temp_folder_2")
    f1 = open(f"{src}/test_temp_file_1", "w")
    f2 = open(f"{src}/test_temp_folder_2/test_temp_file_2", "w")

    """
    Temp directory structure
    - tmpdir1
        - f1
        - tmpdir2
            - f2
    We want to check if the two files will be moved at the destination folder
    """

    f1.close()
    f2.close()

    # Make destination temp folder
    try:
        os.mkdir(dest)
    except:
        shutil.rmtree(dest)
        os.mkdir(dest)

    try:
        # Run the move folders function
        pipeline = base.BasePipeline()
        pipeline.move_folders(src, "test_temp_folder_1", dest)

        # Create two flags to assert the test
        file_1_exists = False
        file_2_exists = False

        # Check if the folders have been moved to the target directory
        # No need to check if the directories have been created. We do it by checking the folders

        if os.path.isfile(f"{dest}/test_temp_folder_1/test_temp_file_1"):
            file_1_exists = True
        if os.path.isfile(
            f"{dest}/test_temp_folder_1/test_temp_folder_2/test_temp_file_2"
        ):
            file_2_exists = True
    except:
        shutil.rmtree(dest)
        shutil.rmtree(src)

    # Clear src and destination folders
    shutil.rmtree(dest)
    shutil.rmtree(src)

    assert file_1_exists == True
    assert file_2_exists == True

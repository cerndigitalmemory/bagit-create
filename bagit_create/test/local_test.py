from .. import main
import tempfile
import os, pytest
from oais_utils.validate import validate_sip


def test_local_files():

    # Prepare the mock folders and expected result from file
    with tempfile.TemporaryDirectory() as tmpdir1:
        with tempfile.TemporaryDirectory(dir=tmpdir1) as tmpdir2:
            with tempfile.TemporaryDirectory() as tmpdir3:
                # Creates two temp directories and two files
                f1 = tempfile.NamedTemporaryFile("w+t", dir=tmpdir1)
                f1.seek(0)

                f2 = tempfile.NamedTemporaryFile("w+t", dir=tmpdir2)
                f2.seek(0)

                """
                Temp directory structure
                - tmpdir1
                   - f1
                   - tmpdir2
                       - f2
                We want to run BagitCreate on tmpdir1 and then validate the resulting folder with the oais_utils.validate tool             
                """

                # Run Bagit Create with the following parameters:
                # Save the results to tmpdir3
                main.process(
                    recid=None,
                    source="local",
                    loglevel=0,
                    target=tmpdir3,
                    source_path=tmpdir1,
                    author="python-test",
                )

                # Check inside the tmpdir3 for any folders. If it finds one, this will be the folder created by Bagit Create.
                target_sip_list = os.listdir(tmpdir3)

                # Run validate_sip on the folder that was found
                valid_structure = validate_sip(os.path.join(tmpdir3, target_sip_list[0]))

                f1.close()
                f2.close()

    assert valid_structure == True

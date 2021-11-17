from ..pipelines import indico
from .. import main
import tempfile
import os, pytest, json
from oais_utils.validate import validate_sip


def test_indico_results():
    # Prepare a temporary folder to save the results
    with tempfile.TemporaryDirectory() as tmpdir1:

        # Run Bagit Create with the following parameters:
        # Save the results to tmpdir1
        main.process(
            recid=1024767,
            source="indico",
            loglevel=0,
            target=tmpdir1,
        )

        # Check inside the tmpdir1 for any folders. If it finds one, this will be the folder created by Bagit Create.
        target_sip_list = os.listdir(tmpdir1)

        # Run validate_sip on the folder that was found
        valid_structure = validate_sip(os.path.join(tmpdir1, target_sip_list[0]))

    assert valid_structure == True

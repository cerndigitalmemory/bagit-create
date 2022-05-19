import os
import tempfile

from oais_utils.validate import validate_sip

from .. import main

"""
All these tests cover different supported pipelines.
Based on the parameters given at the test_variables,
each function calls the pipeline_results() which runs bagit-create and
validates the result using the oais_utils.validate method and returns True or False.
"""


def test_codimd_pipeline():
    test_variables = {
        "source": "codimd",
        # This note should be set as EDITABLE and be PUBLISHED or it
        # won't be accessible without a token
        "recid": "tBD632vFt",
        "dry_run": False,
    }
    valid = pipeline_results(
        test_variables["source"],
        test_variables["recid"],
        test_variables["dry_run"],
    )
    assert valid is True


def test_indico_pipeline():
    # So we can keep INDICO_KEY uncommitted and read it from an environment variable set for the ci/cd job
    token = os.environ["INDICO_KEY"]
    test_variables = {
        "source": "indico",
        "recid": 1024767,
        "dry_run": False,
        "token": token,
    }
    valid = pipeline_results(
        test_variables["source"],
        test_variables["recid"],
        test_variables["dry_run"],
        test_variables["token"],
    )
    assert valid is True


def test_cds_pipeline():
    test_variables = {"source": "cds", "recid": 2728246, "dry_run": True}
    valid = pipeline_results(
        test_variables["source"], test_variables["recid"], test_variables["dry_run"]
    )
    assert valid is True


"""
def test_cds_pipeline():
    test_variables = {"source": "ilcdoc", "recid": 62959, "dry_run": True}
    valid = pipeline_results(
        test_variables["source"], test_variables["recid"], test_variables["dry_run"]
    )
    assert valid == True
"""


def test_zenodo_pipeline():
    test_variables = {"source": "zenodo", "recid": 3911261, "dry_run": True}
    valid = pipeline_results(
        test_variables["source"], test_variables["recid"], test_variables["dry_run"]
    )
    assert valid is True


def test_cod_pipeline():
    test_variables = {"source": "cod", "recid": 10101, "dry_run": True}
    valid = pipeline_results(
        test_variables["source"], test_variables["recid"], test_variables["dry_run"]
    )
    assert valid is True


"""
def test_inveniordm_pipeline():
    test_variables = {"source": "inveniordm", "recid": "v3vqp-bfg07", "dry_run": True}
    valid = pipeline_results(
        test_variables["source"], test_variables["recid"], test_variables["dry_run"]
    )
    assert valid == True
"""


def pipeline_results(source, recid, dry_run, token=None):
    # Prepare a temporary folder to save the results
    with tempfile.TemporaryDirectory() as tmpdir1:

        # Run Bagit Create with the following parameters:
        # Save the results to tmpdir1
        main.process(
            recid=recid,
            source=source,
            loglevel=0,
            target=tmpdir1,
            dry_run=dry_run,
            token=token,
        )

        # Check inside the tmpdir1 for any folders. If it finds one, this will be the folder created by Bagit Create.
        target_sip_list = os.listdir(tmpdir1)

        # Run validate_sip on the folder that was found
        valid_structure = validate_sip(os.path.join(tmpdir1, target_sip_list[0]))

    return valid_structure

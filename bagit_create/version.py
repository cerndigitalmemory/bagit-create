import os
import subprocess

# Change this when releasing a new version
# The version setuptools will read
__version__ = "1.0.0"


def get_commit_hash():
    """
    This function tries to run git from the parent directory of this file,
    (which should be the main repository folder of bagit-create) to get the current commit hash.
    This will return a short git commit hash if BIC is being executed from a local git copy,
    None otherwise (e.g. when installing a released version from PyPi)
    """
    try:
        bagit_base_path = os.path.dirname(__file__)
        git_output = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"], cwd=bagit_base_path
        )
        short_hash = git_output.decode().rstrip()
    except Exception:
        short_hash = None
    return short_hash


def get_version():
    """
    Returns a string like `bic, version 0.1.9 git.47278a2` when running from a git cloned copy of BIC
    or simply `bic, version 0.1.9` when running from a released version of the tool
    """
    git_commit = get_commit_hash()

    if git_commit is not None:
        return f"{__version__}-git.{git_commit}"
    else:
        return __version__


# This is read and exposed by the CLI --version
complete_version = get_version()

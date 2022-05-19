import os
import pathlib
import re

from setuptools import find_packages, setup

# The directory containing this file
HERE = pathlib.Path(__file__).parent

# The text of the README file
README = (HERE / "README.md").read_text()

# Get the version string. Cannot be done with import!
with open(os.path.join("bagit_create", "version.py"), "rt") as f:
    version = re.search(r'__version__\s*=\s*"(?P<version>.*)"\n', f.read()).group(
        "version"
    )

# This call to setup() does all the work
setup(
    name="bagit-create",
    version=version,
    description="Create BagIt packages harvesting data from upstream sources",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://gitlab.cern.ch/digitalmemory/bagit-create",
    author="Antonio Vivace",
    author_email="antonio.vivace@cern.ch",
    license="GPLv3",
    classifiers=[
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
    packages=find_packages(include=["bagit_create", "bagit_create.*"]),
    include_package_data=True,
    install_requires=[
        "appdirs==1.4.4",
        "cernopendata-client==0.2.0",
        "certifi==2020.12.5",
        "chardet==4.0.0",
        "click>=7",
        "flake8==3.9.0",
        "fs==2.4.12",
        "idna==2.10",
        "mccabe==0.6.1",
        "pycodestyle==2.7.0",
        "pyflakes==2.3.0",
        "pymarc==4.0.0",
        "pytz==2021.3",
        "requests==2.25.1",
        "six==1.15.0",
        "urllib3==1.26.4",
        "bagit==1.8.1",
        "checksumdir==1.2.0",
        "jsonschema==3.0.2",
        "python-cern-sso==1.3.3",
        "python-slugify==6.1.2",
    ],
    entry_points={
        "console_scripts": [
            "bic=bagit_create.cli:cli",
        ]
    },
)

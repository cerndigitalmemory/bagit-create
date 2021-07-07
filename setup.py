import pathlib
from setuptools import setup, find_packages

# The directory containing this file
HERE = pathlib.Path(__file__).parent

# The text of the README file
README = (HERE / "README.md").read_text()

# This call to setup() does all the work
setup(
    name="bagit-create",
    version="0.0.2",
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
        "Programming Language :: Python :: 3.6",
    ],
    packages=find_packages(include=["bagit_create", "bagit_create.*"]),
    include_package_data=True,
    install_requires=[
        "appdirs==1.4.4",
        "cernopendata-client==0.2.0",
        "certifi==2020.12.5",
        "chardet==4.0.0",
        "click==7.1.2",
        "flake8==3.9.0",
        "fs==2.4.12",
        "idna==2.10",
        "mccabe==0.6.1",
        "pycodestyle==2.7.0",
        "pyflakes==2.3.0",
        "pymarc==4.0.0",
        "pytz==2021.1",
        "requests==2.25.1",
        "six==1.15.0",
        "urllib3==1.26.4",
    ],
    entry_points={
        "console_scripts": [
            "bic=bagit_create.cli:cli",
        ]
    },
)

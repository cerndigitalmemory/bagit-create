# bagit-create

[![PyPI version](https://badge.fury.io/py/bagit-create.svg)](https://pypi.org/project/bagit-create/)

Python module and CLI tool to prepare SIPs (according to the [CERN SIP specification](https://gitlab.cern.ch/digitalmemory/sip-spec)), harvesting metadata and files from various souces, such as digital repositories powered by Invenio software.


## Install

If you just need to run BagIt Create from the command line:

```bash
# Install from PyPi
pip install bagit-create

# Check installed version
bic --version
```

For development, you can clone this repository and then install it with the `-e` flag:

```bash
# Clone the repository
git clone https://gitlab.cern.ch/digitalmemory/bagit-create
cd bagit-create

# Create a virtual environment and activate it
python -m venv env
source env/bin/activate

# Install the project in editable mode
pip install -e .

# Check installed version
bic --version

# Run tests
python -m pytest
```

## Usage

### Supported sources

Three pipelines (Invenio 1.x, Invenio 3.x, CERN Open Data) are currently implemented, supporting the following digital repositories:

| Name                  | ID           | URL                                | Pipeline                    |
|---------------------- |--------------|------------------------------------|-----------------------------|
| CERN Document Server  | cds          | https://cds.cern.ch/               | Invenio v1.x                |
| ILC Document Server   | ilcdoc       | http://ilcdoc.linearcollider.org   | Invenio v1.x                |
| CERN Open Data        | cod          | https://opendata.cern.ch/          | CERN Open Data              |
| (Generic) Invenio v3  | inv3         |                                    | Invenio v3.x\*              |    
| Zenodo                | zenodo       | https://zenodo.org/                | Invenio v3.x                |
| (Generic) InvenioRDM  | invenio-rdm  |                                    | Invenio v3.x\*              |

\* requires additional configuration in `invenio.ini`

### CLI

```bash
# CDS
bic --recid 1 --source cds --dry-run
bic --recid 1000 --source cds --dry-run
1000571

# CERN Open Data
1
bic --recid 8884 --source cod --dry-run --verbose --alternate-uri
bic --recid 8884 --source cod --dry-run --verbose
5200
bic --recid 8888 --source cod --dry-run --verbose

bic --recid 10101 --source cod --dry-run --verbose
bic --recid 10102 --source cod --dry-run --verbose
bic --recid 10103 --source cod --dry-run --verbose
bic --recid 10104 --source cod --dry-run --verbose
bic --recid 10105 --source cod --dry-run --verbose

bic --recid 10101 --source cod --verbose
bic --recid 10102 --source cod --verbose
bic --recid 10103 --source cod --verbose
bic --recid 10104 --source cod --verbose
bic --recid 10105 --source cod --verbose

# Zenodo
bic --recid 3911261 --source zenodo --verbose
bic --recid 3974864 --source zenodo --verbose

# Invenio
bic --recid gjgvm-4mq98 --source inveniordm --verbose
```

CLI options:

```
Options:
--version                       Show the version and exit.
--recid TEXT                    Unique ID of the record in the upstream
                                  source  [required]

-s, --source [cds|ilcdoc|cod|zenodo|inveniordm]
                                  Select source pipeline  [required]
-d, --dry-run                   Skip downloads
-a, --alternate-uri             Use alternative uri instead of https for
                                  fetch.txt (e.g. root endpoints  for CERN
                                  Open Data instead of http)

-v, --verbose                   Enable logging (verbose, 'info' level)
-vv, --very-verbose             Enable logging (very verbose, 'debug' level)
-b, --bibdoc                    Get metadata for a CDS record from the
                                  bibdocfile utility.
                                  (`/opt/cdsweb/bin/bibdocfile` must be
                                  available in the system and the resource
                                  must be from CDS). See [bibdocfile](#bibdocfile).

--bd-ssh-host <HOSTNAME>        SSH host to run bibdocfile. See [bibdocfile](#bibdocfile).
--help                          Show this message and exit.
```

### Module

The BagIt-Create tool can be used from other python scripts easily:

```python
from bagit_create.main import process

process(recid=2272168, source="cds")
```

### bibdocfile

The `bibdocfile` command line utility can be used to get metadata for CDS, exposing internal file paths and hashes normally not available through the CDS API.

If the executable is available in the path (i.e. you can run `/opt/cdsweb/bin/bibdocfile`) just append `--bibdoc`:

```bash
bic --recid 2751237 --source cds --bibdoc -v
```

If this is not the case, you can pass a `--bd-ssh-host` parameter specifying the name of an SSH configured connection pointing to a machine able to run the command for you. Be aware that your machine must be able to establish such connection without any user interaction (the script will run `ssh <THE_PROVIDED_SSH_HOST> bibdocfile ..args`).

Since in a normal CERN scenario this can't be possible due to required ProxyJumps/OTP authentication steps, you can use the `ControlMaster` feature of any recent version of OpenSSH, allowing to reuse sockets for connecting:

Add an entry in `~/.ssh/config` to set up the SSH connection to the remote machine able to run `bibdocfile` for you in the following way:

```bash
Host <SSH_NAME>
  User <YOUR_USER>
  Hostname <HOSTNAME.cern.ch>
  ProxyJump <LXPLUS_or_AIADM>
  ControlMaster auto
  ControlPath ~/.ssh/control:%h:%p:%r
```

Then, run `ssh <SSH_NAME>` in a shell, authenticate and keep it open. OpenSSH will now reuse this socket everytime you run `<SSH_NAME>`, allowing BagItCreate tool to run `bibdocfile` over this ssh connection for you, if you pass the `bd-ssh-host` parameter:

```bash
bic --recid 2751237 --source cds --bibdoc --bd-ssh-host=<SSH_NAME> -v
``` 

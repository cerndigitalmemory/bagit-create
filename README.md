# bagit-create

Proof of concept utilities, scripts and pipelines to prepare BagIt ([RFC](https://tools.ietf.org/html/rfc8493)) files, following the CERN Archival Information Packages (AIP) [specification](https://digital-repositories.web.cern.ch/digital-repositories/dm/oais-platform/cern-aips/), ready for Archivematica ingestions.

Data is taken from various upstream sources, such as CDS (CERN Document Service) and CERN Open Data.

```bash
# Install python3.8 and pipenv
yum install python3.8 libcurl-devel
pip3 install pipenv
# GCC, ..
dnf group install "Development Tools"
# Headers
yum install python38-devel openssl-devel

# Create and activate virtualenv
python3 -m venv env
source env/bin/activate
# Install dependencies
cd bagit_create
pip3 install -r requirements.txt 
```

## Usage

### CLI

```bash
# Show CLI Usage help
./cli.py --help

./cli.py --recid=2272168 --source=cds

# Generate JSON metadata for arkivum, running in a very verbose way
./cli.py --recid 2766073 --source cds --ark-json -vv

# Deleted resource, running in a very verbose way
./cli.py --recid 1 --source cds -vv

# Tests
pytest
```

CLI options:

- `--recid TEXT`, Unique ID of the record in the upstream source [required]
- `--source [cds|ilcdoc|cod]`, Select source pipeline  [required]
- `--skip-downloads`, Creates files but skip downloading the actual payloads
- `--ark-json`, Generate a JSON metadata file for arkivum ingestions
- `--ark-json-rel`, Generate a JSON metadata file for arkivum ingestions using relative paths
- `-v`, Enable logging (verbose, 'info' level)
- `-vv`, Enable logging (very verbose, 'debug' level)
- `-b`,`--bibdoc`, Get metadata for a CDS record from the bibdocfile utility.
	(`/opt/cdsweb/bin/bibdocfile` must be available in the system and the resource must be from CDS).
	See [bibdocfile](#bibdocfile).
-  `--bd-ssh-host TEXT`, SSH host to run bibdocfile. See [bibdocfile](#bibdocfile).

### Module

The BagIt-Create tool can be used from other python scripts easily:

```python
from bagit_create.main import process

process(recid=2272168, source="cds")
```


### Supported sources

| Name                 	| ID     	| URL                              	| Notes                     	|
|----------------------	|--------	|----------------------------------	|---------------------------	|
| CERN Document Server 	| cds    	| https://cds.cern.ch/             	| Invenio v1.1.3.1106-62468 	|
| ILC Document Server  	| ilcdoc 	| http://ilcdoc.linearcollider.org 	| CDS Invenio v1.0.7.2-5776 	|
| CERN Open Data       	| cod    	| https://opendata.cern.ch/        	|                           	|

### CERN Document Server (CDS)

To prepare a BagIt from a CDS Resource ID, using the CLI interface, run `python cli.py --recid=2272168 --source=cds`

```
> tree bagitexport_2272168
bagitexport_2272168
├── bagit.txt
├── 2272168_1605200583
│   ├── metadata.xml
│   └── references.txt
└── 2272168_bacc9427609e6509f172e6b2604659d6jfkob
    └── 2272168.mp4

2 directories, 3 files
```

CDS metadata is XML/[MARC21](https://cds.cern.ch/help/admin/howto-marc?ln=fr)

### bibdocfile

The `bibdocfile` command line utility can be used to get metadata for CDS, exposing internal file paths and hashes normally not available through the CDS API.

If the executable is available in the path (i.e. you can run `/opt/cdsweb/bin/bibdocfile`) just append `--bibdoc`:

```bash
python3 cli.py --recid 2751237 --source cds --ark-json --bibdoc -v
```

If this is not the case, you can pass a `--bd-ssh-host` parameter specifying the name of an SSH configured connection pointing to a machine able to run the command for you. Be aware that your machine must be able to establish such connection without any user interaction (the script will run `ssh <THE_PROVIDED_SSH_HOST> bibdocfile ..args`).

Since in a normal CERN scenario this can't be possible due to required ProxyJumps/OTP authentication steps, you can use the `ControlMaster` feature of any recent version of OpenSSH, allowing to reuse sockets for connecting:

Add an entry in `~/.ssh/config` to set up the SSH connection to the remote machine able to run `bibdocfile` for you in the following way:

```
Host <SSH_NAME>
  User <YOUR_USER>
  Hostname <HOSTNAME.cern.ch>
  ProxyJump <LXPLUS_or_AIADM>
  ControlMaster auto
  ControlPath ~/.ssh/control:%h:%p:%r
```

Then, run `ssh <SSH_NAME>` in a shell, authenticate and keep it open. OpenSSH will now reuse this socket everytime you run `<SSH_NAME>`, allowing BagItCreate tool to run `bibdocfile` over this ssh connection for you, if you pass the `bd-ssh-host` parameter:

```bash
./cli.py --recid 2751237 --source cds --ark-json --bibdoc --bd-ssh-host=<SSH_NAME> -v
``` 


### CERN Open Data

To prepare a BagIt from a CERN Open Data Record ID, run `./cli.py --recid 1 --source cod`.

CERN Open Data metadata follows [this](http://opendata.cern.ch/schema/records/record-v1.0.0.json) schema.

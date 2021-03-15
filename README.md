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

# Activate virtualenv
pipenv shell
# Install dependencies
pipenv install

# CLI Help
python main.py --help
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

### CERN Open Data

To prepare a BagIt from a CERN Open Data Record ID, run `python cli.py --foldername=1 --method=cod`.

CERN Open Data metadata follows [this](http://opendata.cern.ch/schema/records/record-v1.0.0.json) schema.

### As a module

The BagIt-Create tool can be used from other python scripts easily:

```python
import main

main.process(recid=2272168, source="cds")
```
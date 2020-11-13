# bagit-create

Proof of concept utilities, scripts and pipelines to prepare BagIt ([RFC](https://tools.ietf.org/html/rfc8493)) files, following the CERN Archival Information Packages (AIP) [specification](https://digital-repositories.web.cern.ch/digital-repositories/dm/oais-platform/cern-aips/), ready for Archivematica ingestions.

Data is taken from various upstream sources, such as CDS (CERN Document Service) and CERN Open Data.

```bash
# Activate virtualenv
pipenv shell
# Install dependencies
pipenv install

# CLI Help
python main.py --help
```

### CDS

To prepare a BagIt from a CDS Resource ID, run `python main.py --foldername=2272168 --method=cds`.

```
> tree bagitexport_2272168
bagitexport_2272168
├── 2272168_1605200583
│   ├── metadata.xml
│   └── references.txt
└── 2272168_bacc9427609e6509f172e6b2604659d6jfkob
    └── 2272168.mp4

2 directories, 3 files
```

CDS metadata is XML/[MARC21](https://cds.cern.ch/help/admin/howto-marc?ln=fr)

### Cern Open Data

To prepare a BagIt from a CERN Open Data Record ID, run `python main.py --foldername=1 --method=cod`.

CERN Open Data metadata follows [this](http://opendata.cern.ch/schema/records/record-v1.0.0.json) schema.
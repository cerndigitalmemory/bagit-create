# bagit-create

Utilities to prepare BagIt files for Archivematica ingestions from various upstream sources, such as CERN CDS and CERN Open Data.

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
│			  ├── metadata.xml
│			  └── references.txt
└── 2272168_bacc9427609e6509f172e6b2604659d6jfkob
    └── 2272168.mp4

2 directories, 3 files
```

### Cern Open Data

To prepare a BagIt from a CERN Open Data recid, run `python main.py --foldername=1 --method=cod`.
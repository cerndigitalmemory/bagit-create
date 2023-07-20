from urllib.parse import urlparse

from sickle import Sickle

import bagit_create

sickle = Sickle("https://zenodo.org/oai2d")

""" Harvest the entire repository """
records = sickle.ListRecords(metadataPrefix="oai_dc", set="user-tops")
record = records.next()

failed = []
successful = []
ids = []

while record:
    url = record.metadata["identifier"][0]
    recid = urlparse(url).path.rpartition("/")[2]
    ids.append(recid)
    try:
        record = records.next()
    except Exception:
        record = None


print(f"Final list of ids to process: {ids} \n")

for recid in ids:
    sip = bagit_create.main.process(
        source="zenodo",
        recid=recid,
        loglevel=0,
        target="zenodo_user-tops",
    )
    if sip["status"] != 0:
        failed.append(recid)
    else:
        successful.append(recid)


print(f"Success: {successful} \n")
print(f"Failed: {failed} \n")

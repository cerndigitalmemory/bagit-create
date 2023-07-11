"""
An example recipe to harvest and create SIP for every record in a given Zenodo community
Providing a token will allow more requests before getting ratelimited
See also https://developers.zenodo.org/
"""

from urllib.parse import urlparse

from sickle import Sickle

import bagit_create

sickle = Sickle("https://zenodo.org/oai2d")

""" Fetch records from the OAI Set of the "tops" community """
records = sickle.ListRecords(metadataPrefix="oai_dc", set="user-tops")
record = records.next()

while record:
    url = record.metadata["identifier"][0]
    recid = urlparse(url).path.rpartition("/")[2]

    print("RECID:", recid)
    sip = bagit_create.main.process(source="zenodo", recid=recid, loglevel=0)
    if sip["status"] != 0:
        print("Something went wrong")
    else:
        print("ok")
    # Let's get the next record
    record = records.next()

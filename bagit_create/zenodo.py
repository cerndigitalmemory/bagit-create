import requests, json
import logging
import re
import hashlib
from .cds import prettyprint

SERVER_HTTP_URI = "https://zenodo.org/api/"
headers = {"Content-Type": "application/json"}
"""Default Zenodo server to query over HTTP protocol."""

def getMetadata(recid):
    res = requests.get(SERVER_HTTP_URI+"/records/" + str(recid),headers=headers)

    if res.status_code != 200:
        logging.error("Recid does not exist in Invenio instance")
    else:
        return json.loads(res.text)

def verifyChecksum(checksum, file):
    p = re.compile('([A-Za-z0-9]*):([A-Za-z0-9]*)')
    m = p.match(checksum)
    alg = m.groups()[0].lower()
    checksum = m.groups()[1]

    current_checksum = hashlib.md5(file).hexdigest()

    logging.debug(f'Original checksum: {alg}:{checksum}, Calculated checksum: {alg}:{current_checksum}')

    return current_checksum == checksum

def downloadFile(source, dest, checksum):
    r = requests.get(source,headers=headers)
    
    if not verifyChecksum(checksum, r.content):
        logging.error("File checksum does not match: " + source)
    else:
        with open(dest, "wb+") as file:
            file.write(r.content)
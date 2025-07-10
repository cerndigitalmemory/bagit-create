"""
This is an example on how to harvest your entire codimd history
Set the CODIMD_SESSION environment variable to the value of the `connect.sid_value`
cookie from your codimd authenticated session
To learn more about this: https://gitlab.cern.ch/digitalmemory/bagit-create#codimd
"""

import logging
import os
import time

import requests

import bagit_create

session_id = os.environ["CODIMD_SESSION"]
max_attempts = 5

r = requests.get(
    "https://codimd.web.cern.ch/history",
    stream=True,
    cookies={"connect.sid": session_id},
)

data = r.json()["history"][2:]

print(f"Found {len(data)} notes in your history..")

for note in data:
    attempts = 0
    while True:
        attempts += 1
        print(f"Creating SIP for {note['text']} ({note['id']})..")
        result = bagit_create.main.process(
            source="codimd", recid=note["id"], token=session_id, loglevel=logging.WARNING,
        )
        if result["status"] == 0:
            break
        else:
            print("Waiting to try again..")
            time.sleep(5)
        if attempts == max_attempts:
            print("Giving up..")
            break
        print(result)

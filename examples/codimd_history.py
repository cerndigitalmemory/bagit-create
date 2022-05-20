import os
import time

import bagit_create
import requests

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
            source="codimd", recid=note["id"], token=session_id, loglevel=3
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

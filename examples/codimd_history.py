import bagit_create
import requests

session_id = "PLACE_YOUR_SESSION_ID_HERE"

r = requests.get(
    "https://codimd.web.cern.ch/history",
    stream=True,
    cookies={"connect.sid": session_id},
)

data = r.json()["history"][2:]

print(f"Found {len(data)} notes in your history..")

for note in data:
    print(f"Creating SIP for {note['text']} ({note['id']})..")
    result = bagit_create.main.process(
        source="codimd", recid=note["id"], token="session_id", loglevel=3
    )

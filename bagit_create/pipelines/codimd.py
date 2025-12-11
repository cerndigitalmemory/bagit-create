import logging
import re
import urllib.parse
from http import HTTPStatus

import requests
from slugify import slugify

from . import base

log = logging.getLogger("bic-basic-logger")


class CodimdPipeline(base.BasePipeline):
    def __init__(self, recid, token=None):
        super().__init__()
        self.manifest_algorithms = ["md5", "sha1"]
        self.connect_sid_token = token
        self.recid = recid

    def get_metadata(self, record_id, source):
        # We don't have any metadata fetch-able via exposed routes, so let's
        #  put just some basic information
        return (
            {"record_id": record_id},
            "none",
            HTTPStatus.OK,
            f"codimd-{record_id}.json",
        )

    def parse_metadata(self, metadata):
        # Let's create an empty file object,
        #  we will put the file name after having downloaded it
        files = [{"downloaded": True}]

        # File entry for the metadata file
        meta_file_entry = {
            "origin": {
                "filename": f"codimd-{self.recid}.json",
                "path": "",
                "url": "",
            },
            "metadata": True,
            "downloaded": True,
            "bagpath": f"data/content/codimd-{self.recid}.json",
        }
        files.append(meta_file_entry)
        return (files, meta_file_entry)

    def download_files(self, files, base_path):
        r = requests.get(
            f"https://codimd.web.cern.ch/{self.recid}/download",
            stream=True,
            cookies={"connect.sid": self.connect_sid_token},
        )

        if r.status_code == HTTPStatus.NOT_FOUND:
            raise Exception("Note not found (404)")
        if r.status_code != HTTPStatus.OK:
            raise Exception("Connection Error to CodiMD")
        if "Content-Disposition" in r.headers.keys():
            downloaded_file_name = re.findall(
                "filename=(.+)", r.headers["Content-Disposition"]
            )[0]
            # Decode the urlencoded downloaded file name and slugify it
            #  (as it usually contains encoded entities, coming from the first
            #  H1 found inside the document)
            fname = slugify(urllib.parse.unquote(downloaded_file_name))
            # Remove "-md" and put back ".md" as extension
            fname = fname[:-3] + ".md"
        else:
            raise Exception("Header is missing..")
            fname = "document.md"

        with open(f"{base_path}/data/content/{fname}", "wb") as f:
            for chunk in r.raw.stream(1024, decode_content=False):
                if chunk:
                    f.write(chunk)
        files[0]["bagpath"] = f"data/content/{fname}"

        r = requests.get(
            f"https://codimd.web.cern.ch/{self.recid}/pdf",
            stream=True,
            cookies={"connect.sid": self.connect_sid_token},
        )

        with open(f"{base_path}/data/content/{fname[:-3]}.pdf", "wb") as f:
            for chunk in r.raw.stream(1024, decode_content=False):
                if chunk:
                    f.write(chunk)

        pdf_file_entry = {
            "downloaded": True,
            "bagpath": f"data/content/{fname[:-3]}.pdf",
        }

        files.append(pdf_file_entry)

        return files

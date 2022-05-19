import logging
import re

import requests

from . import base

log = logging.getLogger("bic-basic-logger")


class CodimdPipeline(base.BasePipeline):
    def __init__(self, recid, token=None):
        self.connect_sid_token = token
        self.recid = recid

    def get_metadata(self, record_id, source):
        # We don't have any metadata..
        return (
            {"record_id": record_id},
            "none",
            200,
            f"codimd-{record_id}.json",
        )

    def parse_metadata(self, metadata):
        # Let's create an empty file object, we will put the file name after having downloaded it
        files = [{"downloaded": False}]

        meta_file_entry = {
            "origin": {
                "filename": f"codimd-{self.recid}.json",
                "path": "",
                "url": "self.metadata_url",
            },
            "metadata": True,
            "downloaded": True,
            "bagpath": f"data/content/codimd-{self.recid}.json",
            "size": "1",
        }
        files.append(meta_file_entry)
        return (files, meta_file_entry)

    def download_files(self, files, base_path):

        r = requests.get(
            f"https://codimd.web.cern.ch/{self.recid}/download",
            stream=True,
            cookies={"connect.sid": self.connect_sid_token},
        )
        if "Content-Disposition" in r.headers.keys():
            fname = re.findall("filename=(.+)", r.headers["Content-Disposition"])[0]

        with open(f"{base_path}/data/content/{fname}", "wb") as f:
            for chunk in r.raw.stream(1024, decode_content=False):
                if chunk:
                    f.write(chunk)
        files[0]["bagpath"] = f"data/content/{fname}"
        return files

    def create_manifests(self, files, base_path):
        algs = ["md5", "sha1"]
        for alg in algs:
            log.info(f"Generating manifest {alg}..")
            content, files = self.generate_manifest(files, alg, base_path)
            self.write_file(content, f"{base_path}/manifest-{alg}.txt")
        return files

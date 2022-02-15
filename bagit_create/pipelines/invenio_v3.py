import configparser
import json
import logging
import os

import requests

from . import base

log = logging.getLogger("bic-basic-logger")


def get_dict_value(dct, keys):
    for key in keys:
        try:
            dct = dct[key]
        except KeyError:
            log.error("Key:" + key + " not found in dict: " + str(dct))
            return None
    return dct


"""Invenio V3 pipeline to query over HTTP protocol."""


class InvenioV3Pipeline(base.BasePipeline):
    def __init__(self, source):
        self.headers = {"Content-Type": "application/json"}
        self.response_type = "json"

        self.config_file = configparser.ConfigParser()
        self.config_file.read(os.path.join(os.path.dirname(__file__), "invenio.ini"))
        self.config = None

        if len(self.config_file.sections()) == 0:
            log.error("Could not read config file")

        for instance in self.config_file.sections():
            if instance == source:
                self.config = self.config_file[instance]
                self.base_endpoint = self.config["base_endpoint"]
                # Some instances have the file endpoint separately where the parameters are the filenames
                self.has_file_base_uri = self.config.getboolean("has_file_base_uri")

        log.info(f"Invenio v3 pipeline initialised.\nBase URL: {self.base_endpoint}")

        if not self.config:
            log.error("No such Invenio instance: " + source)

    def get_metadata(self, recid, source):
        res = requests.get(self.base_endpoint + str(recid), headers=self.headers)

        if res.status_code != 200:
            raise Exception(f"Metadata request gave HTTP {res.status_code}.")

        self.recid = recid
        self.metadata_url = res.url
        self.metadata = json.loads(res.text)
        self.metadata_size = len(res.content)
        return self.metadata, self.metadata_url, res.status_code, "metadata.json"

    def create_manifests(self, files, base_path):
        alg = "md5"
        log.info(f"Generating manifest {alg}..")
        content, files = self.generate_manifest(files, alg, base_path)
        self.write_file(content, f"{base_path}/manifest-{alg}.txt")
        return files

    def parse_metadata(self, metadata_filename):
        log.debug("Parsing metadata..")

        files = self.get_fileslist()
        files_obj = []

        # Records with only metadata have the files key set to None
        if files:
            for sourcefile in files:
                filename = self.get_filename(sourcefile)
                if self.has_file_baseuri():
                    file_uri = self.get_file_baseuri()
                    url = file_uri + "/" + filename
                else:
                    url = self.get_file_uri(sourcefile)
                file_obj = {
                    "origin": {"url": url, "filename": filename, "path": ""},
                    "downloaded": False,
                    "metadata": False,
                    "bagpath": f"data/content/{filename}",
                    "checksum": sourcefile["checksum"],
                    "size": sourcefile["size"],
                }
                files_obj.append(file_obj)

            log.debug(f"Got {len(files)} files")

        meta_file_entry = {
            "origin": {
                "filename": "metadata.json",
                "path": "",
                "url": self.metadata_url,
            },
            "metadata": True,
            "downloaded": True,
            "bagpath": "data/content/metadata.json",
            "size": self.metadata_size,
        }

        files_obj.append(meta_file_entry)

        return files_obj, meta_file_entry

    def get_fileslist(self):
        key_list = self.config["files"].split(",")

        if self.config.getboolean("files_separately", fallback=False):
            url = self.base_endpoint + str(self.recid) + "/files"
            res = requests.get(url, headers=self.headers)

            if res.status_code != 200:
                raise Exception(f"File list request gave HTTP {res.status_code}.")

            data = json.loads(res.text)
            key_list = self.config["files"].split(",")

            return get_dict_value(data, key_list)
        else:
            return get_dict_value(self.metadata, key_list)

    def download_files(self, files, base_path):
        log.info(f"Downloading {len(files)} files to {base_path}..")
        for sourcefile in files:
            if sourcefile["metadata"] is False:
                destination = f'{base_path}/{sourcefile["bagpath"]}'

                log.debug(
                    f'Downloading {sourcefile["origin"]["filename"]} from {sourcefile["origin"]["url"]}..'
                )

                sourcefile["downloaded"] = self.download_file(
                    sourcefile["origin"]["url"], destination
                )
            else:
                log.debug(
                    f'Skipped downloading of {sourcefile["origin"]["filename"]} from \
                    {sourcefile["origin"]["url"]}..'
                )

        log.info("Finished downloading")
        return files

    def has_file_baseuri(self):
        return self.has_file_base_uri

    def get_file_baseuri(self):
        key_list = self.config["file_uri"].split(",")

        return get_dict_value(self.metadata, key_list)

    def get_file_uri(self, file):
        key_list = self.config["file_uri"].split(",")
        file["url"] = get_dict_value(file, key_list)

        # If the uri is nested than unnest it (Zenodo's case)
        if key_list[0] != "url":
            file.pop(key_list[0])

        return file["url"]

    def get_filename(self, file):
        key_list = self.config["file_name"].split(",")

        return get_dict_value(file, key_list)

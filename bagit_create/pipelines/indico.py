from . import base
import logging
import requests
import json
import ntpath
import os
import configparser

log = logging.getLogger("basic-logger")


class IndicoV1Pipeline(base.BasePipeline):
    def __init__(self, base_url):

        log.info(f"Indico v1 pipeline initialised.\nBase URL: {base_url}")
        self.base_url = base_url

    # get metadata according to indico api guidelines
    def get_metadata(self, record_id, source):
        """
        Get JSON metadata from an Indico record ID
        Returns: [metadata_serialized, metadata_upstream_url, operation_status_code]
        """

        # Get Indico API Key from environment variable (or indico.ini)
        if os.environ.get("INDICO_KEY"):
            api_key = os.environ.get("INDICO_KEY")
        else:
            self.config_file = configparser.ConfigParser()
            self.config_file.read(os.path.join(os.path.dirname(__file__), "indico.ini"))
            self.config = self.config_file["indico"]
            api_key = self.config["api_key"]

        ## Prepare call Indico API
        # Authenticate with API Key
        headers = {"Authorization": "Bearer " + api_key}

        # Indico API export base endpoint
        endpoint = f"{self.base_url}/export/event/{record_id}.json"

        # Query params
        payload = {"detail": "contributions", "occ": "yes", "pretty": "yes"}

        r = requests.get(endpoint, headers=headers, params=payload)

        log.debug(f"Getting {r.url}")

        if r.status_code != 200:
            raise APIException(f"Metadata request gave HTTP {r.status_code}.")

        self.metadata_url = r.url
        try:
            self.metadata_size = r.headers["Content-length"]
        except Exception:
            self.metadata_size = 0

        if r.json()["count"] == 1:
            metadata_filename = f"metadata-{source}-{record_id}.xml"
            return (
                r.content,
                r.url,
                r.status_code,
                metadata_filename,
            )
        else:
            raise RecidException(
                f"Wrong recid. The {record_id} does not exist or it is not" " available."
            )

    # Download Remote Folders in the cwd
    def download_files(self, files, files_base_path):
        log.info(f"Downloading {len(files)} files to {files_base_path}..")

        for idx, sourcefile in enumerate(files):
            if sourcefile["metadata"] == False:
                destination = f'{files_base_path}/{sourcefile["origin"]["filename"]}'

                log.debug(
                    f'Downloading {sourcefile["origin"]["filename"]} from {sourcefile["origin"]["url"]}..'
                )

                files[idx]["downloaded"] = self.downloadRemoteFile(
                    sourcefile["origin"]["url"], destination
                )
            else:
                log.debug(f"Skipped downloading..")
        return files

    def create_manifests(self, files, base_path):
        algs = ["md5", "sha1"]
        for alg in algs:
            log.info(f"Generating manifest {alg}..")
            content, files = self.generate_manifest(files, alg, base_path)
            self.write_file(content, f"{base_path}/manifest-{alg}.txt")
        return files

    def parse_metadata(self, metadata_filename):

        # Gets metadata and transforms to JSON
        log.info("Parsing metadata..")
        files = []

        with open(metadata_filename) as jsonFile:
            metadataFile = json.load(jsonFile)
            jsonFile.close()

        for results in metadataFile["results"]:
            for folders in results["folders"]:
                # Check for attachments
                for att in folders["attachments"]:
                    obj = self.get_data_from_json(att)

                    if obj is not None:
                        if obj["origin"]["filename"]:
                            files.append(obj)
                        else:
                            log.warning(
                                f"Skipped entry. No basename found (probably an URL?)"
                            )

            for contributions in results["contributions"]:
                for folders in contributions["folders"]:
                    for att in folders["attachments"]:
                        obj = self.get_data_from_json(att)

                        if obj is not None:
                            if obj["origin"]["filename"]:
                                files.append(obj)
                            else:
                                log.warning(
                                    f"Skipped entry. No basename found (probably an"
                                    f" URL?)"
                                )

            # add extra metadata
            meta_file_entry = {
                "origin": {
                    "filename": f"{ntpath.basename(metadata_filename)}",
                    "path": "",
                    "url": self.metadata_url,
                },
                "metadata": True,
                "downloaded": True,
                "bagpath": f"data/content/{ntpath.basename(metadata_filename)}",
                "size": self.metadata_size,
            }
            files.append(meta_file_entry)
        return files, meta_file_entry

    def get_data_from_json(self, att):
        obj = {"origin": {}}

        obj["size"] = 0

        if "link_url" in att:
            return None
        if "size" in att:
            obj["origin"]["size"] = att["size"]
        if "download_url" in att:
            obj["origin"]["url"] = att["download_url"]
            obj["origin"]["filename"] = ntpath.basename(obj["origin"]["url"])
            self.filename1 = ntpath.basename(obj["origin"]["url"])
            obj["origin"]["path"] = ""
            obj[
                "bagpath"
            ] = f"data/content/{obj['origin']['path']}{obj['origin']['filename']}"
        if "title" in att:
            obj["origin"]["title"] = att["title"]

        obj["metadata"] = False
        obj["downloaded"] = False

        return obj


class RecidException(Exception):
    # This exception handles recid errors (incorrect recid or page not accessible)
    pass


class APIException(Exception):
    # This exception handles API errors (wrong API key or wrong url)
    pass

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
    def get_metadata(self, search_id):

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
        endpoint = f"https://indico.cern.ch/export/event/{search_id}.json"

        # Query params
        payload = {"detail": "contributions", "occ": "yes", "pretty": "yes"}

        response = requests.get(endpoint, headers=headers, params=payload)

        if response.status_code == 200:
            if response.json()["count"] == 1:
                metadata_filename = "metadata.json"
                return (
                    response.content,
                    response.url,
                    response.status_code,
                    metadata_filename,
                )
            else:
                raise RecidException(
                    f"Wrong recid. The {search_id} does not exist or it is not"
                    " available."
                )
        else:
            raise APIException(f"Indico API returned HTTP {response.status_code}.")

    # Download Remote Folders in the cwd
    def download_files(self, files, files_base_path):
        log.info(f"Downloading {len(files)} files to {files_base_path}..")

        for sourcefile in files:
            if sourcefile["metadata"] == False:
                destination = f'{files_base_path}/{sourcefile["filename"]}'
                src = sourcefile["url"]
                r = requests.get(src)
                with open(destination, "wb") as f:
                    f.write(r.content)
                sourcefile["downloaded"] = True

    def create_manifests(self, files, base_path):
        algs = ["md5", "sha1"]
        for alg in algs:
            log.info(f"Generating manifest {alg}..")
            content = self.generate_manifest(files, alg, base_path)
            self.write_file(content, f"{base_path}/manifest-{alg}.txt")

    def parse_metadata(self, metadataFilename):

        # Gets metadata and transforms to JSON
        log.info("Parsing metadata..")
        files = []

        with open(metadataFilename) as jsonFile:
            metadataFile = json.load(jsonFile)
            jsonFile.close()

        for results in metadataFile["results"]:
            for folders in results["folders"]:
                # Check for attachments
                for att in folders["attachments"]:
                    obj = self.get_data_from_json(att)

                    if obj is not None:
                        if obj["filename"]:
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
                            if obj["filename"]:
                                files.append(obj)
                            else:
                                log.warning(
                                    f"Skipped entry. No basename found (probably an"
                                    f" URL?)"
                                )

            # add extra metadata
            obj = {}

            if "title" in results["title"]:
                obj["title"] = results["title"]

            if "startDate" in results:
                obj["startDate"] = results["startDate"]

            if "endDate" in results:
                obj["endDate"] = results["endDate"]

            if "room" in results:
                obj["room"] = results["room"]

            if "location" in results["location"]:
                obj["location"] = results["location"]

            obj["metadata"] = True  # is metadata no files
            obj["downloaded"] = True
            obj["filename"] = "metadata.json"
            obj["localpath"] = f"data/meta/metadata.json"

            files.append(obj)
        return files

    def get_data_from_json(self, att):
        obj = {}

        obj["size"] = 0

        if "link_url" in att:
            return None
        if "size" in att:
            obj["size"] = att["size"]
        if "download_url" in att:
            obj["url"] = att["download_url"]
            obj["filename"] = ntpath.basename(obj["url"])
            self.filename1 = ntpath.basename(obj["url"])
            obj["path"] = obj["filename"]
        if "title" in att:
            obj["title"] = att["title"]

        if "content_type" in att:
            obj["content_type"] = att["content_type"]

        obj["metadata"] = False
        obj["downloaded"] = False
        obj["localpath"] = f"data/content/{self.filename1}"

        return obj


class RecidException(Exception):
    # This exception handles recid errors (incorrect recid or page not accessible)
    pass


class APIException(Exception):
    # This exception handles API errors (wrong API key or wrong url)
    pass

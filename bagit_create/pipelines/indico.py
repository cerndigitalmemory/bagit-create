from . import base
import logging
import requests
import json
import ntpath
import os
import configparser
import regex as re

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
            self.config = self.config_file[source]
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
    def download_files(self, files, base_path):
        log.info(f"Downloading {len(files)} files to {base_path}..")

        for idx, sourcefile in enumerate(files):
            if sourcefile["metadata"] == False:
                destination = f'{base_path}/{sourcefile["bagpath"]}'

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
        """
        Gets the received JSON file and creates the files object.
        """
        log.info("Parsing metadata..")
        files = []

        with open(metadata_filename) as jsonFile:
            metadataFile = json.load(jsonFile)
            jsonFile.close()

        for results in metadataFile["results"]:
            for folders in results["folders"]:
                # Check for attachments
                for att in folders["attachments"]:
                    # Gets the file_object and the file_id (in case it is a duplicate)
                    file_object, file_id = self.get_data_from_json(att)
                    if file_object:
                        file_object = self.check_name_conflicts(
                            file_object, files, file_id
                        )
                        if file_object["origin"]["filename"]:
                            files.append(file_object)
                        else:
                            log.warning(
                                f"Skipped entry. No basename found (probably an URL?)"
                            )

            for contributions in results["contributions"]:
                for folders in contributions["folders"]:
                    for att in folders["attachments"]:
                        file_object, file_id = self.get_data_from_json(att)
                        if file_object:
                            file_object = self.check_name_conflicts(
                                file_object, files, file_id
                            )
                            if file_object["origin"]["filename"]:
                                files.append(file_object)
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
        file_object = {"origin": {}}

        file_object["size"] = 0

        if "link_url" in att:
            return None
        if "size" in att:
            file_object["origin"]["size"] = att["size"]
        if "download_url" in att:
            file_object["origin"]["url"] = att["download_url"]
            file_object["origin"]["filename"] = ntpath.basename(
                file_object["origin"]["url"]
            )
            self.filename1 = ntpath.basename(file_object["origin"]["url"])
            file_object["origin"]["path"] = ""
            file_object[
                "bagpath"
            ] = f"data/content/{file_object['origin']['path']}{file_object['origin']['filename']}"
        if "title" in att:
            file_object["origin"]["title"] = att["title"]

        if "id" in att:
            id = att["id"]

        file_object["metadata"] = False
        file_object["downloaded"] = False

        return file_object, id

    def check_name_conflicts(self, file_object, files, id):
        """
        Finds if there are two files with the same name at the same folder.
        If this happens, saves them at different bagpaths with the same filename.

        ex. filename1.jpg data/content/filename1.jpg
            filename1.jpg data/content/filename1_duplicate1.jpg
        """
        bagpath = file_object["bagpath"]
        unique = True
        unique, bagpath = self.check_duplicate(files, unique, bagpath, id)

        while unique == False:
            unique, bagpath = self.check_duplicate(files, unique, bagpath, id)

        file_object["bagpath"] = bagpath

        return file_object

    def check_duplicate(self, files, unique, bagpath, id):
        """
        For each new file_object checks in the files list if there is another entry with the same filename.
        If there is, then appends the _duplicate suffix
        """
        # Set unique to True so if a same bagpath is not found, it will return True and exit from the loop
        unique = True
        for file in files:
            # For each bagpath check in the files object if there is another identical
            if file["bagpath"] == bagpath:
                # If there is split the filename and the file extention ans save them in file_name and file_extention accordingly
                file_name, file_extension = os.path.splitext(bagpath)

                # Check if the file_name ends with _duplicateX, (X is a number).
                # If it does then that means that this is the third time this file exists at the same bagpath, so increase the suffix number by one

                bagpath = file_name + "-" + str(id) + file_extension

                # If the filename was changed check again if there is another filename with the same name
                unique = False
        return unique, bagpath


class RecidException(Exception):
    # This exception handles recid errors (incorrect recid or page not accessible)
    pass


class APIException(Exception):
    # This exception handles API errors (wrong API key or wrong url)
    pass

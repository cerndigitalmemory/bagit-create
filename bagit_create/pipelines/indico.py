import json
import logging
import ntpath
import os

import requests

from . import base

log = logging.getLogger("bic-basic-logger")


class IndicoV1Pipeline(base.BasePipeline):
    def __init__(self, base_url, token=None):

        log.info(f"Indico v3 pipeline initialised.\nBase URL: {base_url}")
        self.base_url = base_url
        self.source = "indico"

        # Get Indico API Key from environment variable
        if token:
            self.api_key = token
        else:
            raise Exception("API token not found, set it through the token parameter.")

    # get metadata according to indico api guidelines
    def get_metadata(self, record_id, source):
        """
        Get JSON metadata from an Indico record ID
        Returns: [metadata_serialized, metadata_upstream_url, operation_status_code]
        """

        # Prepare call Indico API
        # Authenticate with API Key
        headers = {"Authorization": "Bearer " + self.api_key}

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
            metadata_filename = f"metadata-{source}-{record_id}.json"
            return (
                r.content,
                r.url,
                r.status_code,
                metadata_filename,
            )
        else:
            raise RecidException(
                f"Wrong recid. The record {record_id} does not exist or it is not available."
            )

    # Download Remote Folders in the cwd
    def download_files(self, files, base_path):
        log.info(f"Downloading {len(files)} files to {base_path}..")

        headers = {"Authorization": "Bearer " + self.api_key}

        for idx, sourcefile in enumerate(files):
            if sourcefile["metadata"] is False:
                destination = f'{base_path}/{sourcefile["bagpath"]}'

                log.debug(
                    f'Downloading {sourcefile["origin"]["filename"]} from \
                    {sourcefile["origin"]["url"]}..'
                )

                files[idx]["downloaded"] = self.downloadRemoteFile(
                    sourcefile["origin"]["url"], destination, headers
                )
            else:
                log.debug("Skipped downloading..")
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
        Reads the metadata from a given path.
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
                    # Gets the file_object and the file_id.
                    file_object, file_id = self.get_data_from_json(att)
                    if file_object:
                        file_object = self.resolve_name_conflicts(
                            file_object, files, file_id
                        )
                        if file_object["origin"]["filename"]:
                            files.append(file_object)
                        else:
                            log.warning(
                                "Skipped entry. No basename found (probably an URL?)"
                            )

            for contributions in results["contributions"]:
                for folders in contributions["folders"]:
                    for att in folders["attachments"]:
                        file_object, file_id = self.get_data_from_json(att)
                        if file_object:
                            file_object = self.resolve_name_conflicts(
                                file_object, files, file_id
                            )
                            if file_object["origin"]["filename"]:
                                files.append(file_object)
                            else:
                                log.warning(
                                    "Skipped entry. No basename found (probably an"
                                    " URL?)"
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
            return None, None
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

        if "checksum" in att:
            # Append md5: and set the checksum
            file_object["checksum"] = f'md5:{att["checksum"]}'

        file_object["metadata"] = False
        file_object["downloaded"] = False
        return file_object, id

    def resolve_name_conflicts(self, file_object, files, id):
        """
        Finds if there are two files with the same name at the same folder.
        If this happens, it prefixes the file name with the file_id.

        ex. filename1.jpg data/content/filename1.jpg
            filename1.jpg data/content/{file_id}-filename1.jpg
        """
        bagpath = file_object["bagpath"]
        bagpath = self.resolve_duplicate(files, bagpath, id)

        file_object["bagpath"] = bagpath

        return file_object

    def resolve_duplicate(self, files, bagpath, id):
        """
        For each new file_object checks in the files list if there is another entry with the same filename.
        If there is, then appends the {file_id}- prefix
        """
        for file in files:
            # For each bagpath check in the files object if there is another identical
            if file["bagpath"] == bagpath:
                # If there is split the filename and the file extention ans save them in file_name and file_extention accordingly
                file_name, file_extension = os.path.splitext(bagpath)
                # Get the filename of the given path and add the {id}- preffix.
                try:
                    file_base_path, file_name = (
                        os.path.split(file_name)[0],
                        os.path.split(file_name)[1],
                    )
                except Exception:
                    file_base_path = None
                bagpath = os.path.join(
                    file_base_path, str(id) + "-" + file_name + file_extension
                )

        return bagpath


class RecidException(Exception):
    # This exception handles recid errors (incorrect recid or page not accessible)
    pass


class APIException(Exception):
    # This exception handles API errors (wrong API key or wrong url)
    pass

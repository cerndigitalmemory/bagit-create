import json
import logging

import requests
from cernopendata_client import searcher

from . import base

log = logging.getLogger("bic-basic-logger")


class OpenDataPipeline(base.BasePipeline):
    def __init__(self, base_url):
        log.info(f"CERN Open Data pipeline initialised.\nBase URL: {base_url}")
        self.SERVER_HTTP_URI = base_url

    # metadata, metadata_url, status_code, metadata_filename

    def get_metadata(self, recid, source):
        if searcher.verify_recid(server=self.SERVER_HTTP_URI, recid=recid):
            metadata = searcher.get_record_as_json(
                server=self.SERVER_HTTP_URI, recid=recid
            )
            status_code = 200

        metadata_url = f"https://opendata.cern.ch/api/records/{recid}"
        metadata_filename = "metadata.json"
        return metadata, metadata_url, status_code, metadata_filename

    def parse_metadata(self, metadata_file_path):
        log.info("Parsing metadata..")
        files = []
        with open(metadata_file_path) as jsonFile:
            metadata = json.load(jsonFile)
            jsonFile.close()

        for sourcefile in metadata["metadata"]["files"]:
            # file lists are TXT and JSON, look for the JSON ones
            if "type" in sourcefile:
                if sourcefile["key"][-4:] == "json" and "index" in sourcefile["type"]:
                    list_endpoint = f"https://opendata.cern.ch/record/{metadata['id']}/files/{sourcefile['key']}"
                    # Download the file list
                    r = requests.get(list_endpoint)
                    log.debug(f"Unpacking file list {list_endpoint}")
                    # For every file in the list
                    for el in r.json():
                        # Remove the EOS instance prefix to get the path
                        el["url"] = el["uri"].replace(
                            "root://eospublic.cern.ch/", "http://opendata.cern.ch/"
                        )
                        # Append the final path
                        el["path"] = el["filename"]
                        localpath = el["path"]
                        el["bagpath"] = f"data/content/{localpath}"
                        el["metadata"] = False
                        el["downloaded"] = False
                        if type not in el or (type in el and el["type"] != "index.txt"):
                            # Map values to build a "File" entry
                            file = {
                                "origin": {
                                    # Save both HTTP and ROOT URLs
                                    "url": [el["url"], el["uri"]],
                                    "filename": el["filename"],
                                    "path": "",
                                },
                                "size": el["size"],
                                "bagpath": f"data/content/{localpath}",
                                "metadata": False,
                                "downloaded": False,
                            }
                            files.append(file)
            else:
                sourcefile["url"] = sourcefile["uri"].replace(
                    "root://eospublic.cern.ch/", "http://opendata.cern.ch/"
                )
                sourcefile["filename"] = sourcefile["key"]
                sourcefile["path"] = sourcefile["filename"]
                localpath = sourcefile["path"]
                sourcefile["downloaded"] = False
                # Map values to build a "File" entry
                file = {
                    "origin": {
                        # Save both HTTP and ROOT URLs
                        "url": [sourcefile["url"], sourcefile["uri"]],
                        "filename": sourcefile["key"],
                        "path": "",
                    },
                    "size": sourcefile["size"],
                    "bagpath": f"data/content/{localpath}",
                    "metadata": False,
                    "downloaded": False,
                }
                files.append(file)
        return files, {}

    def download_files(self, files, temp_files_path):
        log.info(f"Downloading {len(files)} files to {temp_files_path}..")
        skipped = 0
        for file in files:

            if file["metadata"] is False:
                destination = f'{temp_files_path}/{file["origin"]["filename"]}'
                # If more than one URL is available, use the first one (HTTP)
                if type(file["origin"]["url"]) == list:
                    download_url = file["origin"]["url"][0]
                else:
                    download_url = file["origin"]["url"]
                log.debug(
                    f'Downloading {file["origin"]["filename"]} from {download_url}..'
                )
                if download_url[:4] == "http":
                    file["downloaded"] = self.downloadRemoteFile(
                        download_url,
                        destination,
                    )
                elif download_url[:4] == "/eos":
                    file["downloaded"] = self.downloadEOSfile(
                        download_url,
                        destination,
                    )

                    if file["downloaded"] is False:
                        skipped += 1
        if skipped > 0:
            log.info(
                f"{skipped} files were skipped. Checksums will be searched in metadata \
    but won't be computed locally."
            )
        return files

    def create_manifests(self, files, base_path):
        algs = ["adler32"]
        log.warning(
            "adler32 was selected because it's the only checksum available in CERN Open Data. This will create an invalid Bag, as adler32 is not supported."
        )

        for alg in algs:
            log.info(f"Generating manifest {alg}..")
            content, files = self.generate_manifest(files, alg, base_path)
            self.write_file(content, f"{base_path}/manifest-{alg}.txt")
        return files

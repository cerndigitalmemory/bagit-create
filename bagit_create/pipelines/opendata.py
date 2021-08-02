from . import base
import logging
import requests
from pymarc import marcxml
import ntpath
from .. import cds
import json

from cernopendata_client import searcher, downloader


class OpenDataPipeline(base.BasePipeline):
    def __init__(self, base_url):
        logging.debug(f"CERN Open Data pipeline initialised. Base URL: {base_url}")
        self.SERVER_HTTP_URI = base_url

    # metadata, metadata_url, status_code, metadata_filename

    def get_metadata(self, recid):
        if searcher.verify_recid(server=self.SERVER_HTTP_URI, recid=recid):
            metadata = searcher.get_record_as_json(
                server=self.SERVER_HTTP_URI, recid=recid
            )
            status_code = 200

        metadata_url = f"https://opendata.cern.ch/api/records/{recid}"
        metadata_filename = "metadata.json"
        return metadata, metadata_url, status_code, metadata_filename

    def parse_metadata(self, metadata_file_path):
        files = []
        with open(metadata_file_path) as jsonFile:
            metadata = json.load(jsonFile)
            jsonFile.close()

        for sourcefile in metadata["metadata"]["files"]:
            # file lists are TXT and JSON, look for the JSON ones
            if sourcefile["key"][-4:] == "json":
                list_endpoint = f"https://opendata.cern.ch/record/{metadata['id']}/files/{sourcefile['key']}"
                # Download the file list
                r = requests.get(list_endpoint)
                logging.debug(f"Unpacking file list {list_endpoint}")
                # For every file in the list
                for el in r.json():
                    # Remove the EOS instance prefix to get the path
                    el["url"] = el["uri"].replace("root://eospublic.cern.ch/", "")
                    # Append the final path
                    el["path"] = el["filename"]
                    el["metadata"] = False
                    el["downloaded"] = False
                    if type not in el or (type in el and el["type"] != "index.txt"):
                        files.append(el)
        return files

    def download_files(self, files, temp_files_path):
        logging.info(f"Downloading {len(files)} files to {temp_files_path}..")
        for file in files:
            if file["metadata"] == False:
                destination = f'{temp_files_path}/{file["filename"]}'
                logging.debug(f'Downloading {file["filename"]} from {file["url"]}..')
                if file["url"][:4] == "http":
                    file["downloaded"] = cds.downloadRemotefile(
                        file["url"],
                        destination,
                    )
                elif file["url"][:4] == "/eos":
                    file["downloaded"] = cds.downloadEOSfile(
                        file["url"],
                        destination,
                    )

    def create_manifests(self, files, base_path, files_base_path):
        algs = ["adler32"]
        for alg in algs:
            logging.info(f"Generating manifest {alg}..")
            content = self.generate_manifest(files, alg, files_base_path)
            self.write_file(content, f"{base_path}/manifest-{alg}.txt")

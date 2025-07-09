import json
import logging
import ntpath
import os
import shutil
import subprocess
import time
from urllib.parse import quote

import requests

from bagit_create.exceptions import APIException, GitlabException, ServerException

from . import base
from .local import LocalV1Pipeline

log = logging.getLogger("bic-basic-logger")


class GitlabPipeline(base.BasePipeline):
    def __init__(self, base_url, recid, token=None):
        log.info(f"Gitlab pipeline initialised.\nBase URL: {base_url}")
        self.base_url = base_url
        self.source = "gitlab"
        self.recid = recid
        self.current_time_elapsed = 0  # Keeps track of the seconds elapsed since the repeated api requests have started
        self.interval_seconds = (
            10  # Every 10 seconds triggers a new request to gitlab server
        )
        self.total_seconds_allowed = (
            600  # Repeated requests should not run for more than 600 seconds (10 mins)
        )
        self.metadata_filename = f"metadata-{self.source}-{self.recid}.json"

        # Prepare call Gitlab API
        # Authenticate with API Key
        if token:
            self.api_key = token
            self.headers = {"Authorization": "Bearer " + self.api_key}
        else:
            raise APIException(
                "API token not found, set it through the token parameter."
            )

    def get_metadata(self, record_id, source):
        """
        Given a project ID, query the GitLab API for
        - metadata about the project
        - metadata about the files in the repository tree
        The results are then put in a single JSON file
        Returns: [metadata_serialized, metadata_upstream_url, operation_status_code]
        """

        # Gitlab API export base endpoint
        endpoint = f"{self.base_url}/api/v4/projects/{record_id}/repository/tree"

        # Query params
        # Gitlab API returns paginated results by default. Additional API calls must be done to make sure all the files are fetched
        current_page = 1

        # Gets the metadata and each file path from gitlab (will only return the first page)
        response = self.make_api_request(endpoint, current_page, self.headers)

        # Gets the total pages that need to be returned in total and saves it
        total_pages = int(response.headers["X-Total-Pages"])

        # Saves the received content and the response url
        results_files = response.json()
        results_dict = results_files[0]

        # If there are more pages
        while current_page < total_pages:
            current_page += 1
            # Increase page by one and run the query for the next page
            response = self.make_api_request(endpoint, current_page, self.headers)

            # Append the results received to the prvious one and make the transformation to bytes object
            results_dict2 = response.json()
            results_dict.update(results_dict2[0])
            # Joins the results from multiple api calls

        self.metadata_url = response.url

        try:
            self.metadata_size = response.headers["X-Total"]
        except Exception:
            self.metadata_size = 0

        try:
            response_for_clone = requests.get(
                url=f"https://gitlab.cern.ch/api/v4/projects/{self.recid}/",
                headers=self.headers,
            )

            project_results = response_for_clone.json()
            self.ssh_to_repo = project_results["ssh_url_to_repo"]
            self.url_to_repo = project_results["web_url"]
            self.http_url_to_repo = project_results["http_url_to_repo"]
        except Exception():
            pass

        results = {"project": project_results, "files": results_files}

        return (
            results,
            response.url,
            response.status_code,
            self.metadata_filename,
        )

    def make_api_request(self, endpoint, page, headers):
        """
        Make a (paginated) request to the GitLab api
        Returns: response
        """
        payload = {
            "recursive": True,  # Enable recursive mode to look for files inside directories
            "per_page": 50,  # Results per page
            "page": page,  # Page to retrieve
        }
        log.debug(f"Getting page {page} from {endpoint}")
        r = requests.get(url=endpoint, params=payload, headers=headers)
        if r.status_code in range(400, 404):
            raise APIException("You don't have access to that repository.")
        elif r.ok:
            return r
        else:
            raise ServerException("Server Error. Cannot fetch results from gitlab")

    def parse_metadata(self, metadata_filename):
        """
        Given the metadata file, parse it and return the files object
        """
        log.info("Parsing file metadata..")
        files = []

        with open(metadata_filename) as jsonFile:
            metadataFile = json.load(jsonFile)
            jsonFile.close()

        # Parse results metadata file
        for results in metadataFile["files"]:
            if results["type"] == "blob":
                file_object = {"origin": {}}
                file_object["size"] = 0
                if "name" in results:
                    file_object["origin"]["filename"] = results["name"]
                if "path" in results:
                    path = results["path"]
                    file_object["origin"]["path"] = path
                    encoded_path = quote(path, safe="")
                    download_url = f"{self.base_url}/api/v4/projects/{self.recid}/repository/files/{encoded_path}"
                    file_object["origin"]["url"] = download_url
                    file_object["bagpath"] = f"data/content/repository_files/{path}"
                if "id" in results:
                    file_object["origin"]["id"] = results["id"]

                file_object["metadata"] = False
                file_object["downloaded"] = False

                files.append(file_object)

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

        # Fetch additional metadata package and append to the current values.
        files = self.get_gitlab_exported_metadata(
            self.recid, self.source, self.base_path, files, self.metadata_filename
        )

        return files, meta_file_entry

    def get_gitlab_exported_metadata(
        self, recid, source, base_path, files, metadata_filename
    ):
        """
        This function uses the export API call according to: https://docs.gitlab.com/ee/api/project_import_export.html#schedule-an-export
        To get the metadata the program has 3 steps:

        Step 1: Request the creation of the creation of the zipped file from gitlab using POST /projects/:id/export
        Step 2: Make a request to the backend to check if the package is ready using GET /projects/:id/export
        Step 3: Download the package from gitlab when ready using GET /projects/:id/export/download

        This functions downloads and uncompresses the metadata package.

        Finally appends the downloaded metadata to the existing ones
        """
        log.debug("Requesting export from upstream...")

        r = requests.post(
            f"https://gitlab.cern.ch/api/v4/projects/{recid}/export",
            headers=self.headers,
        )

        if r.ok:
            """
            Makes repetitve API requests till the exported package is available for download
            """
            results_fetched = False
            time.sleep(1)  # Wait 1 second the first time for the results to be exported
            while (
                self.current_time_elapsed <= self.total_seconds_allowed
                and results_fetched is False
            ):  # If the time elapsed is less than the max allowed time, and if the results are not fetched continues the loop.
                """
                Calls the check_export function which returns true if it manages to get the data from the Gitlab API
                """
                results_fetched, files_from_export = self.check_export(base_path)
                if (
                    results_fetched
                ):  # If the response from check_export is True break the loop
                    break
                else:  # Otherwise wait for xx seconds and try again
                    time.sleep(self.interval_seconds)
                    self.current_time_elapsed = (
                        self.current_time_elapsed + self.interval_seconds
                    )
            if not results_fetched:
                raise GitlabException(
                    "Maximum requests to gitlab server reached. Please try again later."
                )

        else:
            raise APIException(
                "Request Failed. Check if you have access to export from this repository."
            )

        log.debug("Export package retrieved")
        for file_from_export in files_from_export:
            """
            For the files list returned from the local pipeline instance, change the
            necessary values of that file and append to the original files list
            """
            # If it is the metadata json, do not add it to the files list as it already exists.
            # (double checks both the name and the bagpath)
            if not (
                file_from_export["origin"]["filename"]
                == ntpath.basename(self.metadata_filename)
                and file_from_export["bagpath"]
                == f"data/content/{ntpath.basename(self.metadata_filename)}"
            ):
                file_from_export["downloaded"] = True
                file_from_export["metadata"] = True
                file_from_export["origin"][
                    "url"
                ] = f"https://gitlab.cern.ch/api/v4/projects/{recid}/export"
                if "sourcePath" in file_from_export["origin"]:
                    origin = file_from_export["origin"]
                    origin.pop("sourcePath")
                if "rawstat" in file_from_export:
                    file_from_export.pop("rawstat")
                files.append(file_from_export)

        return files

    def check_export(self, base_path):
        """
        Check if the requested export is ready and if it is, download and unpack it
        The "local" pipeline is then executed on the downloaded file so they can be added to the manifest
        """
        log.debug("Checking for gitlab server response...")

        URL = f"https://gitlab.cern.ch/api/v4/projects/{self.recid}/export"

        # sending get request and saving the response as response object

        r2 = requests.get(url=URL, headers=self.headers)

        results = r2.json()
        path = results["path"]

        if results["export_status"] == "finished":
            log.debug("Export package ready")

            URL = f"https://gitlab.cern.ch/api/v4/projects/{self.recid}/export/download"
            # sending get request and saving the response as response object

            headers = self.headers
            headers["Accept-encoding"] = "gzip, deflate, br"

            r3 = requests.get(url=URL, stream=True, headers=headers)

            if r3.ok:
                exported_files_destination = (
                    f"{base_path}/data/content/repository_metadata"
                )

                """
                Two subfolders are needed here:
                The first one to store the raw files and ditrectories are in the remote repository.
                The second one to add all the additional files that include metadata about issues, merge requests, ci pipelines etc.
                """

                os.mkdir(
                    exported_files_destination
                )  # additional metadata folder creation

                try:
                    compressed_filename = f"{path}.tar.gz"
                    with open(compressed_filename, "wb") as f:
                        for chunk in r3.raw.stream(1024, decode_content=False):
                            if chunk:
                                f.write(chunk)
                    # uncompress downloaded file
                    shutil.unpack_archive(
                        compressed_filename, exported_files_destination
                    )

                    # delete the compressed file and keep the uncompressed SIP
                    os.remove(compressed_filename)

                    # Initialize a new local mode instance and run the scan_files function passing the uncompressed folder path
                    local_instance = LocalV1Pipeline(exported_files_destination)

                    files = local_instance.scan_files(
                        f"{base_path}/data/content", author=None
                    )

                    # remove job and shut down the scheduler
                    return True, files

                except Exception as e:
                    if os.path.exists(compressed_filename):
                        os.remove(compressed_filename)
                    raise Exception("Retrieving export package failed.", e)
            else:
                log.info("Maximum requests to gitlab server reached. Trying again...")
                return False, None
        else:
            log.debug("Export package not ready yet. Trying again...")
            return False, None

    def download_files(self, files, base_path):
        """
        Gets gitlab files using git clone repository
        """
        clone_destination = f"{base_path}/data/content/repository_files/"
        os.mkdir(clone_destination)  # Creates the raw files folder

        log.debug("Cloning the repository")

        try:
            """
            git clone using https + auth token needs a request with the following format:
            git clone https://oauth2:[token]@gitlab_url_to_repo
            """
            split_http_command = self.http_url_to_repo.split("https://")
            git_clone_command = (
                "https://oauth2:" + self.api_key + "@" + split_http_command[1]
            )
            subprocess.call(["git", "clone", git_clone_command, clone_destination])

            # Initialize a new local mode instance and run the scan_files function passing the uncompressed folder path
            local_instance = LocalV1Pipeline(clone_destination)

            files_from_clone = local_instance.scan_files(
                f"{base_path}/data/content", author=None
            )

        except Exception():
            raise GitlabException("Error while cloning the gitlab repository.")

        for file_from_clone in files_from_clone:
            """
            For the files list returned from the local pipeline instance, change the
            necessary values of that file and append to the original files list
            """
            # If it is the metadata json, do not add it to the files list as it already exists.
            # (double checks both the name and the bagpath)
            if not (
                file_from_clone["origin"]["filename"]
                == ntpath.basename(self.metadata_filename)
                and file_from_clone["bagpath"]
                == f"data/content/{ntpath.basename(self.metadata_filename)}"
            ):
                # Checks if the file already exists in the files list
                exists = False
                for file_from_get_metadata in files:
                    if file_from_get_metadata["bagpath"] == file_from_clone["bagpath"]:
                        exists = True
                        file_from_get_metadata["downloaded"] = True
                        file_from_get_metadata["metadata"] = False
                        if "sourcePath" in file_from_get_metadata["origin"]:
                            origin = file_from_get_metadata["origin"]
                            origin.pop("sourcePath")
                # If it does not, it means it is a metadata file or a file from the .git folder and adds it to the  files list
                if not exists:
                    file_from_clone["metadata"] = False
                    file_from_clone["downloaded"] = True
                    file_from_clone["origin"]["url"] = self.url_to_repo
                    if "sourcePath" in file_from_clone["origin"]:
                        origin = file_from_clone["origin"]
                        origin.pop("sourcePath")
                    if "rawstat" in file_from_get_metadata:
                        file_from_get_metadata.pop("rawstat")
                    files.append(file_from_clone)

        return files

    def create_manifests(self, files, base_path):
        algs = ["sha256"]
        for alg in algs:
            log.info(f"Generating manifest {alg}..")
            content, files = self.generate_manifest(files, alg, base_path)
            self.write_file(content, f"{base_path}/manifest-{alg}.txt")
        return files

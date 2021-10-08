from . import base
import logging
import requests
from pymarc import marcxml
import ntpath
from .. import cds
import re

log = logging.getLogger("basic-logger")


class InvenioV1Pipeline(base.BasePipeline):
    def __init__(self, base_url):

        log.info(f"Invenio v1 pipeline initialised.\nBase URL: {base_url}")
        self.base_url = base_url

    def get_metadata(self, record_id, type="xml"):
        """
        Get MARC21 metadata from a CDS record ID
        Returns: [metadata_serialized, metadata_upstream_url, operation_status_code]
        """

        record_url = f"{self.base_url}{record_id}"

        if type == "xml":
            of = "xm"
        elif type == "dc":
            of = "xd"

        payload = {"of": of}

        r = requests.get(record_url, params=payload)

        log.debug(f"Getting {r.url}")

        if r.status_code != 200:
            raise Exception(f"Metadata request gave HTTP {r.status_code}.")

        self.metadata_url = r.url
        try:
            self.metadata_size = r.headers["Content-length"]
        except Exception:
            self.metadata_size = 0
        return r.content, r.url, r.status_code, "metadata.xml"

    def parse_metadata(self, metadata_filename):
        """
        Given a MARC21 metadata file,
        return an array of "Files" objects, containing:
        `filename`
        `uri`
        `remote`
        `hash`
        `size`
        """

        # Parse the XML as MARC21
        #  and get the first record (result should be one record anyway)
        log.debug("Parsing metadata..")
        try:
            record = marcxml.parse_xml_to_array(metadata_filename)[0]
        except Exception:
            raise Exception("Malformed metadata. Check if the record is public.")
        # Look for 856 entries
        #  MARC21: 856 - Electronic Location and Access (R)
        files = []
        for f in record.get_fields("856"):
            # Prepare the File object
            obj = {
                "source" : {}
            }

            # Default size
            obj["size"] = 0

            if f["u"]:
                obj["source"]["url"] = f["u"]
            elif f["d"]:
                obj["source"]["url"] = f["d"]
            else:
                log.debug(f'Skipped 856 entry "{f}". No `u` or `d` field.')
                continue

            # File checksum
            if f["w"]:
                p = re.compile(r"\([A-Za-z]*:([A-Za-z0-9]*).*;([A-Za-z0-9]*)")
                m = p.match(f["w"])
                alg = m.groups()[0].lower()
                checksum = m.groups()[1]
                obj["checksum"] = f"{alg}:{checksum}"

            # File size
            if f["s"]:
                obj["size"] = int(f["s"])

            # Get basename
            if obj["source"]["url"]:
                obj["source"]["filename"] = ntpath.basename(obj["source"]["url"])
                # We suppose no folder structure
                obj["source"]["path"] = ""
                obj["bagpath"] = f"data/content/{obj['source']['path']}{obj['source']['filename']}"

            obj["metadata"] = False
            obj["downloaded"] = False

            if obj["source"]["filename"]:
                files.append(obj)
            else:
                log.warning(f'Skipped entry "{f}". No basename found (probably an URL?)')
        log.debug(f"Got {len(files)} files")

        meta_file_entry = {
            "source": {
                "filename": "metadata.xml",
                "path": "",
                "url": self.metadata_url,       
            },
            "metadata": True,
            "downloaded": True,
            "bagpath": "data/meta/metadata.xml",
            "size": self.metadata_size,
        }
        files.append(meta_file_entry)

        return files

    def create_manifests(self, files, base_path):
        algs = ["md5", "sha1"]
        for alg in algs:
            log.info(f"Generating manifest {alg}..")
            content, files = self.generate_manifest(files, alg, base_path)
            self.write_file(content, f"{base_path}/manifest-{alg}.txt")
        return files

    def download_files(self, files, files_base_path):
        log.info(f"Downloading {len(files)} files to {files_base_path}..")
        for sourcefile in files:
            if sourcefile["metadata"] == False:
                destination = f'{files_base_path}/{sourcefile["source"]["filename"]}'

                log.debug(
                    f'Downloading {sourcefile["source"]["filename"]} from {sourcefile["source"]["url"]}..'
                )

                sourcefile["downloaded"] = cds.downloadRemoteFile(
                    sourcefile["source"]["url"], destination
                )

            else:
                log.debug(
                    f'Skipped downloading of {sourcefile["source"]["filename"]} from              '
                    f'       {sourcefile["source"]["url"]}..'
                )

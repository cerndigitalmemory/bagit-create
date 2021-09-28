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
            obj = {}

            # Unknown size fallback
            obj["size"] = 0

            if f["u"]:
                obj["url"] = f["u"]
                obj["remote"] = "HTTP"
            elif f["d"]:
                obj["url"] = f["d"]
                obj["remote"] = "EOS"
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
            if obj["url"]:
                obj["filename"] = ntpath.basename(obj["url"])
                # We suppose no folder structure
                obj["path"] = obj["filename"]
                obj["localpath"] = f"data/content/{obj['path']}"

            obj["metadata"] = False
            obj["downloaded"] = False

            if obj["filename"]:
                files.append(obj)
            else:
                log.warning(f'Skipped entry "{f}". No basename found (probably an URL?)')
        log.debug(f"Got {len(files)} files")

        meta_file_entry = {
            "filename": "metadata.xml",
            "path": "metadata.xml",
            "metadata": True,
            "downloaded": True,
            "localpath": "data/meta/metadata.xml",
            "url": self.metadata_url,
            "size": self.metadata_size,
        }
        files.append(meta_file_entry)

        return files

    def create_manifests(self, files, base_path):
        algs = ["md5", "sha1"]
        for alg in algs:
            log.info(f"Generating manifest {alg}..")
            content = self.generate_manifest(files, alg, base_path)
            self.write_file(content, f"{base_path}/manifest-{alg}.txt")

    def download_files(self, files, files_base_path):
        log.info(f"Downloading {len(files)} files to {files_base_path}..")
        for sourcefile in files:
            if sourcefile["metadata"] == False:
                destination = f'{files_base_path}/{sourcefile["filename"]}'

                log.debug(
                    f'Downloading {sourcefile["filename"]} from {sourcefile["url"]}..'
                )

                if sourcefile["remote"] == "HTTP":
                    sourcefile["downloaded"] = cds.downloadRemoteFile(
                        sourcefile["url"], destination
                    )
                elif sourcefile["remote"] == "EOS":
                    cds.downloadEOSfile(sourcefile["url"], destination)
            else:
                log.debug(
                    f'Skipped downloading of {sourcefile["filename"]} from              '
                    f'       {sourcefile["url"]}..'
                )

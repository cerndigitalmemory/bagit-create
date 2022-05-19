import logging
import ntpath
import re

import cern_sso
import requests
from pymarc import marcxml

from .. import bibdocfile, cds
from . import base

log = logging.getLogger("bic-basic-logger")


class InvenioV1Pipeline(base.BasePipeline):
    def __init__(self, base_url, recid, cert_path=None, token=None, skipssl=False):

        log.info(f"Invenio v1 pipeline initialised.\nBase URL: {base_url}")
        self.base_url = base_url
        self.verifyssl = not skipssl

        if cert_path:
            log.info(
                "Running in authenticated mode, trying SSO with the given certificate.."
            )

            cert_file = f"{cert_path}.pem"
            key_file = f"{cert_path}.key"
            try:
                # Authenticate against the record URL
                self.cookies = cern_sso.cert_sign_on(
                    f"{base_url}{recid}", cert_file=cert_file, key_file=key_file
                )

                log.info("SSO successful. Proceeding.")
            except Exception:
                log.error(
                    "SSO failed. Check if the .key and .pem files are in the provided path with the provided (same) file name and they are generated from a valid CERN User Grid certificate (.p12)."
                )
                raise Exception("CERN SSO failed")
        elif token:
            log.info("Setting Invenio cookie")
            self.cookies = {"INVENIOSESSION": token}
        else:
            self.cookies = None

    def run_bibdoc(self, files, record_id, bd_ssh_host):
        output = bibdocfile.run(record_id, bd_ssh_host)
        files = bibdocfile.parse(output, record_id)

        bibdocfile_entry = {
            "origin": {
                "filename": "bibdoc.txt",
                "path": "",
                "url": self.metadata_url,
            },
            "metadata": False,
            "downloaded": True,
            "bagpath": "data/meta/bibdoc.txt",
            "size": 0,
        }
        files.append(bibdocfile_entry)

        return output, files

    def get_metadata(self, record_id, source, type="xml"):
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

        r = requests.get(
            record_url, params=payload, cookies=self.cookies, verify=self.verifyssl
        )

        log.debug(f"Getting {r.url}")

        if r.status_code != 200:
            raise Exception(f"Metadata request gave HTTP {r.status_code}.")

        self.metadata_url = r.url
        try:
            self.metadata_size = r.headers["Content-length"]
        except Exception:
            self.metadata_size = 0

        return r.content, r.url, r.status_code, f"metadata-{source}-{record_id}.xml"

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
            raise Exception(
                """
                Malformed metadata. Check if the record exists and it's public.
                If it requires authentication, use the --cert option to enable CERN SSO.
                """
            )
        # Look for 856 entries
        #  MARC21: 856 - Electronic Location and Access (R)
        files = []
        for f in record.get_fields("856"):
            # Prepare the File object
            obj = {"origin": {}}

            # Default size
            obj["size"] = 0

            if f["u"]:
                obj["origin"]["url"] = f["u"]
            elif f["d"]:
                obj["origin"]["url"] = f["d"]
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
            if obj["origin"]["url"]:
                obj["origin"]["filename"] = ntpath.basename(obj["origin"]["url"])
                # We suppose no folder structure
                obj["origin"]["path"] = ""
                obj[
                    "bagpath"
                ] = f"data/content/{obj['origin']['path']}{obj['origin']['filename']}"

            obj["metadata"] = False
            obj["downloaded"] = False

            if obj["origin"]["filename"]:
                files.append(obj)
            else:
                log.warning(f'Skipped entry "{f}". No basename found (probably an URL?)')
        log.debug(f"Got {len(files)} files")

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

    def create_manifests(self, files, base_path):
        algs = ["md5"]
        for alg in algs:
            log.info(f"Generating manifest {alg}..")
            content, files = self.generate_manifest(files, alg, base_path)
            self.write_file(content, f"{base_path}/manifest-{alg}.txt")
        return files

    def download_files(self, files, base_path):
        """
        Given a Files object, download files in the specified path
        """

        log.info(f"Downloading {len(files)} files to {base_path}..")
        for idx, sourcefile in enumerate(files):
            # We're looking for files not flagged as metadata and not downloaded yet
            if sourcefile["metadata"] is False and sourcefile["downloaded"] is False:
                # If the origin url is a list, use the first for downloading
                if type(sourcefile["origin"]["url"]) == list:
                    download_url = sourcefile["origin"]["url"][0]
                else:
                    download_url = sourcefile["origin"]["url"]

                # prepare the destination path
                destination = f'{base_path}/{sourcefile["bagpath"]}'

                log.debug(
                    f'Downloading {sourcefile["origin"]["filename"]} from {download_url}..'
                )

                files[idx]["downloaded"] = cds.downloadRemoteFile(
                    download_url, destination, self.verifyssl
                )

            else:
                log.debug(
                    f'Skipped downloading of {sourcefile["origin"]["filename"]} from              '
                    f'       {sourcefile["origin"]["url"]}..'
                )
        return files

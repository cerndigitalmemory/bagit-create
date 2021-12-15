# Runs bibdocfile and parses its output to get details on CDS records

import subprocess
import logging
import ntpath

log = logging.getLogger("bic-basic-logger")


def run(resid, ssh_host=None):
    if len(resid) < 16 and resid.isdecimal():

        command = [
            "/opt/cdsweb/bin/bibdocfile",
            "--get-info",
            f"--recid={resid}",
        ]
        if ssh_host:
            command = ["ssh", ssh_host] + command

        log.warning(f"Running {command}")

        proc = subprocess.run(
            command,
            # Don't spawn another shell to run the command because of
            # https://stackoverflow.com/questions/46451466/python-subprocess-doesnt-work-with-ssh-command
            shell=False,
            # After 5 seconds, just give up
            timeout=5000,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        output = proc.stdout.decode()
        return output
    else:
        raise Exception("Bad or malformed input to bibdocfile")


def parse(output, resid):

    # Keys we want to save
    keys = ["fullpath", "checksum", "name", "fullname", "url", "fullurl", "size"]

    files = []

    file = None

    for line in output.splitlines():
        # Only consider lines starting with the Record ID
        # to ignore warning/unrelated output
        if line.startswith(resid):
            # Split them
            parsed_fields = line.split(":")
            # Here's a new file, so save the last parsed one
            for field in parsed_fields:
                if "fullpath" in field or line == output.splitlines()[-1]:
                    if file:
                        # Remap values according to the File schema
                        file_obj["origin"] = {}
                        file_obj["origin"]["fullpath"] = file["fullpath"]
                        file_obj["origin"]["path"] = ""
                        file_obj["origin"]["filename"] = ntpath.basename(
                            file["fullpath"]
                        )
                        if "name" in file:
                            file_obj["origin"]["fullname"] = file["name"]
                        if "fullurl" in file and "url" in file:
                            file_obj["origin"]["url"] = [file["fullurl"], file["url"]]
                        elif "url" in file:
                            file_obj["origin"]["url"] = file["url"]

                        file_obj["origin"]["checksum"] = file["checksum"]
                        file_obj["checksum"] = file["checksum"]

                        file_obj[
                            "bagpath"
                        ] = f'data/content/{file_obj["origin"]["filename"]}'

                        if "size" in file:
                            file_obj["size"] = file["size"]

                        file_obj["downloaded"] = False

                        print("Appending", file_obj)
                        files.append(file_obj)

                    # Create a new File
                    file = {}
                    file_obj = {"metadata": False}

            for key in keys:
                if key == "fullpath":
                    ext = parsed_fields[3]
                if parsed_fields[4].startswith(key):
                    file[key] = parsed_fields[4].replace(f"{key}=", "")
                    if key == "checksum":
                        file["checksum"] = "md5:" + file["checksum"]
                    # name -> filename
                    if key == "name":
                        file["filename"] = f'{file.pop("name")}{ext}'
                    if key == "url" or key == "fullurl":
                        file[key] = f"{parsed_fields[4]}:{parsed_fields[5]}".replace(
                            f"{key}=", ""
                        )

    return files

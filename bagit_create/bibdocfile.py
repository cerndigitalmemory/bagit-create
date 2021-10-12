# Runs bibdocfile and parses its output to get details on CDS records

import subprocess
import logging


def run(resid, ssh_host=None):
    if len(resid) < 16 and resid.isdecimal():

        command = [
            "/opt/cdsweb/bin/bibdocfile",
            "--get-info",
            f"--recid={resid}",
        ]
        if ssh_host:
            command = ["ssh", ssh_host] + command

        logging.warning(f"KRunning {command}")

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
        raise Exception('Bad or malformed input to bibdocfile')


def parse(output, resid):
    metadata = {}

    keys = ["fullpath", "checksum", "name", "fullname", "url", "fullurl"]

    files = []

    file = None

    for line in output.splitlines():
        # Only consider lines starting with the Record ID
        # to ignore warning/unrelated output
        if line.startswith(resid):
            # Split them
            parsed_fields = line.split(":")
            # Here's a new file
            for field in parsed_fields:
                if "fullpath" in field:
                    if file:
                        # Append the last one to files
                        files.append(file)
                        file = {}
                    else:
                        file = {}

            for key in keys:
                if key == "fullpath":
                    ext = parsed_fields[3]
                if parsed_fields[4].startswith(key):
                    file[key] = parsed_fields[4].replace(
                        f"{key}=", ""
                    )
                    if key == "checksum":
                        file["checksum"] = (
                            "md5:" + file["checksum"]
                        )
                    # name -> filename
                    if key == "name":
                        file[
                            "filename"
                        ] = f'{file.pop("name")}{ext}'
                    if key == "url" or key=="fullurl":
                        file[key] = f"{parsed_fields[4]}:{parsed_fields[5]}"

    return files


def get_files_metadata(resid, ssh_host=None):
    output = run(resid, ssh_host)
    return parse(output, resid)

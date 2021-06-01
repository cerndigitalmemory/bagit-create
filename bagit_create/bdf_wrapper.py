# Runs bibdocfile and parses its output to get details on CDS records

import subprocess
import logging






def run_bibdoc(resid, ssh_host=None):
    if len(resid) < 16 and resid.isdecimal():

        command = [
                "/opt/cdsweb/bin/bibdocfile",
                "--get-info",
                f"--recid={resid}",
            ]
        if ssh_host:
             command = [ "ssh", ssh_host ] + command

        logging.warning(f"Running {command}")

        proc = subprocess.run(
            command,
            # Don't spawn another shell to run the command because of
            # https://stackoverflow.com/questions/46451466/python-subprocess-doesnt-work-with-ssh-command
            shell=False,
            # After 5 seconds, just give up
            timeout=5000,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        output = proc.stdout.decode()

        metadata = {}

        keys = ["fullpath","checksum", "name"]

        for line in iter(output.splitlines()):
            # Only consider lines starting with the Record ID
            # to ignore warning/unrelated output
            if line.startswith(resid):
                # Split them
                parsed_metadata = line.split(":")
                if parsed_metadata[0] == resid:
                    file_id = parsed_metadata[1]
                    if file_id not in metadata:
                        metadata[file_id] = {}

                    for key in keys:
                        if key == "fullpath":
                            metadata[file_id]["ext"] = parsed_metadata[3]
                        if parsed_metadata[4].startswith(key):
                            metadata[file_id][key] = parsed_metadata[4].replace(f"{key}=",'')
        
        # Convert from key-form to array of files
        metadata_list = []
        for file in metadata:
            metadata_list.append(metadata[file])

        return metadata_list

# print(run_bibdoc(resid, ssh_host="cds-archive-01"))
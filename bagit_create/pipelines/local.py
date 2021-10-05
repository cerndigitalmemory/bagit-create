from posixpath import relpath
from . import base
import logging
import fs
from fs import open_fs
from fs import copy
import json
import os
from os import walk
import checksumdir
from datetime import datetime

log = logging.getLogger("basic-logger")


class LocalV1Pipeline(base.BasePipeline):
    def __init__(self, src):
        log.info(f"Local v1 pipeline initialised.\nLocal source: {src}")
        self.src = src

    def scan_files(self, src):
        """
        Walks through the source folder and prepare the "files" object
        """

        log.info("Scanning source folder..")
        files = []
        # Walk through the whole directory and prepare an object for each found file
        for (dirpath, dirnames, filenames) in walk(src):
            relpath = dirpath[len(src) - len(dirpath) + 1 :]
            for file in filenames:
                obj = {}
                obj["filename"] = file
                ## If you are in the root directory just use filename
                if dirpath == src:
                    obj["path"] = f"{file}"
                # Otherwise prepare the relative path
                else:
                    obj["path"] = f"{relpath}/{file}"

                obj["abs_path"] = f"{dirpath}/{file}"
                obj["localpath"] = f"data/content/{obj['path']}"

                obj["metadata"] = False
                obj["downloaded"] = False

                files.append(obj)

        # I don't have metadata json here. If we add, fields will be added here
        return files

    def copy_files(self, files, source_dir, dest_dir):
        my_fs = open_fs("/")
        fs.copy.copy_dir(
            src_fs=my_fs,
            dst_fs=my_fs,
            src_path=f"{source_dir}",
            dst_path=f"{dest_dir}",
        )
        for file in files:
            file["downloaded"] = True
        return files

    # needed in case we use the folder name
    def get_local_folder_name(self, src):
        splitted = src.split("/")
        folder_name = ""
        for i in splitted[:-1]:
            if len(i) > 0:
                folder_name += i
                folder_name += "_"
        folder_name += splitted[-1]
        return folder_name

    # gwts the checksum
    def get_folder_checksum(self, src):
        folder_checksum = checksumdir.dirhash(src)
        return folder_checksum

    def create_manifests(self, files, base_path):
        algs = ["md5", "sha1"]
        for alg in algs:
            log.info(f"Generating manifest {alg}..")
            content = self.generate_manifest(files, alg, base_path)
            self.write_file(content, f"{base_path}/manifest-{alg}.txt")

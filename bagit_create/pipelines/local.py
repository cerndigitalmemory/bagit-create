from posixpath import relpath
from . import base
import logging
import fs
from fs import open_fs
from fs import copy
import json
import os
import ntpath
from os import stat
from pwd import getpwuid
from os import walk
import checksumdir

log = logging.getLogger("basic-logger")


class LocalV1Pipeline(base.BasePipeline):
    def __init__(self, src):
        log.info(f"Local v1 pipeline initialised.\nLocal source: {src}")
        self.src = src

    def scan_files(self, src, secure_flag):
        """
        Walks through the source folder and prepare the "files" object
        """

        log.info("Scanning source folder..")
        files = []
        # Base name for the local source folder e.g. for /home/user/Pictures the base_name is Pictures
        base_name = os.path.basename(os.path.normpath(src))
        # The userSourcePath is the path before Pictures e.g. for /home/user/Pictures is /home/user
        userSourcePath = src[: len(src) - len(base_name) - 1]

        # If localsource is a file just get data from that file otherwise use the walk function
        if os.path.isfile(src):
            file = ntpath.basename(src)
            dirpath = ntpath.dirname(src)
            relpath = os.path.basename(os.path.normpath(dirpath))
            obj = self.get_local_metadata(
                file,
                src,
                dirpath,
                relpath,
                base_name,
                userSourcePath,
                secure_flag,
                isFile=True,
            )
            files.append(obj)
        else:
            # Walk through the whole directory and prepare an object for each found file
            for (dirpath, dirnames, filenames) in walk(src):
                relpath = os.path.basename(os.path.normpath(dirpath))
                for file in filenames:
                    obj = self.get_local_metadata(
                        file,
                        src,
                        dirpath,
                        relpath,
                        base_name,
                        userSourcePath,
                        secure_flag,
                        isFile=False,
                    )
                    files.append(obj)

        # I don't have metadata json here. If we add, fields will be added here
        return files

    def copy_files(self, files, source_dir, dest_dir):
        my_fs = open_fs("/")
        if os.path.isfile(source_dir):
            fs.copy.copy_file(
                src_fs=my_fs,
                dst_fs=my_fs,
                src_path=f"{source_dir}",
                dst_path=f"{dest_dir}/{ntpath.basename(source_dir)}",
            )
        else:
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

    # gets the checksum
    def get_local_checksum(self, src):
        if os.path.isfile(src):
            checksum = self.compute_hash(src)
        else:
            checksum = checksumdir.dirhash(src)
        return checksum

    # If the path is relative, return the absolute path
    def get_abs_path(self, src):
        if os.path.isabs(src):
            return src
        else:
            lc_src = os.getcwd() + "/" + src
            return lc_src

    def create_manifests(self, files, base_path):
        algs = ["md5", "sha1"]
        for alg in algs:
            log.info(f"Generating manifest {alg}..")
            content = self.generate_manifest(files, alg, base_path)
            self.write_file(content, f"{base_path}/manifest-{alg}.txt")

    def get_local_metadata(
        self, file, src, dirpath, relpath, base_name, userSourcePath, secure_flag, isFile
    ):
        obj = {}
        obj["filename"] = file
        ## If you are in the root directory just use filename
        if dirpath == src or isFile:
            obj["path"] = f"{file}"
        # Otherwise prepare the relative path
        else:
            obj["path"] = f"{relpath}/{file}"

        sourcePath = obj["path"]
        obj["sourcePath"] = f"{base_name}/{sourcePath}"
        obj["localpath"] = f"data/content/{obj['path']}"
        # If the secure path is not enabled. Get these fields:
        if not secure_flag:
            obj["userSourcePath"] = userSourcePath
            obj["sourceFullpath"] = f"{dirpath}/{file}"
            try:
                obj["creator"] = getpwuid(stat(f"{dirpath}/{file}").st_uid).pw_name
            except OSError:
                log.debug(f" Creator cannot be found. Skipping field. ")
        try:
            obj["size"] = os.path.getsize(f"{dirpath}/{file}")
        except OSError:
            log.debug(f" Size cannot be found. Skipping field. ")
        try:
            obj["date"] = os.path.getmtime(f"{dirpath}/{file}")
        except OSError:
            log.debug(f" Date cannot be found. Skipping field. ")
        obj["metadata"] = False
        obj["downloaded"] = False

        return obj

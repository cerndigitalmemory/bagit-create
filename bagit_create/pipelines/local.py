from posixpath import relpath
from . import base
import logging
import fs
from fs import open_fs
from fs import copy
import json
import os
import ntpath
from os import walk
import checksumdir
from os import stat
from pwd import getpwuid
from datetime import datetime


class LocalV1Pipeline(base.BasePipeline):
    def __init__(self, src):
        logging.info(f"Local v1 pipeline initialised.\nLocal source: {src}")
        self.src = src

    def scan_files(self, src, abs_flag):
        """
        Walks through the source folder and prepare the "files" object
        """

        logging.info("Scanning source folder..")
        files = []
        
        if os.path.isfile(src):
            file = ntpath.basename(src)
            dirpath = ntpath.dirname(src)

            obj = {}
            obj["filename"] = file
            obj["path"] = file
            obj["meaningfulSourcePath"] = file
            if not abs_flag:
                obj["meaninglessSourcePath"] = dirpath
                obj["sourceFullpath"] = f"{dirpath}/{file}"
                obj["creator"] = getpwuid(stat(f"{dirpath}/{file}").st_uid).pw_name
            else:
                obj["meaninglessSourcePath"] = ""
                obj["sourceFullpath"] = ""
                obj["creator"] = ""
            #obj["localpath_2"] = f"data/content/{base_name}/{obj['path']}"
            obj["localpath"] = f"data/content/{obj['path']}"

            obj["size"] = os.path.getsize(f"{dirpath}/{file}")
            obj["date"] = (os.path.getmtime(f"{dirpath}/{file}"))

            obj["metadata"] = False
            obj["downloaded"] = False

            files.append(obj)

        else:
            # Base name for the local source folder e.g. for /home/user/Pictures the base_name is Pictures
            base_name = os.path.basename(os.path.normpath(src))
            # The meaninglessSourcePath is the path before Pictures e.g. for /home/user/Pictures is /home/user
            meaninglessSourcePath = src[:len(src)-len(base_name) - 1]
            # Walk through the whole directory and prepare an object for each found file
            for (dirpath, dirnames, filenames) in walk(src):
                relpath = os.path.basename(os.path.normpath(dirpath))
        
                for file in filenames:
                    obj = {}
                    obj["filename"] = file
                    ## If you are in the root directory just use filename
                    if dirpath == src:
                        obj["path"] = f"{file}"
                    # Otherwise prepare the relative path
                    else:
                        obj["path"] = f"{relpath}/{file}"

                    srcPath =  obj["path"]
                    obj["meaningfulSourcePath"] = f"{base_name}/{srcPath}"
                    if not abs_flag:
                        obj["meaninglessSourcePath"] = meaninglessSourcePath
                        obj["sourceFullpath"] = f"{dirpath}/{file}"
                        obj["creator"] = getpwuid(stat(f"{dirpath}/{file}").st_uid).pw_name
                    else:
                        obj["meaninglessSourcePath"] = ""
                        obj["sourceFullpath"] = ""
                        obj["creator"] = ""
                    obj["localpath_2"] = f"data/content/{base_name}/{obj['path']}"
                    obj["localpath"] = f"data/content/{obj['path']}"

                    obj["size"] = os.path.getsize(f"{dirpath}/{file}")
                    obj["date"] = (os.path.getmtime(f"{dirpath}/{file}"))

                    obj["metadata"] = False
                    obj["downloaded"] = False

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
                dst_path=f"{dest_dir}",
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

    # gwts the checksum
    def get_folder_checksum(self, src):
        if os.path.isfile(src):
            checksum = self.compute_hash(f"{src}", "md5")
        else:
            checksum = checksumdir.dirhash(src)
        return checksum

    def create_manifests(self, files, base_path):
        algs = ["md5", "sha1"]
        for alg in algs:
            logging.info(f"Generating manifest {alg}..")
            content = self.generate_manifest(files, alg, base_path)
            self.write_file(content, f"{base_path}/manifest-{alg}.txt")

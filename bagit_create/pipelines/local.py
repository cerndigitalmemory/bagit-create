import hashlib
import logging
import ntpath
import os
import shutil
from os import stat, walk
from pwd import getpwuid

from . import base

log = logging.getLogger("bic-basic-logger")


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
        # Base name for the local source folder e.g. for /home/author/Pictures the base_name is Pictures
        base_name = os.path.basename(os.path.normpath(src))
        # The authorSourcePath is the path before Pictures e.g. for /home/author/Pictures is /home/author
        authorSourcePath = src[: len(src) - len(base_name) - 1]

        # If source_path is a file just get data from that file otherwise use the walk function
        if os.path.isfile(src):
            file = ntpath.basename(src)
            dirpath = ntpath.dirname(src)
            obj = self.get_local_metadata(file, src, dirpath, isFile=True)
            files.append(obj)
        else:
            # Walk through the whole directory and prepare an object for each found file
            for (dirpath, dirnames, filenames) in walk(src, followlinks=True):
                for file in filenames:
                    obj = self.get_local_metadata(
                        file,
                        src,
                        dirpath,
                        isFile=False,
                    )
                    files.append(obj)
        return files

    def copy_files(self, files, source_dir, dest_dir):
        if os.path.isfile(source_dir):
            shutil.copy(
                f"{source_dir}",
                f"{dest_dir}/{ntpath.basename(source_dir)}",
            )
        else:
            for (dirpath, dirnames, filenames) in walk(source_dir, followlinks=True):

                if source_dir == dirpath:
                    target = dest_dir
                else:
                    dest_relpath = dirpath[len(source_dir) + 1:]
                    target = f"{dest_dir}/{dest_relpath}"
                    os.mkdir(target)

                for file in filenames:
                    shutil.copy(f"{os.path.abspath(dirpath)}/{file}", target)

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
    def get_local_recid(self, src, author):
        checksum = hashlib.md5((src + author).encode("utf-8")).hexdigest()
        return checksum

    # If the path is relative, return the absolute path
    def get_abs_path(self, src):
        if os.path.isabs(src):
            return src
        else:
            lc_src = os.path.abspath(src)
            return lc_src

    def create_manifests(self, files, base_path):
        algs = ["md5", "sha1"]
        for alg in algs:
            log.info(f"Generating manifest {alg}..")
            content, files = self.generate_manifest(files, alg, base_path)
            self.write_file(content, f"{base_path}/manifest-{alg}.txt")
        return files

    def get_local_metadata(self, file, src, dirpath, isFile):
        # Prepare the File object
        obj = {"origin": {}}

        obj["origin"]["filename"] = file
        # If you are in the root directory just use filename

        if dirpath == src or isFile:
            obj["origin"]["path"] = ""
            sourcePath = f"{file}"
        # Otherwise prepare the relative path
        else:
            relpath = dirpath[len(src) + 1:]
            obj["origin"]["path"] = relpath
            sourcePath = f"{relpath}/{file}"
        obj["origin"]["sourcePath"] = f"{os.path.abspath(dirpath)}/{file}"

        obj["bagpath"] = f"data/content/{sourcePath}"

        try:
            obj["size"] = os.path.getsize(f"{dirpath}/{file}")
        except OSError:
            log.debug(" Size cannot be found. Skipping field. ")
        try:
            obj["date"] = os.path.getmtime(f"{dirpath}/{file}")
        except OSError:
            log.debug(" Date cannot be found. Skipping field. ")
        try:
            obj["creator"] = getpwuid(stat(f"{dirpath}/{file}").st_uid).pw_name
        except OSError:
            log.debug(" Creator cannot be found. Skipping field. ")

        obj["metadata"] = False
        obj["downloaded"] = False

        return obj

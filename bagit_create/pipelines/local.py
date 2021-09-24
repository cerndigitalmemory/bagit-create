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



class LocalV1Pipeline(base.BasePipeline):

    def __init__(self, src):
        logging.info(f"Local v1 pipeline initialised.\nLocal source: {src}")
        self.src = src

    def get_parse_metadata(self, src):
        # Gets metadata and transforms to JSON
        logging.info("Parsing metadata..")
        files = []

        for (dirpath, dirnames, filenames) in walk(src):
            if dirpath == src:
                relpath = "/"
            else:
                relpath = dirpath[len(src) - len(dirpath):]            
            
            for found_files in filenames:
                obj = {}
                filename, file_extension = os.path.splitext(found_files)
                obj["filename"] = found_files
                obj["title"] = filename
                obj["path"] = f'{relpath}'
                obj["abs_path"] = f'{dirpath}/{found_files}'
                obj["localpath"] = f"data/content/{found_files}" 
                obj["content-type"] = file_extension
                obj["metadata"] =  False
                obj["downloaded"] = False

                files.append(obj)
        
        obj = {}

        obj["abs_path"] = src
        obj["metadata"] = True  # is metadata no files
        obj["downloaded"] = False
        obj["filename"] = "metadata.json"
        obj["localpath"] = f"data/meta/metadata.json"      
        
        files.append(obj)

        return files


    def move_local_files(self, files, files_base_path):
        logging.info(f"Moving {len(files)} files to {files_base_path}..")
        my_fs = open_fs("/") 
        for file in files:
            if file["metadata"] == False:
                destination = f'{files_base_path}/{file["filename"]}'
                source = file["abs_path"]
                my_fs.copy(source, destination)
                file["downloaded"] = True

    def get_local_folder_name(self, src):
        splitted = src.split("/")
        folder_name = ""
        for i in splitted[:-1]:
            if len(i) > 0:
                folder_name+=i
                folder_name+="_"
        folder_name+=splitted[-1]
        return folder_name

    def get_folder_checksum(self, src):
        folder_checksum = checksumdir.dirhash(src)
        return folder_checksum


    def create_manifests(self, files, base_path):
        algs = ["md5", "sha1"]
        for alg in algs:
            logging.info(f"Generating manifest {alg}..")
            content = self.generate_manifest(files, alg, base_path)
            self.write_file(content, f"{base_path}/manifest-{alg}.txt")
            
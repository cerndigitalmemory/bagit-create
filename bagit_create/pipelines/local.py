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
        # Gets source folder and makes the file
        logging.info("Parsing metadata..")
        files = []
        #walk through the whole directory and get append all file names in the list
        for (dirpath, dirnames, filenames) in walk(src):
            relpath = dirpath[len(src) - len(dirpath) + 1:]                  
            for found_files in filenames:
                obj = {}
                filename, file_extension = os.path.splitext(found_files)
                obj["filename"] = found_files
                obj["title"] = filename
                if dirpath == src:
                    obj["path"] = f'{found_files}'
                else:
                    obj["path"] = f'{relpath}/{found_files}'
                obj["abs_path"] = f'{dirpath}/{found_files}'
                obj["localpath"] = f"data/content/{found_files}" 
                obj["content-type"] = file_extension
                obj["metadata"] =  False
                obj["downloaded"] = False

                files.append(obj)
        

        # I don't have metadata json here. If we add, fields will be added here    
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

    #needed in case we use the folder name
    def get_local_folder_name(self, src):
        splitted = src.split("/")
        folder_name = ""
        for i in splitted[:-1]:
            if len(i) > 0:
                folder_name+=i
                folder_name+="_"
        folder_name+=splitted[-1]
        return folder_name

    #gwts the checksum
    def get_folder_checksum(self, src):
        folder_checksum = checksumdir.dirhash(src)
        return folder_checksum


    def create_manifests(self, files, base_path):
        algs = ["md5", "sha1"]
        for alg in algs:
            logging.info(f"Generating manifest {alg}..")
            content = self.generate_manifest(files, alg, base_path)
            self.write_file(content, f"{base_path}/manifest-{alg}.txt")
            
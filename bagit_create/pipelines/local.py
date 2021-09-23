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
    
    def get_folder_checksum(self, src):
        folder_checksum = checksumdir.dirhash(src)
        return folder_checksum

    def prepare_folders_ls(self, src, checksum, target=None, delimiter_str="::"):

        path = os.getcwd()

        splitted = src.split("/")
        dst_folder = ""
        for i in splitted[:-1]:
            if len(i) > 0:
                dst_folder+=i
                dst_folder+="_"
        dst_folder+=splitted[-1]
                
        timestamp = datetime.now().strftime("%Y%m%d_%I%M%S")

        delimiter_str = "::"

        # Prepare the base folder for the BagIt export
        #  e.g. "bagitexport::cds::42"
        search_name = f"bagitexport{delimiter_str}local{delimiter_str}{dst_folder}{delimiter_str}"
        base_name = f"bagitexport{delimiter_str}local{delimiter_str}{dst_folder}{delimiter_str}{timestamp}"
        base_path = f"{path}/{base_name}"
        name = base_name
        if target:
            path = f'{path}/{target}'
            folder_list = [name for name in os.listdir(target) if os.path.isdir(f'{target}/{name}') and search_name in name]
        else:
            #Check if folder exists
            folder_list = [name for name in os.listdir() if os.path.isdir(name) and search_name in name]

        
        if len(folder_list) == 0:
            os.mkdir(base_path)
            os.mkdir(f"{base_path}/data")
            os.mkdir(f"{base_path}/data/meta")
            os.mkdir(f"{base_path}/data/content")
            return (base_path, name) 

        else:
            for folder in folder_list:
                search_path = f"{path}/{folder}"
                if (os.path.isfile(f"{search_path}/data/meta/sip.json")):
                    f = open(f"{search_path}/data/meta/sip.json",)
                    data = json.load(f)

                    #Check if folder is the same
                    if(data["src"] == src):
                        print("Source already exists")

                        #Check if folder has remained the same
                        if(data["checksum"] == checksum):
                            print(f"Diectory {search_path} already exists and has remained the same")
                            raise FileExistsError
                        #If folder has been changed then make a new one
                        else:
                            print(f"Diectory {search_name} already exits but has changed. New directory is created!")
                            os.mkdir(base_path)
                            os.mkdir(f"{base_path}/data")
                            os.mkdir(f"{base_path}/data/meta")
                            os.mkdir(f"{base_path}/data/content")

                            return (base_path, name)

                #If folder has no sip.json then ????
                else:
                    return (base_path, name)
        

    
    def folder_creation(self, src, folder_checksum, base_path):
        metadata = {}
        metadata["src"] = src
        metadata["checksum"] = folder_checksum
        accepted_formats = [".jpg", ".pdf", ".mp4", ".png"]
        list_of_files = []
        my_fs = open_fs("/") 
        for (dirpath, dirnames, filenames) in walk(src):
            if dirpath == src:
                relpath = "/"
            else:
                relpath = dirpath[len(src) - len(dirpath):]            
            
            for files in filenames:
                folders = {}
                filename, file_extension = os.path.splitext(files)
                folders["title"] =  filename
                folders["path"] = relpath
                folders["file"] = files
                folders["type"] = file_extension

                list_of_files.append(folders)
                try:       
                    my_fs.copy(f"{dirpath}/{files}", f"{base_path}/data/content/{files}")
                except:
                    print(f"File {files} already exists")

        my_fs.close()   

        metadata["content_files"] = list_of_files


        with open('sip.json', 'w') as fp:
            json.dump(metadata, fp)

        fs.move.move_file(os.getcwd(), "sip.json", f"{base_path}/data/meta", "sip.json")



    def create_manifests_ls(self, base_path):
        algs = ["md5", "sha1"]
        for algorithm in algs:
            logging.info(f"Generating manifest {algorithm}..")
            contents = ""
            #add checksum of content folders
            for file in os.listdir(f"{base_path}/data/content"):
                path = f"{base_path}/data/content/{file}"
                logging.debug(f"No checksum available for {file}")
                logging.debug(f"Computing {algorithm} of {file}")
                checksum = self.compute_hash(f"{path}", algorithm)
                relpath = f"data/content/{file}"
                line = f"{checksum} {relpath}\n"
                contents += line
            #add sip.json and other files at meta
            for file in os.listdir(f"{base_path}/data/meta"):
                path = f"{base_path}/data/meta/{file}"
                logging.debug(f"No checksum available for {file}")
                logging.debug(f"Computing {algorithm} of {file}")
                checksum = self.compute_hash(f"{path}", algorithm)
                relpath = f"data/meta/{file}"
                line = f"{checksum} {relpath}\n"
                contents += line
            self.write_file(contents, f"{base_path}/manifest-{algorithm}.txt")
            
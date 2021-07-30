from . import base_pipeline
import logging
import os, requests, json
import configparser

def get_dict_value(dct, keys):
    for key in keys:
        try:
            dct = dct[key]
        except KeyError:
            logging.error("Key:" + key +" not found in dict: " + str(dct))
            return None
    return dct

"""Invenio V3 pipeline to query over HTTP protocol."""
class InvenioV3Pipeline(base_pipeline.BasePipeline):

    def __init__(self, source):
        self.headers = {"Content-Type": "application/json"}
        self.response_type = 'json'

        self.config_file = configparser.ConfigParser()
        self.config_file.read(os.path.join(os.path.dirname(__file__), 'invenio.ini'))
        self.config = None
        
        if len(self.config_file.sections()) == 0:
            logging.error("Could not read config file")

        for instance in self.config_file.sections():
            if instance == source:
                self.config = self.config_file[instance]
                self.base_endpoint = self.config['base_endpoint']
                #Some instances have the file endpoint separately where the parameters are the filenames
                self.has_file_base_uri = self.config.getboolean("has_file_base_uri")

        if not self.config:
            logging.error("No such Invenio instance: " + source)

    def get_metadata(self, recid):
        res = requests.get(self.base_endpoint + str(recid), headers = self.headers)

        self.recid = recid
        self.metadata_url = res.url
        self.metadata = json.loads(res.text)
        return self.metadata, self.metadata_url, res.status_code, "metadata.json"

    def create_manifests(self, files, base_path, files_base_path):
        alg = "md5"
        logging.info(f"Generating manifest {alg}..")
        content = self.generate_manifest(files, alg, files_base_path)
        self.write_file(content, f"{base_path}/manifest-{alg}.txt")

    def parse_metadata(self, metadata_filename):
        logging.debug("Parsing metadata..")

        files = self.get_fileslist()

        if self.has_file_baseuri():
            file_uri = self.get_file_baseuri()
            for sourcefile in files:
                filename = self.get_filename(sourcefile)
                sourcefile["url"] = file_uri + "/" + filename
                sourcefile["filename"] = filename
                sourcefile["path"] = filename
                sourcefile["remote"] = "HTTP"
                sourcefile["downloaded"] = False
                sourcefile["metadata"] = False
        else:
            for sourcefile in files:
                filename = self.get_filename(sourcefile)
                sourcefile = self.get_file_uri(sourcefile)
                sourcefile["filename"] = filename
                sourcefile["path"] = filename
                sourcefile["remote"] = "HTTP"
                sourcefile["downloaded"] = False
                sourcefile["metadata"] = False
        
        '''
        files.append(
            {
                "metadata": True,
                "filename": "metadata." + self.response_type,
                "localpath": f"data/{aic_name}/metadata." + self.response_type,
                "path": "metadata." + self.response_type,
                "url": self.metadata_url,
                "size": 1,
                "downloaded": True,
            }
        )
        '''

        logging.debug(f"Got {len(files)} files")
        return files

    def get_fileslist(self):
        key_list = self.config['files'].split(',')
        
        if self.config.getboolean("files_separately", fallback = False):
            res = requests.get(self.base_endpoint + str(self.recid) + "/files", headers = self.headers)

            if res.status_code != 200:
                logging.error(f'Getting files return status code {res.status_code}')
                return None
            else:
                data = json.loads(res.text)
                key_list = self.config['files'].split(',')
                
                return get_dict_value(data, key_list)
        else:
            return get_dict_value(self.metadata, key_list)

    def download_files(self, files, files_base_path):
        logging.info(f"Downloading {len(files)} files to {files_base_path}..")
        for sourcefile in files:
            if sourcefile["metadata"] == False:
                destination = f'{files_base_path}/{sourcefile["filename"]}'

                logging.debug(
                    f'Downloading {sourcefile["filename"]} from {sourcefile["url"]}..'
                )

                sourcefile["downloaded"] = self.download_file(sourcefile, destination)
            else:
                logging.debug(
                    f'Skipped downloading of {sourcefile["filename"]} from \
                    {sourcefile["url"]}..'
                )

        logging.warning("Finished downloading")

    def has_file_baseuri(self):
        return self.has_file_base_uri

    def get_file_baseuri(self):
        key_list = self.config['file_uri'].split(',')

        return get_dict_value(self.metadata, key_list)

    def get_file_uri(self, file):
        key_list = self.config['file_uri'].split(',')
        file["url"] = get_dict_value(file, key_list)

        #If the uri is nested than unnest it (Zenodo's case)
        if key_list[0] != 'url':
            file.pop(key_list[0])
        
        return file

    def get_filename(self, file):
            key_list = self.config['file_name'].split(',')

            return get_dict_value(file, key_list)




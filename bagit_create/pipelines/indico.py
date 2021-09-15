from . import base
import logging
import hashlib
import hmac
import time
import requests
import json
import ntpath
import os


try:
    from urllib.parse import urlencode
except ImportError:
    from urllib import urlencode

class IndicoV1Pipeline(base.BasePipeline):

    def __init__(self, base_url):
        logging.info(f"Indico v1 pipeline initialised.\nBase URL: {base_url}")
        self.base_url = base_url

    #get metadata according to indico api guidelines
    def get_metadata(self, search_id):

        #!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        #       API Keys must be changed 
        #!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        
        self.api_key = 'a9e2f652-bc6c-4de3-b892-543a39d71f5e'
        self.secret_key = '5b804a16-2199-4b06-81fb-195a67601e63'
        self.path = f"https://indico.cern.ch/export/event/{search_id}.json?detail=contributions&occ=yes&pretty=yes"
        self.params = {
            'limit': 123
        }
        self.persistent=False
        self.items = list(self.params.items()) if hasattr(self.params, 'items') else list(self.params)
        if self.api_key:
            self.items.append(('apikey', self.api_key))
        # if only_public:
        #     items.append(('onlypublic', 'yes'))
        if self.secret_key:
            if not self.persistent:
                self.items.append(('timestamp', str(int(time.time()))))
            items = sorted(self.items, key=lambda x: x[0].lower())
            url = '%s?%s' % (self.path, urlencode(items))
            signature = hmac.new(self.secret_key.encode('utf-8'), url.encode('utf-8'),
                                hashlib.sha1).hexdigest()
            items.append(('signature', signature))
        if not items:
            return self.path

        search_link = '%s?%s' % (self.path, urlencode(items))
        #print(search_link)
        
        
        
        response = requests.get(search_link)
        metadata_filename = "metadata.json"
        return response.content, response.status_code, response.url, metadata_filename
    


    #Download Remote Folders at cwd
    def download_files(self, files, files_base_path):
        logging.info(f"Downloading {len(files)} files to {files_base_path}..")

        for sourcefile in files:
            if sourcefile["metadata"] == False:
                destination = f'{files_base_path}/{sourcefile["filename"]}'
                src = sourcefile["url"]
                r = requests.get(src)
                with open(destination, "wb") as f:
                    f.write(r.content)
                sourcefile["downloaded"] = True
        #return True


    def create_manifests(self, files, base_path, files_base_path):
        algs = ["md5", "sha1"]
        for alg in algs:
            # print("create manifests")
            # print(base_path, files_base_path)
            logging.info(f"Generating manifest {alg}..")
            content = self.generate_manifest(files, alg, files_base_path)
            self.write_file(content, f"{base_path}/manifest-{alg}.txt")


    def parse_metadata(self, metadataFilename):
        
        #Gets metadata and transforms to JSON

        logging.info("Parsing metadata..")
        files = []

        with open(metadataFilename) as jsonFile:
            metadataFile = json.load(jsonFile)
            jsonFile.close()
        
        
        for results in metadataFile['results']:
            for folders in results['folders'] :
                #Check for attachments
                for att in folders['attachments']:
                    obj = self.get_data_from_json(att)

                    if obj is not None:
                        if obj["filename"]:
                            files.append(obj)
                        else:
                            logging.warning(
                                f'Skipped entry. No basename found (probably an URL?)'
                            )

            for contributions in results['contributions']:
                for folders in contributions['folders']:
                    for att in folders['attachments']:
                        obj = self.get_data_from_json(att)
                        
                        if obj is not None:
                            if obj["filename"]:
                                files.append(obj)
                            else:
                                logging.warning(
                                    f'Skipped entry. No basename found (probably an URL?)'
                                )
        
                


            # add extra metadata
            obj = {}
            
            if 'title' in results['title']:
                obj['title'] = results['title']

            if 'startDate' in results:
                obj['startDate'] = results['startDate']
                
            if 'endDate' in results:
                obj['endDate'] = results['endDate']
                
            if 'room' in results:
                obj['room'] = results['room']
                
            if 'location' in results['location']:
                obj['location'] = results['location']
            

                
            
                
            obj["metadata"] = True #is metadata no files
            obj["downloaded"] = False
            obj["localpath"] = f"data/{self.aic_name}/metadata.json"
            #obj["localsavepath"] = f"{self.base_path}/data/{self.aic_name}"
                
                
            files.append(obj)
        return files

    
    def get_data_from_json(self, att):
        obj = {}

        obj["size"] = 0

        if 'link_url' in att:
            return None
        if 'size' in att:
            obj['size'] = att['size']
        if 'download_url' in att:
            obj['url'] = att['download_url']
            obj["filename"] = ntpath.basename(obj["url"]) 
            obj["path"] = obj["filename"]
        if 'title' in att:
            obj['title'] = att['title']
        if 'content_type' in att:
            obj['content_type'] = att['content_type']

            #obj['url'] = att['link_url']
            #obj["filename"] = att['link_url'].rsplit('/', 2)[2]+".html"
            #obj["path"] = obj["filename"]
            

        obj["metadata"] = False
        obj["downloaded"] = False
        obj["localpath"] = f"data/{self.aic_name}/metadata.json"
        #obj["localsavepath"] = f"{self.base_path}/data/{self.aic_name}"
        
        return obj

        

            




        
        



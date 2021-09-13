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

#get metadata according to indico api guidelines
def getMetadata(path, params, api_key=None, secret_key=None, only_public=False, persistent=False):
    items = list(params.items()) if hasattr(params, 'items') else list(params)
    if api_key:
        items.append(('apikey', api_key))
    if only_public:
        items.append(('onlypublic', 'yes'))
    if secret_key:
        if not persistent:
            items.append(('timestamp', str(int(time.time()))))
        items = sorted(items, key=lambda x: x[0].lower())
        url = '%s?%s' % (path, urlencode(items))
        signature = hmac.new(secret_key.encode('utf-8'), url.encode('utf-8'),
                             hashlib.sha1).hexdigest()
        items.append(('signature', signature))
    if not items:
        return path
    return '%s?%s' % (path, urlencode(items))

#Download Remote Folders at cwd
def downloadRemoteFile(src, dest):
    r = requests.get(src)
    with open(dest, "wb") as f:
        f.write(r.content)
    return True


def parseMetadata(metadataFilename):
    
    #Gets metadata and transforms to JSON
    metadataFile = json.loads(response.content)
    files = []
    
    for results in metadataFile['results']:
        for folders in results['folders']:
            #Check for attachments
            for att in folders['attachments']:
                obj = {}
        
                # Unknown size fallback
                obj["size"] = 0

                #Take useful information
                if 'size' in att:
                    obj['size'] = att['size']
                if 'download_url' in att:
                    obj['url'] = att['download_url']
                    obj["filename"] = ntpath.basename(obj["url"])
                    #print(ntpath.basename(obj["download_url"]))
                    obj["path"] = obj["filename"]
                if 'title' in att:
                    obj['title'] = att['title']
                if 'content_type' in att:
                    obj['content_type'] = att['content_type']
                if 'link_url' in att:
                        obj['url'] = att['link_url']

                
                obj["metadata"] = False
                obj["downloaded"] = False

                #if there is a filename append to the folder
                if obj["filename"]:
                    files.append(obj)
                else:
                    logging.warning(
                        f'Skipped entry "{f}". No basename found (probably an URL?)'
                    )
        for contributions in results['contributions']:
            for folders in contributions['folders']:
                for att in folders['attachments']:
                    obj = {}

                    '''
                    
                    SHOULD DO!

                    Merge the functions above and below into one


                    '''
                    obj["size"] = 0
                    if 'size' in att:
                        obj['size'] = att['size']
                    if 'download_url' in att:
                        obj['url'] = att['download_url']
                        obj["filename"] = ntpath.basename(obj["url"]) 
                        #print(ntpath.basename(obj["download_url"]))
                        obj["path"] = obj["filename"]
                    if 'title' in att:
                        obj['title'] = att['title']
                        print(att['title'])
                    if 'content_type' in att:
                        obj['content_type'] = att['content_type']
                    if 'link_url' in att:
                        obj['url'] = att['link_url']
                        #obj["filename"] = att['link_url'].rsplit('/', 2)[2]+".html"
                        #obj["path"] = obj["filename"]
                        

                    obj["metadata"] = False
                    obj["downloaded"] = False

                    if obj["filename"]:
                        files.append(obj)
                    else:
                        logging.warning(
                            f'Skipped entry "{f}". No basename found (probably an URL?)'
                        )

        # add extra metadata
        obj = {}
        
        if 'startDate' in results:
            obj['startDate'] = results['startDate']
            
        if 'endDate' in results:
            obj['endDate'] = results['endDate']
            
        if 'room' in results:
            obj['room'] = results['room']
            
        if 'location' in results['location']:
            obj['location'] = results['location']
            
        obj["metadata"] = True
        obj["downloaded"] = False
            
            
        files.append(obj)
    return files

#Download files
def filesDownload(files, files_base_path):

    for sourcefile in files:
        if sourcefile["metadata"] == False:
            destination = f'{files_base_path}/{sourcefile["filename"]}'

            sourcefile["downloaded"] = downloadRemoteFile(
                sourcefile["url"],
                destination,
                )
        else:
            print('Download skipped'
            )
        
        

if __name__ == '__main__':

    #Set the ID [Must be added to the CLI]
    search_id = '1017679'

    #!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    #       API Keys must be changed 
    #!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    API_KEY = 'a9e2f652-bc6c-4de3-b892-543a39d71f5e'
    SECRET_KEY = '5b804a16-2199-4b06-81fb-195a67601e63'
    PATH = f"https://indico.cern.ch/export/event/{search_id}.json?detail=contributions&occ=yes&pretty=yes"
    PARAMS = {
        'limit': 123
    }

    search_link = getMetadata(PATH, PARAMS, API_KEY, SECRET_KEY)
    print(search_link)
    
    response = requests.get(search_link)
    #return response.status_code, response.content, response.url
    
    files = parseMetadata(response)
    
    files_base_path = os.getcwd()
    
    filesDownload(files, files_base_path)
    



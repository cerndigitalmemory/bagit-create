from fs import open_fs
import fs
import os
import time
import random
import string
import cds
import cod
import click
import json
import copy

my_fs = open_fs(".")

def get_random_string(length):
    """
    Get a random string of the desired length
    """
    letters = string.ascii_lowercase
    result_str = "".join(random.choice(letters) for i in range(length))
    return result_str


# Stub


def createBagItTxt(path, version="1.0", encoding="UTF-8"):
    """
    Creates the Bag Declaration file, as specified by the RFC:
    https://tools.ietf.org/html/rfc8493#section-2.1.1
    """
    bagittxt = (f'BagIt-Version: {version}\n'
                f'Tag-File-Character-Encoding: {encoding}')
    my_fs.writetext(path + "/" + "bagit.txt", bagittxt)

def checkunique(id):
    """
    Check if the given ID is unique in our system
    """
    print("ID is unique")
    return True


def generateReferences(filepathslist):
    """
    Given a list of file paths, compute hashes and generate a
    HASH FILEPATH
    ...
    file
    """
    references = ""
    for filename in filepathslist:
        filehash = getHash(filename)
        line = filehash + " " + filename
        references += line
        references += "\n"
    return references


def getHash(filename, alg="md5"):
    """
    Compute hash of a given file
    """
    computedhash = my_fs.hash(filename, alg) + get_random_string(5)
    # TODO: remove the random part with real payloads
    return computedhash

@click.command()
@click.option("--foldername", default="1", help="ID of the resource")
@click.option("--method", help="Processing method to use")
@click.option("--skip_downloads", help="Creates files but skip downloading the actual payloads", default=False, is_flag=True)
def process(skip_downloads, foldername, method, timestamp=0, requestedFormat="MP4"):
    # Check if the target name is actually unique
    checkunique(foldername)

    # Save the plain resource ID
    resid = copy.copy(foldername)

    # Prepend the system name
    foldername = method + foldername

    # Get current path
    path = os.getcwd()

    # Prepare the high level folder which will contain the AIC and the AIUs
    baseexportpath = "bagitexport_" + foldername
    os.mkdir(path + "/" + baseexportpath)

    # If we're on CDS1 pipeline, create also the source folder,
    #  since we will need to fetch the raw files
    if method == "cds" or method =="cod":
        os.mkdir(path + "/" + foldername)

    # Create the AIC folder (ResourceID_timestamp)
    if timestamp == 0:
        print("No timestamp provided. Using 'now'")
        timestamp = int(time.time())
    aicfoldername = baseexportpath + "/" + foldername + "_" + str(timestamp)
    print("AIC folder name is", aicfoldername)
    os.mkdir(path + "/" + aicfoldername)

    # CERN CDS Pipeline
    if method == "cds":
        print("Fetching the CDS Resource", resid)

        # Get and save metadata
        metadata = cds.getMetadata(resid)
        open(path + "/" + foldername + "/" + "metadata.xml", "wb").write(metadata)
        print("Getting source files locations")

        # From the metadata, extract info about the upstream file sources
        files = cds.getRawFilesLocs(foldername + "/metadata.xml")
        print("Got", len(files), "sources")
        print("Looking for an", requestedFormat, "file..")
        for sourcefile in files:
            if sourcefile["filetype"] == requestedFormat:
                destination = path + "/" + foldername + "/" + sourcefile["filename"]
                print("Downloading", sourcefile["url"], "to", destination)
                if skip_downloads:
                    filedata = b"FILEDATA DOWNLOAD SKIPPED. If you need the real payloads, remove the --skipdownloads flag."
                    open(destination, 'wb').write(filedata)
                    print("skipped download")
                else:
                    filedata = cds.downloadRemoteFile(
                        sourcefile["url"],
                        destination,
                    )
                

    # CERN Open Data pipeline
    if method == "cod":
        # Get and save metadata about the requested resource
        metadata = cod.getMetadata(foldername)
        open(path + "/" + foldername + "/" + "metadata.json", "w").write(json.dumps(metadata))

    # Prepare AIC
    filelist = []

    for el in my_fs.scandir(foldername):
        if el.is_dir:
            for file in my_fs.listdir(foldername + "/" + el.name):
                filepath = foldername + "/" + el.name + "/" + file
                filelist.append(filepath)
        else:
            filepath = foldername + "/" + el.name
            filelist.append(filepath)

    # Look for high-level metadata and copy it into the AIC
    metadatafilenames = ["metadata.json", "metadata.xml"]

    for filename in metadatafilenames:
        if filename in my_fs.listdir(foldername):
            my_fs.copy(foldername + "/" + filename, aicfoldername + "/" + filename)
            filelist.remove(foldername + "/" + filename)

    print("Filelist:", filelist)

    references = generateReferences(filelist)

    my_fs.writetext(aicfoldername + "/" + "references.txt", references)

    # AIUs
    for file in filelist:
        filehash = getHash(file)
        aiufoldername = baseexportpath + "/" + foldername + "_" + filehash
        os.mkdir(aiufoldername)
        my_fs.copy(file, aiufoldername + "/" + fs.path.basename(file))

    createBagItTxt(baseexportpath)
    return baseexportpath

if __name__ == "__main__":
    process()

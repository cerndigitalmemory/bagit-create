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
    bagittxt = f"BagIt-Version: {version}\n" f"Tag-File-Character-Encoding: {encoding}"
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
@click.option("--recid", default="1", help="Unique ID of the record in the upstream source")
@click.option("--source", help="Select source pipeline")
@click.option(
    "--skip_downloads",
    help="Creates files but skip downloading the actual payloads",
    default=False,
    is_flag=True,
)
def process(skip_downloads, recid, source, timestamp=0):

    # Delimiter string
    delimiter_str = "::"

    # Check if the target name is actually unique
    checkunique(recid)

    # Save the plain resource ID
    resid = copy.copy(recid)

    # Prepend the system name and the delimiter
    recid = f"{source}{delimiter_str}{recid}"

    # Get current path
    path = os.getcwd()

    # Prepare the high level folder which will contain the AIC and the AIUs
    baseexportpath = f"bagitexport{delimiter_str}{recid}"
    os.mkdir(path + "/" + baseexportpath)

    # If we're on CDS1 pipeline, create also the source folder,
    #  since we will need to fetch the raw files
    if source == "cds" or source == "cod":
        os.mkdir(path + "/" + recid)

    # Create the AIC folder (ResourceID_timestamp)
    if timestamp == 0:
        print("No timestamp provided. Using 'now'")
        timestamp = int(time.time())
    aicfoldername = baseexportpath + "/" + recid + delimiter_str + str(timestamp)
    print("AIC folder name is", aicfoldername)
    os.mkdir(path + "/" + aicfoldername)

    # CERN CDS Pipeline
    if source == "cds":
        print("Fetching the CDS Resource", resid)

        # Get and save metadata
        metadata = cds.getMetadata(resid)
        open(path + "/" + recid + "/" + "metadata.xml", "wb").write(metadata)
        print("Getting source files locations")

        # From the metadata, extract info about the upstream file sources
        files = cds.getRawFilesLocs(recid + "/metadata.xml")
        print("Got", len(files), "files")
        for sourcefile in files:
            destination = path + "/" + recid + "/" + sourcefile["filename"]
            print(f'Downloading {sourcefile["filename"]} from {sourcefile["uri"]}..')
            if skip_downloads:
                filedata = b"FILEDATA DOWNLOAD SKIPPED. If you need the real payloads, remove the --skipdownloads flag."
                open(destination, "wb").write(filedata)
                print("skipped download") 
            elif sourcefile["remote"] == "HTTP":
                filedata = cds.downloadRemoteFile(
                        sourcefile["uri"],
                        destination,
                    )
            elif sourcefile["remote"] == "EOS":
                filedata = cds.downloadEOSfile(
                        sourcefile["uri"],
                        destination,
                    )

    # CERN Open Data pipeline
    if source == "cod":
        # Get and save metadata about the requested resource
        metadata = cod.getMetadata(recid)
        open(path + "/" + recid + "/" + "metadata.json", "w").write(
            json.dumps(metadata)
        )

    # Prepare AIC
    filelist = []

    for el in my_fs.scandir(recid):
        if el.is_dir:
            for file in my_fs.listdir(recid + "/" + el.name):
                filepath = recid+ "/" + el.name + "/" + file
                filelist.append(filepath)
        else:
            filepath = recid+ "/" + el.name
            filelist.append(filepath)

    # Look for high-level metadata and copy it into the AIC
    metadatafilenames = ["metadata.json", "metadata.xml"]

    for filename in metadatafilenames:
        if filename in my_fs.listdir(recid):
            my_fs.copy(recid + "/" + filename, aicfoldername + "/" + filename)
            filelist.remove(recid + "/" + filename)

    print("Harvested files:", filelist)

    references = generateReferences(filelist)

    my_fs.writetext(aicfoldername + "/" + "references.txt", references)

    # AIUs
    for file in filelist:
        filehash = getHash(file)
        aiufoldername = baseexportpath + "/" + recid + delimiter_str + filehash
        os.mkdir(aiufoldername)
        my_fs.copy(file, aiufoldername + "/" + fs.path.basename(file))

    createBagItTxt(baseexportpath)
    return baseexportpath


if __name__ == "__main__":
    process()

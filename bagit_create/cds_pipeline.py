import cds
import json

# CDS Resource ID
resourceID = 2272168

# Example pipeline
resourceID = str(resourceID)
record_metadata = cds.getMetadata(2272168)
open(resourceID +".xml", 'wb').write(record_metadata)
print("Wrote MARC21 metadata file")
files = cds.getRawFilesLocs("2272168.xml")

with open(resourceID +"_resources.json", 'w') as outfile:
	json.dump(files, outfile)
print("Wrote resources file")

print("Looking for MP4 file..")
for sourcefile in files:
	if sourcefile["filetype"] == "MP4":
		print("Downloading", sourcefile["url"])
		cds.downloadRemoteFile(sourcefile["url"], ".")



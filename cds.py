import requests
import xml.etree.ElementTree as ET
from pymarc import MARCReader, marcxml

import fs
my_fs = open_fs('/')

def getMetadata(record_id, type="xml"):
	"""
	Get MARC21 metadata from a CDS record ID
	Returns a string
	"""
	baseEndpoint = "http://cds.cern.ch/record/" + str(record_id)
	
	if type == "xml":
		of = "xm"
	elif type == "dc":
		of = "xd"

	payload = {'of': of}

	r = requests.get(baseEndpoint, params=payload)
	#filename = str(record_id) + "_metadata.xml"

	return r.content



def getRawFilesLocs(metadata_filename):
	# Parse the XML as MARC21
	#  and get the first record (result should be one record anyway)
	record = marcxml.parse_xml_to_array(metadata_filename)[0]
	# Look for 856 fields
	#  MARC21: 856 - Electronic Location and Access (R)
	rawData = []
	for f in record.get_fields('856'):
		if f["q"]:
			obj = {}
			obj.filetype = f["q"]
			if f["u"]:
				obj.url = f["u"] 
			elif f["d"]:
				obj.url = f["d"]
		
		print(f["q"], f["u"], f["d"])



def downloadRemoteFile(src, dest):
	r = requests.get(src)  
	with open(dest, 'wb') as f:
    	f.write(r.content)

def downloadEOSfile(src, dest):
	# todo

# Example pipeline
record_metadata = getMetadata(2272168)
open("2272168.xml", 'wb').write(record)
files = getRawFilesLocs("2272168.xml")

print(files)

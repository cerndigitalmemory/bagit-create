from fs import open_fs
import fs
my_fs = open_fs('.')
import os
import time

import random
import string

def get_random_string(length):
    letters = string.ascii_lowercase
    result_str = ''.join(random.choice(letters) for i in range(length))
    return result_str

# Stub
def checkunique(id):
	print("ID is unique")
	return True

def generateReferences(filepathslist):
	references = ""
	for filename in filepathslist:
		filehash = getHash(filename)
		line = filehash + " " + filename
		references += line
		references += "\n"
	return references

def getHash(filename):
	computedhash = my_fs.hash(filename, "md5") + get_random_string(5)
	return computedhash


def process(foldername, method, timestamp=0):
	checkunique(foldername)
	path = os.getcwd()
	baseexportpath = "bagitexport_"+foldername
	os.mkdir(path+"/"+baseexportpath)
	# Create AIC folder
	if timestamp == 0:
		print("No timestamp provided. Using 'now'")
		timestamp = int(time.time())
	aicfoldername = baseexportpath+"/"+foldername+"_"+str(timestamp)
	print("AIC folder name is", aicfoldername)
	os.mkdir(path+"/"+aicfoldername)
	
	# Prepare AIC
	filelist = []
	for el in my_fs.scandir(foldername):
		if el.is_dir:
			for file in my_fs.listdir(foldername+'/'+el.name):
				filepath = foldername + "/" + el.name + "/" + file
				filelist.append(filepath)
	references = generateReferences(filelist)
	my_fs.writetext(aicfoldername+"/"+'references.txt', references)
				
	# Look for high-level metadata and copy it into the AIC
	if "metadata.json" in my_fs.listdir(foldername):
		my_fs.copy(foldername + "/"+"metadata.json", aicfoldername+"/metadata.json")

	# AIUs
	for file in filelist:
		filehash = getHash(file)
		aiufoldername =baseexportpath+"/"+ foldername+"_"+filehash
		os.mkdir(aiufoldername)
		my_fs.copy(file, aiufoldername+"/"+fs.path.basename(file))


# standardarchive -> transfermode
process("photoid-2704179", "standardarchive", timestamp=12031239)

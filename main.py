from fs import open_fs
my_fs = open_fs('.')
import os

def process(foldername):
	path = os.getcwd()
	os.mkdir(path+"/"+foldername+"AA")
	# Prepare AIC

	# Prepare AIUs

	# Look for high-level metadata
	if "metadata.json" in my_fs.listdir(foldername):
		my_fs.copy(foldername + "/"+"metadata.json", foldername+"AA"+"/metadata.json")		


process("photoid-2704179")
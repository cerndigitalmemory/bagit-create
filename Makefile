clean:
	rm -rf *::*

updateremote:
	tsocks scp *.py am-dev-two:/root/bagit-create
	
copypackage:
	tsocks scp -r $(FOLDER) am-dev-two:/root/.am/ss-location-data/archivematica/bie-testdata


clean:
	rm -rf bagitexport_*
	rm -rf 2272168
	rm -rf 1

updateremote:
	tsocks scp *.py am-dev-two:/root/bagit-create
	
copypackage:
	tsocks scp -r $(FOLDER) am-dev-two:/root/.am/ss-location-data/archivematica/bie-testdata


import main

def cds(resourceid):
	return main.process(foldername=resourceid,method="cds"),200
	

def test():
    return "DONE", 200
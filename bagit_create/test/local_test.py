from ..pipelines import local
import tempfile
import json, os, pytest, ntpath, shutil
from os import mkdir, walk


def test_local_files():
    
    # Prepare the mock folders and expected result from file
    with tempfile.TemporaryDirectory() as tmpdir1:
        with tempfile.TemporaryDirectory(dir = tmpdir1) as tmpdir2:
            #Creates two temp directories and two files
            f1 = tempfile.NamedTemporaryFile('w+t',dir = tmpdir1)
            f1.write('Hello World. This is temp_1!') 
            f1.seek(0)

            f2 = tempfile.NamedTemporaryFile('w+t',dir = tmpdir2)
            f2.write('Hello World. This is temp_11!') 
            f2.seek(0)

            #Temp directory structure
            # - tmpdir1
            #   - f1
            #   - tmpdir2
            #       - f2
            # We want to check if the resulting files list will be the same  
            test_files = [
                {
                    'filename': ntpath.basename(f1.name), 
                    'path': ntpath.basename(f1.name), 
                    'meaningfulSourcePath': f'{ntpath.basename(tmpdir1)}/{ntpath.basename(f1.name)}',
                    'meaninglessSourcePath': f'/tmp',
                    'sourceFullpath': f'/tmp/{ntpath.basename(tmpdir1)}/{ntpath.basename(f1.name)}', 
                    'localpath': f'data/content/{ntpath.basename(f1.name)}', 
                    'metadata': False, 
                    'downloaded': False
                }, 
                {
                    'filename': ntpath.basename(f2.name), 
                    'path': f'{ntpath.basename(tmpdir2)}/{ntpath.basename(f2.name)}', 
                    'meaningfulSourcePath': f'{ntpath.basename(tmpdir1)}/{ntpath.basename(tmpdir2)}/{ntpath.basename(f2.name)}',
                    'meaninglessSourcePath': f'/tmp',
                    'sourceFullpath': f'/tmp/{ntpath.basename(tmpdir1)}/{ntpath.basename(tmpdir2)}/{ntpath.basename(f2.name)}', 
                    'localpath': f'data/content/{ntpath.basename(tmpdir2)}/{ntpath.basename(f2.name)}', 
                    'metadata': False, 
                    'downloaded': False
                }
            ] 
            print(ntpath.basename(tmpdir2))
            pipeline = local.LocalV1Pipeline(tmpdir1)

            files = pipeline.scan_files(tmpdir1)

            for file in files:
                file.pop("size")
                file.pop("creator")
                file.pop("date")
                file.pop("localpath_2")
            f1.close()
            f2.close()

    assert files == test_files

def test_copy_files():

    # Prepare the mock folders and expected result from file
    with tempfile.TemporaryDirectory() as tmpdir1:
        with tempfile.TemporaryDirectory(dir = tmpdir1) as tmpdir2:
            with tempfile.TemporaryDirectory() as destdir:
                #Creates two temp directories and two files
                f1 = tempfile.NamedTemporaryFile('w+t',dir = tmpdir1)
                f2 = tempfile.NamedTemporaryFile('w+t',dir = tmpdir2)

                #Temp directory structure
                # - tmpdir1
                #   - f1
                #   - tmpdir2
                #       - f2
                # We want to check if the two files will be moved at the destination folder  

                #Initial files structure
                test_files = [
                {
                    'filename': ntpath.basename(f1.name), 
                    'path': ntpath.basename(f1.name), 
                    'abs_path': f'/tmp/{ntpath.basename(tmpdir1)}/{ntpath.basename(f1.name)}', 
                    'localpath': f'data/content/{ntpath.basename(f1.name)}', 
                    'metadata': False, 
                    'downloaded': False
                }, 
                {
                    'filename': ntpath.basename(f2.name), 
                    'path': f'{ntpath.basename(tmpdir2)}/{ntpath.basename(f2.name)}', 
                    'abs_path': f'/tmp/{ntpath.basename(tmpdir1)}/{ntpath.basename(tmpdir2)}/{ntpath.basename(f2.name)}', 
                    'localpath': f'data/content/{ntpath.basename(tmpdir2)}/{ntpath.basename(f2.name)}', 
                    'metadata': False, 
                    'downloaded': False
                }
                ] 

                #Resulting files structure that we want to check (downloaded turns to True)
                test_result_files = [
                {
                    'filename': ntpath.basename(f1.name), 
                    'path': ntpath.basename(f1.name), 
                    'abs_path': f'/tmp/{ntpath.basename(tmpdir1)}/{ntpath.basename(f1.name)}', 
                    'localpath': f'data/content/{ntpath.basename(f1.name)}', 
                    'metadata': False, 
                    'downloaded': True
                }, 
                {
                    'filename': ntpath.basename(f2.name), 
                    'path': f'{ntpath.basename(tmpdir2)}/{ntpath.basename(f2.name)}', 
                    'abs_path': f'/tmp/{ntpath.basename(tmpdir1)}/{ntpath.basename(tmpdir2)}/{ntpath.basename(f2.name)}', 
                    'localpath': f'data/content/{ntpath.basename(tmpdir2)}/{ntpath.basename(f2.name)}', 
                    'metadata': False, 
                    'downloaded': True
                }
                ] 

                # Call the function
                pipeline = local.LocalV1Pipeline(tmpdir1)
                new_files = pipeline.copy_files(test_files,tmpdir1, destdir)
                
                # Flags to be checked
                f1_is_here = False
                f2_is_here = False
                dirpath_1 = f'/tmp/{ntpath.basename(destdir)}'
                dirpath_2 = f'/tmp/{ntpath.basename(destdir)}/{ntpath.basename(tmpdir2)}'
                dirpath_1_flag = False
                dirpath_2_flag = False

                for (dirpath, dirnames, filenames) in walk(destdir):
                
                    for i in filenames:
                        if ntpath.basename(f1.name) == i:
                            f1_is_here = True
                            if dirpath == dirpath_1:
                                dirpath_1_flag = True
                        if ntpath.basename(f2.name) == i:
                            f2_is_here = True
                            if dirpath == dirpath_2:
                                dirpath_2_flag = True
                

                f1.close()
                f2.close()
                
    #Check if file 1 and 2 have been moved
    assert f1_is_here == True, f2_is_here == True

    #Check if directories are correct
    assert dirpath_1_flag == True, dirpath_2_flag == True

    #Check if new files object has been changed correctly
    assert new_files == test_result_files


def test_sip_json():

    # Prepare the mock folders and expected result from file
    with tempfile.TemporaryDirectory() as tmpdir1:
        #Creates a temp directory and metadata path
        f1 = tempfile.NamedTemporaryFile('w+t',dir = tmpdir1)
        os.mkdir(f'{tmpdir1}/data')
        os.mkdir(f'{tmpdir1}/data/meta')
        test_files = [
                {
                    'filename': "temp_file1", 
                    'path': "temp_file1", 
                    'sourceFullpath': f'/tmp/{ntpath.basename(tmpdir1)}/{ntpath.basename(f1.name)}', 
                    'localpath': f'data/content/{ntpath.basename(f1.name)}', 
                    'metadata': False, 
                    'downloaded': True
                }
            ] 

        resulting_files = [
                {
                    'filename': 'temp_file1', 
                    'path': 'temp_file1', 
                    'sourceFullpath': f'/tmp/{ntpath.basename(tmpdir1)}/{ntpath.basename(f1.name)}', 
                    'localpath': f'data/content/{ntpath.basename(f1.name)}', 
                    'metadata': False, 
                    'downloaded': True
                }, 
                {
                    'filename': 'sip.json', 
                    'path': 'sip.json', 
                    'metadata': False, 
                    'downloaded': True, 
                    'localpath': 'data/meta/sip.json', 
                    'localsavepath': f'/tmp/{ntpath.basename(tmpdir1)}/data/meta'
                }
            ]

        sip_json_tuple_data = {'created_by': 'bagit-create 0.0.7.4', 
        'audit': [{'tool': 'BagIt Create tool 0.07', 
        'param': {'recid': 1000, 'source': 'local'}}], 
        'source': 'local', 
        'recid': 1000, 
        'metadataFile_upstream': 
        'metadata.json', 
        'contentFiles': 
            [{'filename': 'temp_file1', 
            'path': 'temp_file1', 
            'sourceFullpath': f'/tmp/{ntpath.basename(tmpdir1)}/{ntpath.basename(f1.name)}', 
            'localpath': f'data/content/{ntpath.basename(f1.name)}', 
            'metadata': False, 'downloaded': True}], 
            'sip_creation_timestamp': 0}
        pipeline = local.LocalV1Pipeline(f'/tmp/{ntpath.basename(tmpdir1)}')
        audit = [
                {
                    "tool": f"BagIt Create tool 0.07",
                    "param": {"recid": 1000, "source": "local"},
                }
            ]
        timestamp = 0
        base_path = f'/tmp/{ntpath.basename(tmpdir1)}'
        metadata_url = "metadata.json"

        files = pipeline.create_sip_meta(
                test_files, audit, timestamp, base_path, metadata_url
            )

        f = open(f'/tmp/{ntpath.basename(tmpdir1)}/data/meta/sip.json')
        json_data = json.load(f)

        f.close()
        f1.close()
        shutil.rmtree(f'{tmpdir1}/data')

    assert files == resulting_files
    
    assert sip_json_tuple_data == json_data





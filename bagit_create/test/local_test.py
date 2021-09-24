from ..pipelines import local
import tempfile
import json, os, pytest, ntpath
from os import walk


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
             
            test_file = [
                {
                    'filename': ntpath.basename(f1.name), 
                    'title': ntpath.basename(f1.name), 
                    'path': '/', 
                    'abs_path': f'/tmp/{ntpath.basename(tmpdir1)}/{ntpath.basename(f1.name)}', 
                    'localpath': f'data/content/{ntpath.basename(f1.name)}', 
                    'content-type': '', 
                    'metadata': False, 
                    'downloaded': False
                }, 
                {
                    'filename': ntpath.basename(f2.name), 
                    'title': ntpath.basename(f2.name), 
                    'path': f'/{ntpath.basename(tmpdir2)}', 
                    'abs_path': f'/tmp/{ntpath.basename(tmpdir1)}/{ntpath.basename(tmpdir2)}/{ntpath.basename(f2.name)}', 
                    'localpath': f'data/content/{ntpath.basename(f2.name)}', 
                    'content-type': '', 
                    'metadata': False, 
                    'downloaded': False
                }, 
                {
                    'abs_path': f'/tmp/{ntpath.basename(tmpdir1)}', 
                    'metadata': True, 
                    'downloaded': False, 
                    'filename': 'metadata.json', 
                    'localpath': 'data/meta/metadata.json'
                }
            ] 

            pipeline = local.LocalV1Pipeline(tmpdir1)

            files = pipeline.get_parse_metadata(tmpdir1)
            
            f1.close()
            f2.close()

    assert files == test_file

def test_local_move():
   
    # Prepare the mock folders and expected result from file
    with tempfile.TemporaryDirectory() as tmpdir1:
        with tempfile.TemporaryDirectory(dir = tmpdir1) as tmpdir2:
            with tempfile.TemporaryDirectory() as destdir:
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
                # We want to check if the two files will be moved at the destination folder  
                 
                test_file = [
                    {
                        'filename': ntpath.basename(f1.name), 
                        'title': ntpath.basename(f1.name), 
                        'path': '/', 
                        'abs_path': f'/tmp/{ntpath.basename(tmpdir1)}/{ntpath.basename(f1.name)}', 
                        'localpath': f'data/content/{ntpath.basename(f1.name)}', 
                        'content-type': '', 
                        'metadata': False, 
                        'downloaded': False
                    }, 
                    {
                        'filename': ntpath.basename(f2.name), 
                        'title': ntpath.basename(f2.name), 
                        'path': f'/{ntpath.basename(tmpdir2)}', 
                        'abs_path': f'/tmp/{ntpath.basename(tmpdir1)}/{ntpath.basename(tmpdir2)}/{ntpath.basename(f2.name)}', 
                        'localpath': f'data/content/{ntpath.basename(f2.name)}', 
                        'content-type': '', 
                        'metadata': False, 
                        'downloaded': False
                    }, 
                    {
                        'abs_path': f'/tmp/{ntpath.basename(tmpdir1)}', 
                        'metadata': True, 
                        'downloaded': False, 
                        'filename': 'metadata.json', 
                        'localpath': 'data/meta/metadata.json'
                    }] 

                pipeline = local.LocalV1Pipeline(tmpdir1)

                pipeline.move_local_files(test_file, destdir)

                for (dirpath, dirnames, filenames) in walk(destdir):
                    f1_is_here = False
                    f2_is_here = False
                    for i in filenames:
                        if ntpath.basename(f1.name) == i:
                            f1_is_here = True
                        if ntpath.basename(f1.name) == i:
                            f2_is_here = True
                
                f1.close()
                f2.close()

    assert f1_is_here, f2_is_here


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

            pipeline = local.LocalV1Pipeline(tmpdir1)

            files = pipeline.scan_files(tmpdir1)
            
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





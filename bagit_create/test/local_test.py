from ..pipelines import local
import tempfile
import json, os, pytest, ntpath


def test_create_temp_folders():

    
    
    # Prepare the mock folders and expected result from file
    with tempfile.TemporaryDirectory() as tmpdir1:
        print('created temporary directory', tmpdir1)
        with tempfile.TemporaryDirectory(dir = tmpdir1) as tmpdir2:
            print('created temporary directory', tmpdir2)
            
            f1 = tempfile.NamedTemporaryFile('w+t',dir = tmpdir1)
            #f1.name = "temp_1.txt"
            f1.write('Hello World. This is temp_1!') 
            f1.seek(0)

            f2 = tempfile.NamedTemporaryFile('w+t',dir = tmpdir2)
            #f2.name = "temp_11.txt"
            f2.write('Hello World. This is temp_11!') 
            f2.seek(0)

            local_pipeline = local.LocalV1Pipeline(tmpdir1)
            checksum = local_pipeline.get_folder_checksum(tmpdir1)
            base_path, name = local_pipeline.prepare_folders_ls(tmpdir1, checksum)

            local_pipeline.folder_creation(tmpdir1, checksum, base_path)

            with open(f"{base_path}/data/meta/sip.json", 'r') as sip_json:
                parsed = json.load(sip_json)
                print(parsed)

            parsed.pop("checksum")

            assert_json = {'src': f'{tmpdir1}', 
                    'content_files': [{'title': ntpath.basename(f1.name), 'path': '/', 'file': ntpath.basename(f1.name), 'type': ''}, 
                    {'title': ntpath.basename(f2.name), 'path': f'/{ntpath.basename(tmpdir2)}', 'file': ntpath.basename(f2.name), 'type': ''}]}

            f1.close()
            f2.close()
    local_pipeline.delete_folder(base_path)

    assert parsed == assert_json



from .. import main

a = [{"filename": "42.txt", "path": "8"}, {"filename": "47.txt", "path": "/opt/47"}]
b = [
    {"filename": "42.txt", "path": "/opt/42", "hash": "0"},
    {"filename": "43.txt", "path": "/opt/43", "hash": "4"},
]

c1 = [
    {"filename": "42.txt", "path": "8"},
    {"filename": "47.txt", "path": "/opt/47"},
    {"filename": "43.txt", "path": "/opt/43", "hash": "4"},
]

c2 = [
    {"filename": "42.txt", "path": "/opt/42", "hash": "0"},
    {"filename": "43.txt", "path": "/opt/43", "hash": "4"},
    {"filename": "47.txt", "path": "/opt/47"},
]


def test_mergelists():
    assert main.merge_lists(a, b, "filename") == c1


def test_mergelists_inverted():
    assert main.merge_lists(b, a, "filename") == c2


files = [
    {"url": "http://someurl.com/42.txt", "size": "123123", "path": "data/local/42.txt"},
    {"url": "http://someurl.com/43.txt", "size": "1024", "path": "data/local/43.txt"},
]

fetch_txt = """http://someurl.com/42.txt 123123 data/local/42.txt
http://someurl.com/43.txt 1024 data/local/43.txt

"""


def test_fetch_txt():
    assert main.generate_fetch_txt(files) == fetch_txt

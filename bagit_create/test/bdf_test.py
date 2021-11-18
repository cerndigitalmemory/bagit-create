from .. import bibdocfile

test_output = """
2751237:2276585:::status=
2751237:2276585:::basedir=/opt/cdsweb/var/data/files/g227/2276585
2751237:2276585:::creation date=2021-02-06 04:33:21
2751237:2276585:::modification date=2021-02-14 04:45:52
2751237:2276585:::text extraction date=2021-02-06 05:15:11
2751237:2276585:::total file attached=1
2751237:2276585:::total size latest version=3865962
2751237:2276585:::total size all files=3865962
2751237:2276585:1:.pdf:fullpath=/opt/cdsweb/var/data/files/g227/2276585/content.pdf;1
2751237:2276585:1:.pdf:name=2101.02245
2751237:2276585:1:.pdf:subformat=
2751237:2276585:1:.pdf:status=
2751237:2276585:1:.pdf:checksum=be99bc4762f1add866d8c08abb2e0657
2751237:2276585:1:.pdf:size=3865962
2751237:2276585:1:.pdf:creation time=2021-02-06 04:33:21
2751237:2276585:1:.pdf:modification time=2021-02-06 04:33:21
2751237:2276585:1:.pdf:magic=('PDF document, version 1.5', 'application/pdf; charset=binary', 'PDF document, version 1.5', 'application/pdf; charset=binary', 'application/pdf')
2751237:2276585:1:.pdf:mime=application/pdf
2751237:2276585:1:.pdf:encoding=None
2751237:2276585:1:.pdf:url=http://cds.cern.ch/record/2751237/files/2101.02245.pdf
2751237:2276585:1:.pdf:fullurl=http://cds.cern.ch/record/2751237/files/2101.02245.pdf?version=1
2751237:2276585:1:.pdf:description=Fulltext
2751237:2276585:1:.pdf:comment=None
2751237:2276585:1:.pdf:hidden=False
2751237:2276585:1:.pdf:flags=[]
2751237:2276585:1:.pdf:etag="2276585.pdf1"
recast_atlas_2019_08_pyhf:2751237:2276586:::doctype=Plot
2751237:2276586:::status=
2751237:2276586:::basedir=/opt/cdsweb/var/data/files/g227/2276586
2751237:2276586:::creation date=2021-02-06 04:33:21
2751237:2276586:::modification date=2021-02-14 04:45:52
2751237:2276586:::text extraction date=None
2751237:2276586:::total file attached=1
2751237:2276586:::total size latest version=40234
2751237:2276586:::total size all files=40234
2751237:2276586:1:.png:fullpath=/opt/cdsweb/var/data/files/g227/2276586/content.png;1
2751237:2276586:1:.png:name=recast_atlas_2019_08_pyhf
2751237:2276586:1:.png:subformat=
2751237:2276586:1:.png:status=
2751237:2276586:1:.png:checksum=6e40ebc1649be639050f0fdd7f67aa45
2751237:2276586:1:.png:size=40234
"""

parsed_metadata = [
    {
        "metadata": False,
        "origin": {
            "fullpath": "/opt/cdsweb/var/data/files/g227/2276585/content.pdf;1",
            "path": "",
            "filename": "content.pdf;1",
            "url": [
                "http://cds.cern.ch/record/2751237/files/2101.02245.pdf?version=1",
                "http://cds.cern.ch/record/2751237/files/2101.02245.pdf",
            ],
            "checksum": "md5:be99bc4762f1add866d8c08abb2e0657",
        },
        "checksum": "md5:be99bc4762f1add866d8c08abb2e0657",
        "bagpath": "data/content/content.pdf;1",
        "size": "3865962",
        "downloaded": False,
    },
    {
        "metadata": False,
        "origin": {
            "fullpath": "/opt/cdsweb/var/data/files/g227/2276586/content.png;1",
            "path": "",
            "filename": "content.png;1",
            "checksum": "md5:6e40ebc1649be639050f0fdd7f67aa45",
        },
        "checksum": "md5:6e40ebc1649be639050f0fdd7f67aa45",
        "bagpath": "data/content/content.png;1",
        "downloaded": False,
    },
]


resid = "2751237"


def test_parse():
    assert bibdocfile.parse(test_output, resid) == parsed_metadata

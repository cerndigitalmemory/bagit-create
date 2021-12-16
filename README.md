# bagit-create

[![PyPI version](https://badge.fury.io/py/bagit-create.svg)](https://pypi.org/project/bagit-create/)

Python module and CLI tool to prepare SIPs (according to the [CERN SIP specification](https://gitlab.cern.ch/digitalmemory/sip-spec)), harvesting metadata and files from various souces, such as digital repositories powered by Invenio software, Invenio, CERN Open Data,..

## Install

If you just need to run BagIt Create from the command line:

```bash
# Install from PyPi
pip install bagit-create

# Check installed version
bic --version
```

For development, you can clone this repository and then install it with the `-e` flag:

```bash
# Clone the repository
git clone https://gitlab.cern.ch/digitalmemory/bagit-create
cd bagit-create

# Create a virtual environment and activate it
python -m venv env
source env/bin/activate

# Install the project in editable mode
pip install -e .

# Check installed version
bic --version

# Run tests
python -m pytest
```

## Usage

### Options

```sh
  --version                       Show the version and exit.
  --recid TEXT                    Record ID of the resource the upstream
                                  digital repository. Required by every
                                  pipeline but local.                      
                                                                                   
  -s, --source [cds|ilcdoc|cod|zenodo|inveniordm|indico|local|ilcagenda]  
                                  Select source pipeline from the supported 
                                  ones.  [required]                

  -d, --dry-run                   Skip downloads and create a `light` bag,
                                  without any payload.                        
                                                                                   
  -a, --alternate-uri             Use alternative uri instead of https for
                                  fetch.txt (e.g. root endpoints  for CERN   
                                  Open Data instead of http).               
                                                                                   
  -v, --verbose                   Enable basic logging (verbose, 'info'
                                  level).

  -vv, --very-verbose             Enable verbose logging (very verbose,
                                  'debug' level).

  -b, --bibdoc                    [ONLY for Supported Invenio v1 pipelines]
                                  Get metadata for a CDS record from the
                                  bibdocfile utility
                                  (`/opt/cdsweb/bin/bibdocfile` must be
                                  available in the system).

  --bd-ssh-host TEXT              [ONLY for Supported Invenio v1 pipelines]
                                  Specify SSH host to run bibdocfile. Access
                                  must be promptless. (See documentation for
                                  usage and configuration). By default uses
                                  the local machine.

  -t, --target TEXT               Output folder for the generated SIP. By
                                  default uses the same folder  the tool is
                                  being executed from.

  -sp, --source-path TEXT         [Local source ONLY, required] Set path of
                                  the local folder to use as a source.

  -u, --author TEXT               [Local source ONLY] Specify the Author of
                                  data.

  -sbp, --source-base-path TEXT   [Local source ONLY] Specify a part of the
                                  path as  relevant for extracting an
                                  hierachy.

  -ic, --invcookie TEXT           [Invenio v1.x ONLY] Use custom
                                  INVENIOSESSION cookie value to authenticate.
                                  Useful for local accounts.

  -ss, --skipssl                  [Invenio v1.x ONLY] Skip SSL authentication
                                  in HTTP requests. Useful for misconfigured
                                  or deprecated instances.

  -c, --cert TEXT                 [Invenio v1.x ONLY] Full path to the
                                  certificate to use to authenticate. Don't
                                  specify extension, only the file name. A
                                  '.key' and a '.pem' will be loaded. Read
                                  documentation (CERN SSO authentication) to
                                  learn more on how to generate it.

  --help                          Show this message and exit.
```

### Supported sources

Four pipelines (Invenio 1.x, Invenio 3.x, CERN Open Data, Indico) are currently implemented, supporting the following digital repositories:

| Name                  | Source ID    | URL                                | Pipeline                    |
|---------------------- |--------------|------------------------------------|-----------------------------|
| CERN Document Server  | cds          | https://cds.cern.ch/               | Invenio v1.x                |
| ILC Document Server   | ilcdoc       | http://ilcdoc.linearcollider.org   | Invenio v1.x                |
| CERN Open Data        | cod          | https://opendata.cern.ch/          | CERN Open Data              |
| Zenodo                | zenodo       | https://zenodo.org/                | Invenio v3.x                |
| CERN Indico           | indico       | https://indico.cern.ch/            | Indico v3.0.x\*             |
| ILC Agenda            | ilcagenda    | https://agenda.linearcollider.org/ | Indico v3.0.x\*             |


\* requires additional configuration

### Indico

To use any Indico pipeline you need an API Token. From your browser, login to the Indico instance, go to "Preferences" and then "API Token". Create new token, name can be anything. Select (at least) `Everything (all methods)` and `Classic API (read only)` as scopes. Note down the token and set the `INDICO_KEY` environement variable to it.

```bash
export INDICO_KEY=<INDICO_API_TOKEN>
```

This will also allow you to run the tool for **restricted** events you have access to.

### CERN SSO Authentication for Invenio v1.x

BIC can run in a "authenticated" mode for Invenio v1.x pipelines (e.g. CDS) by getting CERN SSO HTTP cookies through the [cern-sso-python](https://gitlab.cern.ch/digitalmemory/cern-sso-python) tool.

For this, you'll need to provide a Grid User certificate obtained from the [CERN Certification Authority](https://ca.cern.ch/ca/) of an account that has access to the desired restricted record. 

Once you downloaded your `.p12` certificate, you'll need to process the certificate files to remove passwords and separate the key and certificate:

```bash
openssl pkcs12 -clcerts -nokeys -in myCert.p12 -out myCert.pem
# A passphrase is required here (after the Import one)
openssl pkcs12 -nocerts -in myCert.p12 -out myCert.tmp.key
openssl rsa -in ~/private/myCert.tmp.key -out myCert.key
```

>  WARNING: openssl rsa.. command removes the passphrase from the private key. Keep it in a secure location.

Once you have your `myCert.key` and `myCert.pem` files, you can run BagIt-Create with the `--cert` option, providing the path to those files (without extension, as it is assumed that your certificate and key files have the same base name and are located in the same folder, and that the key has the file ending `.key` and the certificate `.pem`). E.g.:

```bash
bic --source cds --recid 2748063 --cert /home/avivace/Downloads/myCert
```

Will make the tool look for "/home/avivace/Downloads/**myCert.key**" and "/home/avivace/Downloads/**myCert.pem**" and the pipeline will run authenticating every HTTP request with the obtained Cookies, producing a SIP of the desired restricted record.

For more information, check the [cern-sso-python](https://gitlab.cern.ch/digitalmemory/cern-sso-python) docs.

### Local authentication for Invenio v1.x

To authenticate with a local account (i.e. without CERN SSO), login on your Invenio v1.x instance with a browser and what your `INVENIOSESSION` cookie is set to.

On Firefox, open the Developers tools, go in the "Storage" tab and select "Cookies", you should see an `INVENIOSESSION` cookie. Copy its value and pass it to BagIt Create with the `--invcookie` option:

```bash
bic --source cds --recid 2748063 --invcookie COOKIECONTENTHERE
```

### CLI

Some examples:

CDS:

```bash
# (Expect error) Removed resource
bic --recid 1 --source cds

# (Expect error) Public resource but metadata requires authorisation
bic --recid 1000 --source cds

# Resource with a lot of large videos, light bag
bic --recid 1000571 --source cds --dry-run

# Resource with just a PDF
bic --recid 2728246 --source cds
```

ilcdoc:

```bash
# ilcdoc #
bic --source ilcdoc --recid 62959 --verbose
bic --source ilcdoc --recid 34794 --verbose
```

Zenodo

```bash
bic --recid 3911261 --source zenodo --verbose
bic --recid 3974864 --source zenodo --verbose
```

Indico

```bash
bic --recid 1024767 --source indico 
```


CERN Open Data

```bash
bic --recid 1 --source cod --dry-run --verbose
bic --recid 8884 --source cod --dry-run --verbose --alternate-uri
bic --recid 8884 --source cod --dry-run --verbose
bic --recid 5200 --source cod --dry-run --verbose
bic --recid 8888 --source cod --dry-run --verbose

bic --recid 10101 --source cod --dry-run --verbose
bic --recid 10102 --source cod --dry-run --verbose
bic --recid 10103 --source cod --dry-run --verbose
bic --recid 10104 --source cod --dry-run --verbose
bic --recid 10105 --source cod --dry-run --verbose

bic --recid 10101 --source cod --verbose
bic --recid 10102 --source cod --verbose
bic --recid 10103 --source cod --verbose
bic --recid 10104 --source cod --verbose
bic --recid 10105 --source cod --verbose
```

### Module

BIC can easily be run inside other Python scripts. Just import it and use the `process` method with the same parameters you can pass to the CLI.

E.g., this snippet creates SIP packages for CDS resources from ID 2728246 to 27282700.

```python
import bagit_create

for i in range(2728246, 27282700):
    result = bagit_create.main.process(
        source="cds", recid=i, loglevel=3
    )
    if result["status"] == 0:
        print("Success")
    else:
        print("Error")
```

### Accessing CERN firewalled websites

If the upstream source you're trying to access is firewalled, you can set up a SOCKS5 proxy via a SSH tunnel through LXPLUS and then run `bic` through it with tools like `proxychains` or `tsocks`. E.g.:


Bring up the SSH tunnel:
```bash
ssh -D 1337 -q -N -f -C lxplus.cern.ch
```

The proxy will be up at `socks5://localhost:1337`. After having installed `tsocks`, edit the its configuration file (`/etc/tsocks.conf`) as follows:

```bash
[...]
server = localhost
server_type = 5
server_port = 1337
[...]
```

Now, just run `bic` as documented here but prepend `tsocks` to the command:

```bash
tsocks bic --recid 1024767 --source indico -vv
```

### Module

The BagIt-Create tool can be used from other python scripts easily:

```python
from bagit_create.main import process

process(recid=2272168, source="cds")
```

### bibdocfile

The `bibdocfile` command line utility can be used to get metadata for CDS, exposing internal file paths and hashes normally not available through the CDS API.

If the executable is available in the path (i.e. you can run `/opt/cdsweb/bin/bibdocfile`) just append `--bibdoc`:

```bash
bic --recid 2751237 --source cds --bibdoc -v
```

If this is not the case, you can pass a `--bd-ssh-host` parameter specifying the name of an SSH configured connection pointing to a machine able to run the command for you. Be aware that your machine must be able to establish such connection without any user interaction (the script will run `ssh <THE_PROVIDED_SSH_HOST> bibdocfile ..args`).

Since in a normal CERN scenario this can't be possible due to required ProxyJumps/OTP authentication steps, you can use the `ControlMaster` feature of any recent version of OpenSSH, allowing to reuse sockets for connecting:

Add an entry in `~/.ssh/config` to set up the SSH connection to the remote machine able to run `bibdocfile` for you in the following way:

```bash
Host <SSH_NAME>
  User <YOUR_USER>
  Hostname <HOSTNAME.cern.ch>
  ProxyJump <LXPLUS_or_AIADM>
  ControlMaster auto
  ControlPath ~/.ssh/control:%h:%p:%r
```

Then, run `ssh <SSH_NAME>` in a shell, authenticate and keep it open. OpenSSH will now reuse this socket everytime you run `<SSH_NAME>`, allowing BagItCreate tool to run `bibdocfile` over this ssh connection for you, if you pass the `bd-ssh-host` parameter:

```bash
bic --recid 2751237 --source cds --bibdoc --bd-ssh-host=<SSH_NAME> -v
``` 

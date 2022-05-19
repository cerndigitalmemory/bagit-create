#!/usr/bin/env python3

from typing import Text

import click

from .main import process
from .version import __version__

"""bagit-create command line tool."""


@click.command()
@click.version_option(__version__)
@click.option(
    "--recid",
    help="Record ID of the resource the upstream digital repository. Required by every pipeline but local.",
)
@click.option(
    "-u",
    "--url",
    help="Provide an URL for the Record. Must be from the supported sources.",
    type=Text,
)
@click.option(
    "-s",
    "--source",
    help="Select source pipeline from the supported ones.",
    type=click.Choice(
        [
            "cds",
            "ilcdoc",
            "cod",
            "zenodo",
            "inveniordm",
            "indico",
            "local",
            "ilcagenda",
            "codimd",
        ],
        case_sensitive=False,
    ),
)
@click.option(
    "-d",
    "--dry-run",
    help="Skip downloads and create a `light` bag, without any payload.",
    default=False,
    is_flag=True,
)
@click.option(
    "-a",
    "--alternate-uri",
    help="""
         Use alternative uri instead of https for fetch.txt (e.g. root endpoints
         for CERN Open Data instead of http).""",
    default=False,
    is_flag=True,
)
@click.option(
    "--verbose",
    "-v",
    help="Enable basic logging (verbose, 'info' level).",
    default=False,
    is_flag=True,
)
@click.option(
    "--very-verbose",
    "-vv",
    help="Enable verbose logging (very verbose, 'debug' level).",
    default=False,
    is_flag=True,
)
@click.option(
    "--bibdoc",
    "-b",
    help="""
    [ONLY for Supported Invenio v1 pipelines]
    Get metadata for a CDS record from the bibdocfile utility
    (`/opt/cdsweb/bin/bibdocfile` must be available in the system).""",
    default=False,
    is_flag=True,
)
@click.option(
    "--bd-ssh-host",
    help="""
    [ONLY for Supported Invenio v1 pipelines]
    Specify SSH host to run bibdocfile. Access must be promptless.
    (See documentation for usage and configuration).
    By default uses the local machine.""",
    default=None,
    is_flag=False,
)
@click.option(
    "--target",
    "-t",
    help="""
    Output folder for the generated SIP. By default uses the same folder
    the tool is being executed from.""",
    type=Text,
    default=None,
    is_flag=False,
)
@click.option(
    "--source-path",
    "-sp",
    help="""[Local source ONLY, required]
    Set path of the local folder to use as a source.""",
    type=Text,
    default=None,
    is_flag=False,
)
@click.option(
    "--author",
    "-u",
    help="[Local source ONLY] Specify the Author of data.",
    type=Text,
    default=None,
    is_flag=False,
)
@click.option(
    "--source-base-path",
    "-sbp",
    help="""
    [Local source ONLY] Specify a part of the path as
    relevant for extracting an hierachy.""",
    type=Text,
    default=None,
    is_flag=False,
)
@click.option(
    "--cert",
    "-c",
    help="""
    [Invenio v1.x ONLY] Full path to the certificate to use to authenticate.
    Don't specify extension, only the file name. A '.key' and a '.pem' will be loaded.
    Read documentation (CERN SSO authentication) to learn more on how to generate it.
    """,
    type=Text,
    default=None,
    is_flag=False,
)
@click.option(
    "--skipssl",
    "-ss",
    help="""
    [Invenio v1.x ONLY] Skip SSL authentication in HTTP requests.
    Useful for misconfigured or deprecated instances.
    """,
    type=Text,
    default=False,
    is_flag=True,
)
@click.option(
    "--token",
    "-tk",
    help="""
    Additional authentication token, depending on the chosen source pipeline.
    See the documentation for more information on how to obtain those strings.
    [Invenio v1.x] Value of the `INVENIOSESSION` session cookie
    [Indico] API token
    [CodiMD] Value of the `connect.sid` cookie
    """,
    type=Text,
    default=False,
    is_flag=False,
)
def cli(
    recid,
    source,
    target,
    source_path,
    author,
    source_base_path,
    dry_run,
    alternate_uri,
    verbose,
    very_verbose,
    bibdoc,
    bd_ssh_host,
    cert,
    token,
    skipssl,
    url,
):

    # Select the desired log level (default is 2, warning)
    if very_verbose:
        loglevel = 0
    elif verbose:
        loglevel = 1
    else:
        loglevel = 2

    # This "wrapper" method allows the main one to be called
    #  from python, ignoring the click CLI interface
    result = process(
        recid,
        source,
        loglevel,
        target,
        source_path,
        author,
        source_base_path,
        dry_run,
        alternate_uri,
        bibdoc,
        bd_ssh_host,
        cert,
        token,
        skipssl,
        url,
    )
    print(f"Job result: {result}")


if __name__ == "__main__":
    cli()

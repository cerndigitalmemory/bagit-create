#!/usr/bin/env python3

from typing import Text
from .main import process
from .version import __version__
import click

"""bagit-create command line tool."""


@click.command()
@click.version_option(__version__)
@click.option(
    "--recid",
    help="Record ID of the resource the upstream digital repository. Required by every pipeline but local.",
)
@click.option(
    "-s",
    "--source",
    help="Select source pipeline from the supported ones.",
    required=True,
    type=click.Choice(
        ["cds", "ilcdoc", "cod", "zenodo", "inveniordm", "indico", "local", "ilcagenda"],
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
    )
    print(f"Job result: {result}")


if __name__ == "__main__":
    cli()

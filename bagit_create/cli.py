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
    help="Unique ID of the record in the upstream source",
    required=True,
)
@click.option(
    "-s",
    "--source",
    help="Select source pipeline",
    required=True,
    type=click.Choice(
        ["cds", "ilcdoc", "cod", "zenodo", "inveniordm", "indico", "local"], case_sensitive=False
    ),
)
@click.option(
    "-d",
    "--dry-run",
    help="Skip downloads",
    default=False,
    is_flag=True,
)
@click.option(
    "-a",
    "--alternate-uri",
    help="""
         Use alternative uri instead of https for fetch.txt (e.g. root endpoints 
         for CERN Open Data instead of http)""",
    default=False,
    is_flag=True,
)
@click.option(
    "--verbose",
    "-v",
    help="Enable logging (verbose, 'info' level)",
    default=False,
    is_flag=True,
)
@click.option(
    "--very-verbose",
    "-vv",
    help="Enable logging (very verbose, 'debug' level)",
    default=False,
    is_flag=True,
)
@click.option(
    "--bibdoc",
    "-b",
    help="""
    Get metadata for a CDS record from the bibdocfile utility.
    (`/opt/cdsweb/bin/bibdocfile` must be available in the system
    and the resource must be from CDS)""",
    default=False,
    is_flag=True,
)
@click.option(
    "--bd-ssh-host",
    help="""
    SSH host to run bibdocfile""",
    default=None,
    is_flag=False,
)
@click.option(
    "--target",
    "-t",
    help="Select destination folder",
    type=Text,
    default=None,
    is_flag=False,
)
@click.option(
    "--localsource",
    "-ls",
    help="Select the local source folder.",
    type=Text,
    default=None,
    is_flag=False,
)
def cli(
    recid,
    source,
    target,
    localsource,
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
        localsource,
        dry_run,
        alternate_uri,
        bibdoc,
        bd_ssh_host,
    )
    print(f"Job result: {result}")


if __name__ == "__main__":
    cli()

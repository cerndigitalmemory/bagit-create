#!/usr/bin/env python3

from . import main
import click

"""bagit-create command line tool."""


@click.command()
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
    type=click.Choice(["cds", "ilcdoc", "cod"], case_sensitive=False),
)
@click.option(
    "-skip",
    "--skip-downloads",
    help="Creates files but skip downloading the actual payloads",
    default=False,
    is_flag=True,
)
@click.option(
    "-aj",
    "--ark-json",
    help="Generate a JSON metadata file for arkivum ingestions",
    default=False,
    is_flag=True,
)
@click.option(
    "--ark-json-rel",
    help="Generate a JSON metadata file for arkivum ingestions using relative paths",
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
def cli(
    recid,
    source,
    skip_downloads,
    ark_json,
    ark_json_rel,
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
    result = main.process(
        recid,
        source,
        loglevel,
        ark_json,
        ark_json_rel,
        skip_downloads,
        bibdoc,
        bd_ssh_host,
    )
    print(f"Result object: {result}")


if __name__ == "__main__":
    cli()

from bagit_create.main import process
import click

"""bagit-create command line tool."""

@click.command()
@click.option(
    "--recid",
    help="Unique ID of the record in the upstream source",
    required=True,
)
@click.option(
    "-s", "--source",
    help="Select source pipeline",
    required=True,
    type=click.Choice(["cds", "ilcdoc", "cod"], case_sensitive=False)
)
@click.option(
    "-skip", "--skip_downloads",
    help="Creates files but skip downloading the actual payloads",
    default=False,
    is_flag=True,
)
@click.option(
    "-aj", "--ark_json",
    help="Generate a JSON metadata file for arkivum ingestions",
    default=False,
    is_flag=True,
)
@click.option(
    "--ark_json_rel",
    help="Generate a JSON metadata file for arkivum ingestions using relative paths",
    default=False,
    is_flag=True,
)
@click.option(
    "--verbose", "-v",
    help="Enable logging (verbose, 'info' level)",
    default=False,
    is_flag=True,
)
@click.option(
    "--very-verbose", "-vv",
    help="Enable logging (very verbose, 'debug' level)",
    default=False,
    is_flag=True,
)
def cli(recid, source, skip_downloads, ark_json, ark_json_rel, v, vv):
    
    # Select the desired log level (default is 2, warning)
    if vv:
        loglevel = 0
    elif v:
        loglevel = 1
    else:
        loglevel = 2

    # This "wrapper" method allows the main one to be called
    #  from python, ignoring the click CLI interface
    result = process(recid, source, loglevel, ark_json, ark_json_rel, skip_downloads)
    print(f"Result object: {result}")

if __name__ == "__main__":
    cli()

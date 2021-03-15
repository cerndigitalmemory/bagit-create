import main
import click

@click.command()
@click.option(
    "--recid",
    default="1",
    help="Unique ID of the record in the upstream source",
    required=True,
)
@click.option(
    "--source",
    help="Select source pipeline",
    required=True,
    type=click.Choice(["cds", "ilcdoc", "cod"], case_sensitive=False)
)
@click.option(
    "--skip_downloads",
    help="Creates files but skip downloading the actual payloads",
    default=False,
    is_flag=True,
)
def cli(recid, source, skip_downloads):
    # This "wrapper" method allows the main one to be called
    #  from python, ignoring the click CLI interface
    main.process(recid, source, skip_downloads)

if __name__ == "__main__":
    cli()

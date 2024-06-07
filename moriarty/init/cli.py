import click


@click.command()
def download():
    pass


@click.group()
def cli():
    pass


cli.add_command(download)

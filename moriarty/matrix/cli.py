import click


@click.command()
def list_plugins():
    pass


@click.group()
def cli():
    pass


cli.add_command(list_plugins)

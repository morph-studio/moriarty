import click


@click.command()
def start():
    pass


@click.command()
def init():
    pass


@click.group()
def cli():
    pass


cli.add_command(init)
cli.add_command(start)

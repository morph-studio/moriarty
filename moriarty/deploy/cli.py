import click


@click.command()
def deploy():
    pass


@click.group()
def cli():
    pass


cli.add_command(deploy)

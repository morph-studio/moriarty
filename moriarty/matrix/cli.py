from pprint import pformat

import click

from .job_manager.bridge.manager import BridgeManager
from .operator_.spawner.manager import SpawnerManager


@click.command()
def list_plugins():
    click.echo(f"Registered Bridges: \n{pformat(BridgeManager().registed_cls)}")
    click.echo(f"Registered Spawners: \n{pformat(SpawnerManager().registed_cls)}")


@click.group()
def cli():
    pass


cli.add_command(list_plugins)

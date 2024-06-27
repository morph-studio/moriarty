from moriarty.matrix.operator.spawner.plugin import Spawner, hookimpl


class LocalSpawner(Spawner):
    register_name = "local"


@hookimpl
def register(manager):
    manager.register(LocalSpawner)

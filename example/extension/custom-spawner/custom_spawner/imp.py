from moriarty.matrix.operator_.spawner.plugin import Spawner, hookimpl


class CustomSpawner(Spawner):
    register_name = "example"


@hookimpl
def register(manager):
    manager.register(CustomSpawner)

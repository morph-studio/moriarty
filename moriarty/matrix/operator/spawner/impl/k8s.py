from moriarty.matrix.operator.spawner.plugin import Spawner, hookimpl


class KubeSpawner(Spawner):
    register_name = "kube"


@hookimpl
def register(manager):
    manager.register(KubeSpawner)

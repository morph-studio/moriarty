from moriarty.matrix.operator_.spawner.plugin import Spawner, hookimpl
from moriarty.matrix.operator_.tools import load_kube_config


class KubeSpawner(Spawner):
    register_name = "kube"

    async def prepare(self) -> None:
        await load_kube_config()


@hookimpl
def register(manager):
    manager.register(KubeSpawner)

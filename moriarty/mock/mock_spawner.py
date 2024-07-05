from moriarty.log import logger
from moriarty.matrix.operator_.spawner.plugin import Spawner, hookimpl


class MockSpawner(Spawner):
    register_name = "mock"

    async def prepare(self) -> None:
        pass

    async def count_avaliable_instances(self, endpoint_name: str) -> int:
        return 1


@hookimpl
def register(manager):
    manager.register(MockSpawner)

from moriarty.log import logger
from moriarty.matrix.operator_.orm import EndpointORM
from moriarty.matrix.operator_.params import EndpointRuntimeInfo
from moriarty.matrix.operator_.spawner.plugin import Spawner, hookimpl


class MockSpawner(Spawner):
    register_name = "mock"

    async def prepare(self) -> None:
        pass

    async def count_avaliable_instances(self, endpoint_name: str) -> int:
        return 1

    async def create(self, endpoint_orm: EndpointORM) -> None:
        return None

    async def update(self, endpoint_orm: EndpointORM, need_restart: bool = False) -> None:
        return None

    async def delete(self, endpoint_name: str) -> None:
        return None

    async def get_runtime_info(self, endpoint_name: str) -> EndpointRuntimeInfo:
        return EndpointRuntimeInfo(
            total_replicas_nums=1,
            updated_replicas_nums=1,
            avaliable_replicas_nums=1,
            unavailable_replicas_nums=0,
        )

    async def scale(self, endpoint_name: str, target_replicas: int) -> None:
        return None


@hookimpl
def register(manager):
    manager.register(MockSpawner)

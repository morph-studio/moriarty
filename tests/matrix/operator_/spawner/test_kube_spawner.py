from __future__ import annotations

import typing

import kubernetes_asyncio as kubernetes
import pytest
from sqlalchemy import delete

from moriarty.matrix.operator_.orm import EndpointORM
from moriarty.matrix.operator_.spawner.manager import get_spawner, get_spawner_manager

if typing.TYPE_CHECKING:
    from moriarty.matrix.operator_.spawner.impl.k8s import KubeSpawner


@pytest.fixture
async def kube_spawner():
    try:
        return await get_spawner("kube", get_spawner_manager())
    except kubernetes.config.config_exception.ConfigException:
        pytest.skip("Kubernetes is not available, use minikube or kind for tests")


@pytest.fixture
def mock_endpoint_orm(session):
    endpoint_name = "mock_endpoint"
    orm = EndpointORM(
        endpoint_name=endpoint_name,
        model_path="host://tmp/",
    )
    session.add(orm)
    session.commit()
    session.refresh(orm)
    yield orm
    session.execute(delete(EndpointORM).where(EndpointORM.endpoint_name == endpoint_name))
    session.commit()


async def test_kube_spawner(kube_spawner: KubeSpawner, mock_endpoint_orm):
    assert await kube_spawner.count_avaliable_instances(mock_endpoint_orm.endpoint_name) == 0

    await kube_spawner.create(mock_endpoint_orm)

    mock_endpoint_orm.cpu_request = 1
    await kube_spawner.update(mock_endpoint_orm)
    await kube_spawner.count_avaliable_instances(mock_endpoint_orm.endpoint_name)
    # runtime_info = await kube_spawner.get_runtime_info(mock_endpoint_orm.endpoint_name)
    # assert runtime_info.total_replicas_nums == 1

    await kube_spawner.scale(mock_endpoint_orm.endpoint_name, 2)
    # runtime_info = await kube_spawner.get_runtime_info(mock_endpoint_orm.endpoint_name)
    # assert runtime_info.total_replicas_nums == 2

    await kube_spawner.delete(mock_endpoint_orm.endpoint_name)


if __name__ == "__main__":
    pytest.main(["-vv", "-s", __file__])

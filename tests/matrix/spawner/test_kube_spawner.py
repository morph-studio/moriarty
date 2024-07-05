from __future__ import annotations

import typing

import kubernetes_asyncio as kubernetes
import pytest

from moriarty.matrix.operator_.spawner.manager import get_spawner, get_spawner_manager

if typing.TYPE_CHECKING:
    from moriarty.matrix.operator_.spawner.impl.k8s import KubeSpawner


@pytest.fixture
async def kube_spawner():
    try:
        return await get_spawner("kube", get_spawner_manager())
    except kubernetes.config.config_exception.ConfigException:
        pytest.skip("Kubernetes is not available")


async def test_kube_spawner(kube_spawner: KubeSpawner):
    assert kube_spawner

from __future__ import annotations

import os
from functools import cached_property

from fastapi import Depends

from moriarty.matrix.operator.orm import AutoscalerORM, EndpointORM
from moriarty.matrix.operator.spawner import plugin
from moriarty.matrix.operator.spawner.manager import SpawnerManager, get_spawner_manager
from moriarty.sidecar.params import MatrixCallback


def get_spawner_name() -> str:
    return os.getenv("MORIARTY_SPAWNER_NAME", "kube")


def get_operaotr(
    spawner_name: str = Depends(get_spawner_name),
    spawner_manager=Depends(get_spawner_manager),
) -> Operator:
    return Operator(
        spawner_name=spawner_name,
        spawner_manager=spawner_manager,
    )


class Operator:
    def __init__(self, spawner_name: str, spawner_manager: SpawnerManager):
        self.spawner_name = spawner_name
        self.spawner_manager = spawner_manager

    @cached_property
    def spawner(self) -> plugin.Spawner:
        return self.spawner_manager.init(self.spawner_name)

    async def handle_callback(self, callback: MatrixCallback, token: str = None) -> None: ...

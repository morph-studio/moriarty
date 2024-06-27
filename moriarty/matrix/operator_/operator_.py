from __future__ import annotations

import os
from functools import cached_property

from fastapi import Depends

from moriarty.matrix.operator_.autoscaler import (
    AutoscalerManager,
    get_autoscaler_manager,
)
from moriarty.matrix.operator_.orm import AutoscalerORM, EndpointORM
from moriarty.matrix.operator_.spawner import plugin
from moriarty.matrix.operator_.spawner.manager import (
    SpawnerManager,
    get_spawner_manager,
)
from moriarty.sidecar.params import MatrixCallback


def get_spawner_name() -> str:
    return os.getenv("MORIARTY_SPAWNER_NAME", "kube")


async def get_operaotr(
    spawner_name: str = Depends(get_spawner_name),
    spawner_manager: SpawnerManager = Depends(get_spawner_manager),
    autoscaler_manager: AutoscalerManager = Depends(get_autoscaler_manager),
) -> Operator:
    spawner = spawner_manager.init(spawner_name)
    await spawner.prepare()

    return Operator(spawner=spawner, autoscaler_manager=autoscaler_manager)


class Operator:
    def __init__(
        self,
        spawner: plugin.Spawner,
        autoscaler_manager: AutoscalerManager,
    ) -> None:
        self.spawner = spawner
        self.autoscaler_manager = autoscaler_manager

    async def handle_callback(self, callback: MatrixCallback, token: str = None) -> None: ...

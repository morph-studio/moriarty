from __future__ import annotations

import pluggy

from moriarty.matrix.operator_.orm import EndpointORM
from moriarty.tools import FlexibleModel

project_name = "moriarty.matrix.spawner"
"""
The entry-point name of this extension.

Should be used in ``pyproject.toml`` as ``[project.entry-points."{project_name}"]``
"""
hookimpl = pluggy.HookimplMarker(project_name)
"""
Hookimpl marker for this extension, extension module should use this marker

Example:

    .. code-block:: python

        @hookimpl
        def register(manager):
            ...
"""

hookspec = pluggy.HookspecMarker(project_name)


@hookspec
def register(manager):
    """
    For more information about this function, please check the :ref:`manager`

    We provided an example package for you in ``{project_root}/example/extension/custom-spawner``.

    Example:

    .. code-block:: python

        class CustomSpawner(Spawner):
            register_name = "example"

        from moriarty.matrix.operator_.spawner.plugin import hookimpl

        @hookimpl
        def register(manager):
            manager.register(CustomSpawner)


    Config ``project.entry-points`` so that we can find it

    .. code-block:: toml

        [project.entry-points."moriarty.matrix.spawner"]
        {whatever-name} = "{package}.{path}.{to}.{file-with-hookimpl-function}"


    You can verify it by `moriarty-matrix list-plugins`.
    """


class EndpointRuntimeInfo(FlexibleModel):
    total_node_nums: int
    pending_node_nums: int
    avaliable_node_nums: int
    error_node_nums: int


class Spawner:
    register_name: str

    async def prepare(self) -> None:
        pass

    async def count_avaliable_instances(self, endpoint_name: str) -> int:
        raise NotImplementedError

    async def create(self, endpoint_orm: EndpointORM) -> None:
        raise NotImplementedError

    async def update(self, endpoint_orm: EndpointORM) -> None:
        raise NotImplementedError

    async def delete(self, endpoint_name: str) -> None:
        raise NotImplementedError

    async def get_runtime_info(self, endpoint_name: str) -> EndpointRuntimeInfo:
        raise NotImplementedError

    async def scale(self, endpoint_name: str, target_replicas: int) -> None:
        raise NotImplementedError

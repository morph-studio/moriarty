from __future__ import annotations

import os
from typing import Awaitable

import pluggy

from moriarty.envs import MORIARTY_BRIDGE_OUTPUT_BUCKET_ENV
from moriarty.matrix.job_manager.params import InferenceJob, InferenceResult

project_name = "moriarty.matrix.bridge"
"""
The entry-point name of this extension.

Should be used in ``pyproject.toml`` as ``[project.entry-points."moriarty.matrix.bridge"]``
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

    We provided an example package for you in ``{project_root}/example/extension/custom-bridge``.

    Example:

    .. code-block:: python

        class CustomBridge(QueueBridge):
            register_name = "example"

        from moriarty.matrix.job_manager.bridge.plugin import hookimpl

        @hookimpl
        def register(manager):
            manager.register(CustomBridge)


    Config ``project.entry-points`` so that we can find it

    .. code-block:: toml

        [project.entry-points."moriarty.matrix.bridge"]
        {whatever-name} = "{package}.{path}.{to}.{file-with-hookimpl-function}"


    You can verify it by `moriarty-matrix list-plugins`.
    """


class QueueBridge:
    """
    A bridge is used to adapt different queue system to job manager

    Implementation of subclass should follow:
        - When dequeue a job or result, `process_func` will be called.
          And if exception occurs, the job may not be removed from queue.
          So that we can retry later.
    """

    register_name: str
    output_bucket = os.getenv(MORIARTY_BRIDGE_OUTPUT_BUCKET_ENV)

    def __init__(
        self,
        bridge_result_queue_url: str = None,
        *args,
        **kwargs,
    ):
        self.bridge_result_queue_url = bridge_result_queue_url

    def make_job_queue_url(self, endpoint_name: str, priority: int = None) -> str:
        raise NotImplementedError

    def remove_job_queue(self, endpoint_name: str, priority: int = None) -> None:
        raise NotImplementedError

    async def list_avaliable_priorities(self, endpoint_name: str) -> list[int]:
        raise NotImplementedError

    async def enqueue_job(self, endpoint_name: str, job: InferenceJob, priority: int = None) -> str:
        raise NotImplementedError

    async def dequeue_job(
        self,
        endpoint_name: str,
        process_func: Awaitable[InferenceJob],
        size: int = 1,
        priority: int = None,
    ) -> int:
        raise NotImplementedError

    async def enqueue_result(self, result: InferenceResult) -> str:
        raise NotImplementedError

    async def dequeue_result(
        self, process_func: Awaitable[InferenceResult], size: int = 10
    ) -> list[InferenceResult]:
        raise NotImplementedError

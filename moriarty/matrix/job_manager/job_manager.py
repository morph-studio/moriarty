from typing import Any

from fastapi import Depends

from moriarty.matrix.job_manager.bridge.manager import BridgeManager
from moriarty.matrix.job_manager.bridge.plugin import QueueBridge
from moriarty.matrix.job_manager.params import InferenceJob, InferenceResult
from moriarty.tools import ensure_awaitable


def get_bridge_manager():
    return BridgeManager()


def get_job_manager(
    bridge_manager: BridgeManager = Depends(get_bridge_manager),
):
    return JobManager(bridge_manager)


class JobManager:
    def __init__(
        self,
        bridge_manager: BridgeManager,
        bridge_kwargs: dict[str, Any] = None,
    ):
        self.bridge_manager = bridge_manager
        self.bridge_kwargs = bridge_kwargs or {}

    def get_bridge(self, bridge_name: str, **kwargs) -> QueueBridge:
        return self.bridge_manager.init(bridge_name, **{**self.bridge_kwargs, **kwargs})

    async def enqueue_job(
        self,
        bridge: str | QueueBridge,
        job: InferenceJob,
        *bridge_args,
        **bridge_kwargs,
    ) -> str:
        if isinstance(bridge, str):
            bridge = self.get_bridge(bridge, *bridge_args, **bridge_kwargs)
        await bridge.enqueue_job(
            job,
        )
        return job.job_id

    async def pool_job(
        self,
        bridge: str | QueueBridge,
        process_func: callable,
        *bridge_args,
        **bridge_kwargs,
    ) -> InferenceJob:
        if isinstance(bridge, str):
            bridge = self.get_bridge(bridge, *bridge_args, **bridge_kwargs)
        process_func = ensure_awaitable(process_func)

        return await bridge.dequeue_job(
            process_func,
        )

    async def enqueue_result(
        self,
        bridge: str | QueueBridge,
        result: InferenceResult,
        *bridge_args,
        **bridge_kwargs,
    ) -> str:
        if isinstance(bridge, str):
            bridge = self.get_bridge(bridge, *bridge_args, **bridge_kwargs)
        await bridge.enqueue_result(
            result,
        )
        return result.job_id

    async def pool_result(
        self,
        bridge: str | QueueBridge,
        process_func: callable,
        *bridge_args,
        **bridge_kwargs,
    ) -> InferenceResult:
        if isinstance(bridge, str):
            bridge = self.get_bridge(bridge, *bridge_args, **bridge_kwargs)
        process_func = ensure_awaitable(process_func)
        return await bridge.dequeue_result(
            process_func,
        )

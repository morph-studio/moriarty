from typing import Any, Awaitable, Callable

from fastapi import Depends

from moriarty.log import logger
from moriarty.matrix.job_manager.bridge.manager import BridgeManager
from moriarty.matrix.job_manager.bridge.plugin import QueueBridge
from moriarty.matrix.job_manager.params import InferenceJob, InferenceResult
from moriarty.tools import ensure_awaitable, sample_as_weights


def get_bridge_manager():
    return BridgeManager()


def get_bridge_wrapper(
    bridge_manager: BridgeManager = Depends(get_bridge_manager),
):
    return BridgeWrapper(bridge_manager)


class BridgeWrapper:
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
        endpoint_name: str,
        job: InferenceJob,
        priority: int = None,
        *bridge_args,
        **bridge_kwargs,
    ) -> str:
        if isinstance(bridge, str):
            bridge = self.get_bridge(
                bridge,
                *bridge_args,
                **bridge_kwargs,
            )
        await bridge.enqueue_job(
            job,
            endpoint_name=endpoint_name,
            priority=priority,
        )
        return job.job_id

    async def dequeue_job(
        self,
        bridge: str | QueueBridge,
        endpoint_name: str,
        process_func: Callable[[InferenceResult], None] | Awaitable[InferenceResult],
        size: int = None,
        priority: int = None,
        *bridge_args,
        **bridge_kwargs,
    ) -> int:
        if isinstance(bridge, str):
            bridge = self.get_bridge(
                bridge,
                *bridge_args,
                **bridge_kwargs,
            )
        process_func = ensure_awaitable(process_func)

        return await bridge.dequeue_job(
            process_func,
            endpoint_name=endpoint_name,
            size=size,
            priority=priority,
        )

    async def sample_job(
        self,
        bridge: str | QueueBridge,
        endpoint_name: str,
        process_func: Callable[[InferenceResult], None] | Awaitable[InferenceResult],
        size: int = None,
        *bridge_args,
        **bridge_kwargs,
    ) -> int:
        if isinstance(bridge, str):
            bridge = self.get_bridge(
                bridge,
                *bridge_args,
                **bridge_kwargs,
            )
        process_func = ensure_awaitable(process_func)

        avaliable_priorities = bridge.list_priorities(endpoint_name)
        sampled_priority = sample_as_weights(avaliable_priorities)
        logger.info(
            f"Sampled priority: {sampled_priority} from {avaliable_priorities} for {endpoint_name}"
        )
        return await bridge.dequeue_job(
            process_func,
            endpoint_name=endpoint_name,
            size=size,
            priority=sampled_priority,
        )

    async def enqueue_result(
        self,
        bridge: str | QueueBridge,
        bridge_result_queue_url: str,
        result: InferenceResult,
        *bridge_args,
        **bridge_kwargs,
    ) -> str:
        if isinstance(bridge, str):
            bridge = self.get_bridge(
                bridge,
                bridge_result_queue_url=bridge_result_queue_url,
                *bridge_args,
                **bridge_kwargs,
            )
        await bridge.enqueue_result(
            result,
        )
        return result.job_id

    async def dequeue_result(
        self,
        bridge: str | QueueBridge,
        bridge_result_queue_url: str,
        process_func: Callable[[InferenceResult], None] | Awaitable[InferenceResult],
        size: int = None,
        *bridge_args,
        **bridge_kwargs,
    ) -> InferenceResult:
        if isinstance(bridge, str):
            bridge = self.get_bridge(
                bridge,
                bridge_result_queue_url=bridge_result_queue_url,
                *bridge_args,
                **bridge_kwargs,
            )
        process_func = ensure_awaitable(process_func)
        return await bridge.dequeue_result(
            process_func,
            size=size,
        )

    async def prune_endpoint(
        self,
        endpoint_name: str,
        bridge: str | QueueBridge,
        *bridge_args,
        **bridge_kwargs,
    ):
        if isinstance(bridge, str):
            bridge = self.get_bridge(
                bridge,
                *bridge_args,
                **bridge_kwargs,
            )
        f = ensure_awaitable(bridge.remove_job_queue)
        await f(endpoint_name)

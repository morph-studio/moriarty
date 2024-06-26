from __future__ import annotations

from typing import Awaitable

from moriarty.log import logger
from moriarty.matrix.job_manager.bridge.plugin import QueueBridge, hookimpl
from moriarty.matrix.job_manager.params import (
    InferenceJob,
    InferenceResult,
    InferenceResultStatus,
)
from moriarty.utils import Singleton


class MockBridge(QueueBridge, metaclass=Singleton):
    register_name = "mock"

    def __init__(
        self,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.bridge_result_queue_url = "result"
        self.endpoints = {}
        self.result_queue = []

    def make_job_queue_url(self, endpoint_name: str, priority: int = None) -> str:
        priority = priority or 100
        endpoint_queue_map = self.endpoints.setdefault(endpoint_name, {})
        endpoint_queue_map.setdefault(priority, [])
        return f"{endpoint_name}.{priority}"

    def remove_job_queue(self, endpoint_name: str, priority: int = None) -> None:
        priority = priority or 100
        endpoint_queue_map = self.endpoints.setdefault(endpoint_name, {})
        endpoint_queue_map[priority] = []

    async def list_avaliable_priorities(self, endpoint_name: str) -> list[int]:
        return list(
            k for k in self.endpoints[endpoint_name].keys() if self.endpoints[endpoint_name][k]
        )

    async def enqueue_job(self, endpoint_name: str, job: InferenceJob, priority: int = None) -> str:
        queue_url = self.make_job_queue_url(endpoint_name, priority=priority)
        k1, k2 = queue_url.split(".")
        self.endpoints[k1][int(k2)].append(job)
        return job.inference_id

    async def dequeue_job(
        self,
        endpoint_name: str,
        process_func: Awaitable[InferenceJob],
        size: int = 1,
        priority: int = None,
    ) -> int:
        queue_url = self.make_job_queue_url(endpoint_name, priority=priority)
        k1, k2 = queue_url.split(".")
        jobs = []
        for _ in range(size):
            if not self.endpoints[k1][int(k2)]:
                break
            jobs.append(self.endpoints[k1][int(k2)].pop(0))

        for job in jobs:
            await process_func(job)
        return len(jobs)

    async def enqueue_result(self, result: InferenceResult) -> str:
        self.result_queue.append(result)
        return result.inference_id

    async def dequeue_result(
        self, process_func: Awaitable[InferenceResult], size: int = 10
    ) -> list[InferenceResult]:
        size = size or 1
        results = []
        for _ in range(size):
            if not self.result_queue:
                break
            results.append(self.result_queue.pop(0))
        for result in results:
            await process_func(result)
        return results


@hookimpl
def register(manager):
    manager.register(MockBridge)

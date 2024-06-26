from __future__ import annotations

import asyncio
import base64
import json
from typing import Awaitable

try:
    from functools import cache
except ImportError:
    from functools import lru_cache as cache

from fastapi.concurrency import run_in_threadpool

from moriarty.log import logger
from moriarty.matrix.job_manager.bridge.plugin import QueueBridge, hookimpl
from moriarty.matrix.job_manager.params import (
    InferenceJob,
    InferenceResult,
    InferenceResultStatus,
)


class MockBridge(QueueBridge):
    register_name = "mock"

    def __init__(
        self,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.endpoints = {}

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
        raise NotImplementedError

    async def enqueue_job(self, job: InferenceJob, priority: int = None) -> str:
        raise NotImplementedError

    async def dequeue_job(
        self,
        endpoint_name: str,
        process_func: Awaitable[InferenceJob],
        size: int = 1,
        priority: int = None,
    ) -> int:
        raise NotImplementedError

    async def enqueue_job(self, endpoint_name: str, job: InferenceJob, priority: int = None) -> str:
        raise NotImplementedError

    async def dequeue_result(
        self, process_func: Awaitable[InferenceResult], size: int = None
    ) -> list[InferenceResult]:
        raise NotImplementedError


@hookimpl
def register(manager):
    manager.register(MockBridge)

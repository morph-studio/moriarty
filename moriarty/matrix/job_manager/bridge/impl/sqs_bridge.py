from __future__ import annotations

import asyncio
from typing import Awaitable

try:
    from functools import cache
except ImportError:
    from functools import lru_cache as cache

from fastapi.concurrency import run_in_threadpool

from moriarty.log import logger
from moriarty.matrix.job_manager.bridge.plugin import QueueBridge, hookimpl
from moriarty.matrix.job_manager.params import InferenceJob, InferenceResult
from moriarty.utils import Singleton


class SQSBridge(QueueBridge):
    register_name = "sqs"

    def __init__(
        self,
        poll_time: int = 1,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.poll_time = poll_time

    @property
    def client(self):
        try:
            import boto3
        except ImportError:
            raise RuntimeError(
                "boto3 is not installed, try install moriarty with `pip install moriarty[matrix]` for all components or `pip install moriarty[sqs]` for sqs only"
            )
        return boto3.client("sqs")

    def make_queue_name(self, endpoint_name: str) -> str:
        return f"moriarty-job-{endpoint_name}"

    @cache
    def make_job_queue_url(self, endpoint_name: str) -> str:
        queue_name = self.make_queue_name(endpoint_name)
        try:
            return self.client.get_queue_url(QueueName=queue_name)["QueueUrl"]
        except self.client.exceptions.QueueDoesNotExist:
            logger.info(f"Create queue: {queue_name} as it does not exist")
            response = self.client.create_queue(QueueName=queue_name)
            return response["QueueUrl"]

    def remove_job_queue(self, endpoint_name: str) -> None:
        queue_name = self.make_queue_name(endpoint_name)
        self.client.delete_queue(QueueUrl=queue_name)
        logger.info(f"Queue {queue_name} removed")

    async def enqueue_job(self, endpoint_name: str, job: InferenceJob) -> str:
        await run_in_threadpool(
            self.client.send_message,
            QueueUrl=self.make_job_queue_url(endpoint_name),
            MessageBody=job.model_dump_json(),
        )
        return job.job_id

    async def dequeue_job(
        self,
        endpoint_name: str,
        process_func: Awaitable[InferenceJob],
        size: int = 1,
    ) -> int:
        url = self.make_job_queue_url(endpoint_name)
        return await self._poll_and_execute(process_func, url, size)

    async def enqueue_result(self, result: InferenceResult) -> str:
        # TODO: Use sagemaker style response
        await run_in_threadpool(
            self.client.send_message,
            QueueUrl=self.bridge_result_queue_url,
            MessageBody=result.model_dump_json(),
        )
        return result.job_id

    async def dequeue_result(self, process_func: Awaitable[InferenceJob], size: int = 10) -> int:
        # TODO: Use sagemaker style response
        return await self._poll_and_execute(process_func, self.bridge_result_queue_url, size)

    async def _poll_and_execute(self, process_func: Awaitable[InferenceJob], url, size) -> int:
        if size and size > 10:
            messages = []
            while len(messages) < size:
                messages_len_before = len(messages)
                messages.extend(self._poll_message(url, 10))
                if len(messages) == messages_len_before:
                    # No message left
                    break
        else:
            messages = self._poll_message(url, size)

        await asyncio.gather(
            *[
                self._wrap_process_func(process_func, job, url, receipt_handle)
                for job, receipt_handle in messages
            ]
        )
        return len(messages)

    def _poll_message(self, url, size: int = None) -> list[tuple[InferenceJob, str]]:
        size = size or 1
        valid_messages = []
        response = self.client.receive_message(
            QueueUrl=url,
            MaxNumberOfMessages=size,
            WaitTimeSeconds=self.poll_time,
        )
        if "Messages" not in response:
            return valid_messages

        for message in response["Messages"]:
            receipt_handle = message["ReceiptHandle"]
            try:
                body = message["Body"]
                message = InferenceJob.model_validate_json(body)
            except Exception as e:
                logger.error(f"Error parsing message {body}: {e}")
                logger.exception(e)
            else:
                valid_messages.append((message, receipt_handle))
        return valid_messages

    async def _wrap_process_func(
        self,
        process_func: Awaitable[InferenceJob],
        job: InferenceJob,
        url: str,
        receipt_handle: str,
    ):
        try:
            await process_func(job)
        except Exception as e:
            logger.error(f"Error processing job {job.job_id}: {e}")
            logger.exception(e)
            raise
        else:
            await run_in_threadpool(
                self.client.delete_message,
                QueueUrl=url,
                ReceiptHandle=receipt_handle,
            )


@hookimpl
def register(manager):
    manager.register(SQSBridge)

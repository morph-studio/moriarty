from __future__ import annotations

import asyncio
import base64
import io
import json
import uuid
from typing import Awaitable

from moriarty.tools import parse_s3_path

try:
    from functools import cache
except ImportError:
    from functools import lru_cache as cache

import boto3
from fastapi.concurrency import run_in_threadpool

from moriarty.log import logger
from moriarty.matrix.job_manager.bridge.plugin import QueueBridge, hookimpl
from moriarty.matrix.job_manager.params import (
    InferenceJob,
    InferenceResult,
    InferenceResultStatus,
)


class SQSBridge(QueueBridge):
    """
    Bridge to SQS. Compatible with AWS SageMaker AsyncInference.

    Will create SQS queue for each endpoint.

    Args:
        bridge_result_queue_url: AWS SQS URL for result queue
        poll_time: polling time in seconds
    """

    register_name = "sqs"

    def __init__(
        self,
        bridge_result_queue_url: str = None,
        poll_time: int = 1,
        *args,
        **kwargs,
    ):
        super().__init__(bridge_result_queue_url, *args, **kwargs)
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

    def _get_queue_url_prefix(self, endpoint_name: str) -> str:
        return f"moriarty-bridge-{endpoint_name}"

    def make_queue_name(self, endpoint_name: str, priority: int = None) -> str:
        priority = priority or 100
        return f"{self._get_queue_url_prefix(endpoint_name)}-{priority}"

    async def list_avaliable_priorities(self, endpoint_name: str) -> list[int]:
        def _():
            priorities = []
            response = self.client.list_queues(
                QueueNamePrefix=self._get_queue_url_prefix(endpoint_name)
            )
            if "QueueUrls" not in response:
                return priorities

            while response["QueueUrls"]:
                for url in response["QueueUrls"]:
                    response = self.client.get_queue_attributes(
                        QueueUrl=url, AttributeNames=["ApproximateNumberOfMessages"]
                    )
                    if int(response["Attributes"]["ApproximateNumberOfMessages"]) == 0:
                        continue

                    queue_name = url.split("/")[-1]
                    priority = int(queue_name.split("-")[-1])
                    priorities.append(priority)
                if "NextToken" in response:
                    response = self.client.list_queues(
                        QueueNamePrefix=self._get_queue_url_prefix(endpoint_name),
                        NextToken=response["NextToken"],
                    )
                else:
                    break
            return sorted(priorities)

        return await run_in_threadpool(_)

    @cache
    def make_job_queue_url(self, endpoint_name: str, priority: int = None) -> str:
        queue_name = self.make_queue_name(endpoint_name, priority)
        try:
            return self.client.get_queue_url(QueueName=queue_name)["QueueUrl"]
        except self.client.exceptions.QueueDoesNotExist:
            logger.info(f"Create queue: {queue_name} as it does not exist")
            response = self.client.create_queue(QueueName=queue_name)
            return response["QueueUrl"]

    def remove_job_queue(self, endpoint_name: str, priority: int = None) -> None:
        queue_name = self.make_queue_name(endpoint_name, priority)
        self.client.delete_queue(QueueUrl=queue_name)
        logger.info(f"Queue {queue_name} removed")

    async def enqueue_job(self, endpoint_name: str, job: InferenceJob, priority: int = None) -> str:
        queue_url = self.make_job_queue_url(endpoint_name, priority=priority)
        await run_in_threadpool(
            self.client.send_message,
            QueueUrl=queue_url,
            MessageBody=job.model_dump_json(),
        )
        logger.debug(f"Job {job.inference_id} enqueued")
        return job.inference_id

    async def dequeue_job(
        self,
        endpoint_name: str,
        process_func: Awaitable[InferenceJob],
        size: int = 1,
        priority: int = None,
    ) -> int:
        url = self.make_job_queue_url(endpoint_name, priority)
        logger.debug(f"Polling job from queue: {url}")
        return await self._poll_job_and_execute(process_func, url, size)

    async def enqueue_result(self, result: InferenceResult) -> str:
        invocation_status = (
            "Completed" if result.status == InferenceResultStatus.COMPLETED else "Failed"
        )
        response_body = (
            {
                "content": base64.b64encode(result.payload.encode()).decode(),
                "message": result.message,
                "encoding": "BASE64",
            }
            if result.status == InferenceResultStatus.COMPLETED
            else {
                "content": result.payload,
                "message": result.message,
            }
        )
        # If content is bigger than 255KB, drop it
        if len(response_body["content"]) > 255 * 1024:
            if not self.output_bucket:
                raise RuntimeError(
                    "Cannot handle big content without output bucket, please set MORIARTY_BRIDGE_OUTPUT_BUCKET env"
                )
            response_body["content"] = "Content too long, will upload to S3"
            if "encoding" in response_body:
                del response_body["encoding"]

        message_content = {
            "invocationStatus": invocation_status,
            "eventName": "InferenceResult",
            "responseBody": response_body,
            "inferenceId": result.inference_id,
            "eventName": "InferenceResult",
        }

        if self.output_bucket:
            output_location = await self._upload_to_s3(result.payload)
            message_content["responseParameters"] = {
                "contentType": "text/plain; charset=utf-8",
                "outputLocation": output_location,
            }

        message = {"Message": json.dumps(message_content)}
        await run_in_threadpool(
            self.client.send_message,
            QueueUrl=self.bridge_result_queue_url,
            MessageBody=json.dumps(message),
        )
        logger.debug(f"Result {result.inference_id} enqueued")
        return result.inference_id

    async def _upload_to_s3(self, payload: str | None):
        if payload is None:
            return None

        if not payload:
            payload = ""

        fp = io.BytesIO(payload.encode())

        def _():
            s3_client = boto3.client("s3")
            fp.seek(0)
            key = f"moriarty-async-inference/output/{uuid.uuid4().hex}.out"
            s3_client.upload_fileobj(
                Fileobj=fp,
                Bucket=self.output_bucket,
                Key=key,
            )
            return f"s3://{self.output_bucket}/{key}"

        return await run_in_threadpool(_)

    async def _get_s3_content(self, output_location: str) -> str:
        def _():
            s3_resource = boto3.resource("s3")
            bucket_name, object_name = parse_s3_path(output_location)
            response = s3_resource.Object(bucket_name, object_name).get()
            return response["Body"].read().decode("utf-8")

        return await run_in_threadpool(_)

    async def dequeue_result(self, process_func: Awaitable[InferenceResult], size: int = 10) -> int:
        logger.debug(f"Polling result from queue: {self.bridge_result_queue_url}")
        return await self._poll_result_and_execute(process_func, self.bridge_result_queue_url, size)

    async def _poll_job_and_execute(self, process_func: Awaitable[InferenceJob], url, size) -> int:
        if size and size > 10:
            messages = []
            while len(messages) < size:
                messages_len_before = len(messages)
                messages.extend(await self._poll_message(url, 10))
                if len(messages) == messages_len_before:
                    # No message left
                    break
        else:
            messages = await self._poll_job_message(url, size)

        await asyncio.gather(
            *[
                self._wrap_process_func(process_func, job, url, receipt_handle)
                for job, receipt_handle in messages
            ]
        )
        return len(messages)

    async def _poll_job_message(self, url, size: int = None) -> list[tuple[InferenceJob, str]]:
        size = size or 1
        valid_messages = []
        response = await run_in_threadpool(
            self.client.receive_message,
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

    async def _poll_result_and_execute(
        self, process_func: Awaitable[InferenceResult], url, size
    ) -> int:
        if size and size > 10:
            messages = []
            while len(messages) < size:
                messages_len_before = len(messages)
                messages.extend(await self._poll_message(url, 10))
                if len(messages) == messages_len_before:
                    # No message left
                    break
        else:
            messages = await self._poll_result_message(url, size)

        logger.debug(f"Got {len(messages)} messages")
        for result, receipt_handle in messages:
            await self._wrap_process_func(
                process_func, result, self.bridge_result_queue_url, receipt_handle
            )
        return len(messages)

    async def _poll_result_message(
        self, url, size: int = None
    ) -> list[tuple[InferenceResult, str]]:
        size = size or 1
        valid_messages = []
        response = await run_in_threadpool(
            self.client.receive_message,
            QueueUrl=url,
            MaxNumberOfMessages=size,
            WaitTimeSeconds=self.poll_time,
        )
        if "Messages" not in response:
            return valid_messages

        for message in response["Messages"]:
            receipt_handle = message["ReceiptHandle"]
            try:
                message_body = json.loads(message["Body"])
                message = json.loads(message_body["Message"])
            except Exception as e:
                logger.error(f"Error parsing message {message}: {e}")
                logger.exception(e)
                continue
            if "eventName" in message and message["eventName"] != "InferenceResult":
                logger.warning(f"Unexpected message {message}. Message will be ignored.")
                continue
            if "invocationStatus" in message and message["invocationStatus"] == "Failed":
                logger.debug(f"Result {message['inferenceId']} failed")
                valid_messages.append(
                    (
                        InferenceResult(
                            inference_id=message["inferenceId"],
                            status=InferenceResultStatus.FAILED,
                            message=message["responseBody"]["message"],
                            payload=message["responseBody"]["content"],
                        ),
                        receipt_handle,
                    )
                )
                continue
            try:
                logger.debug(f"Result {message['inferenceId']} completed")
                inference_result = InferenceResult(
                    inference_id=message["inferenceId"],
                    status=InferenceResultStatus.COMPLETED,
                    message=message["responseBody"]["message"],
                    payload=base64.b64decode(message["responseBody"]["content"]).decode("utf-8"),
                )
                valid_messages.append(
                    (
                        inference_result,
                        receipt_handle,
                    )
                )
                continue
            except Exception as e:
                logger.info(f"Cannot parse inference result {message}, try output location.")
                output_location = message["responseParameters"].get("outputLocation")
                if not output_location:
                    logger.error(
                        f"Neither response body parsed nor output location, can't handle now: {message}"
                    )
                    logger.exception(e)
                    continue

                inference_result = InferenceResult(
                    inference_id=message["inferenceId"],
                    status=InferenceResultStatus.COMPLETED,
                    message=message["responseBody"]["message"],
                    payload=await self._get_s3_content(output_location),
                )
                valid_messages.append(
                    (
                        inference_result,
                        receipt_handle,
                    )
                )
                continue

        return valid_messages

    async def _wrap_process_func(
        self,
        process_func: Awaitable[InferenceJob] | Awaitable[InferenceResult],
        job: InferenceJob,
        url: str,
        receipt_handle: str,
    ):
        try:
            await process_func(job)
        except Exception as e:
            logger.error(f"Error processing job {job.inference_id}: {e}")
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

from __future__ import annotations

import asyncio
import base64
import io
import os
import json
import uuid
from typing import Awaitable

from moriarty.tools import parse_gcs_path

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

from google.api_core.exceptions import (
    AlreadyExists,
    NotFound,
    GoogleAPICallError,
    InvalidArgument,
)
from google.cloud import pubsub_v1
from google.cloud import storage


class PubSubBridge(QueueBridge):
    """
    Bridge to GCP Pub/Sub. Compatible with GCP services.

    Will creater Pub/Sub topics and subscriptions for each endpoint

    Args:
        bridge_result_topic: GCP Pub/Sub topic for result messages
    """

    register_name = 'pubsub'

    def __init__(
        self,
        bridge_result_topic: str = None,
        project_id: str = None,
        output_bucket: str = None,
        *args,
        **kwargs,
    ):
        super().__init__(bridge_result_topic, *args, **kwargs)

        self.publisher_client = pubsub_v1.PublisherClient()
        self.subscriber_client = pubsub_v1.SubscriberClient()
        self.output_bucket = output_bucket

        if not bridge_result_topic:
            raise ValueError("Bridge result topic must be provided.")

        self.project_id = project_id or os.environ.get("GOOGLE_CLOUD_PROJECT")
        if not self.project_id:
            raise ValueError("Project ID must be provided or set in the environment variable GOOGLE_CLOUD_PROJECT")

        self.bridge_result_topic_path = self._ensure_topic(
            topic_path=f"projects/{self.project_id}/topics/{bridge_result_topic}"
        )
        self.bridge_result_subscription_path = self._ensure_subscription(
            topic_path = self.bridge_result_topic_path,
            subscription_path = f"projects/{self.project_id}/subscriptions/{bridge_result_topic}-sub",
        )


    def _get_topic_prefix(self, endpoint_name: str) -> str:
        return f"moriarty-bridge-{endpoint_name}"

    def _get_subscription_prefix(self, endpoint_name: str) -> str:
        return f"moriarty-bridge-{endpoint_name}-sub"

    def _get_topic_path(self, endpoint_name: str = None, priority: int = None) -> str:
        priority = priority or 100
        topic_id = self._get_topic_prefix(endpoint_name)
        return self.publisher_client.topic_path(self.project_id, topic_id)

    def _get_subscription_path(self, endpoint_name: str = None, priority: int = None) -> str:
        priority = priority or 100
        subscription_id = self._get_subscription_prefix(endpoint_name)
        return self.subscriber_client.subscription_path(self.project_id, subscription_id)

    @cache
    def _ensure_topic(self, topic_path: str = None) -> str:
        try:
            self.publisher_client.get_topic(request={"topic": topic_path})
            logger.info(f"Topic already exists: {topic_path}")
        except NotFound:
            self.publisher_client.create_topic(request={"name": topic_path})
            logger.info(f"Created topic: {topic_path}")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
        return topic_path

    @cache
    def _ensure_subscription(self, topic_path: str = None, subscription_path: str = None) -> str:
        try:
            self.subscriber_client.get_subscription(request={"subscription": subscription_path})
            logger.info(f"Subscription already exists: {subscription_path}")
        except NotFound:
            self.subscriber_client.create_subscription(
                request={"name": subscription_path, "topic": topic_path}
            )
            logger.info(f"Created subscription: {subscription_path}")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
        return subscription_path

    def _delete_subscription(self, subscription_path: str):
        try:
            self.subscriber_client.delete_subscription(subscription=subscription_path)
            logger.info(f"Deleted subscription: {subscription_path}")
        except NotFound:
            logger.info(f"Subscription not found (already deleted): {subscription_path}")

    def _delete_topic(self, topic_path: str):
        try:
            self.publisher_client.delete_topic(topic=topic_path)
            logger.info(f"Deleted topic: {topic_path}")
        except NotFound:
            logger.info(f"Topic not found (already deleted): {topic_path}")

    async def publish_job(self, endpoint_name: str, job: InferenceJob, priority: int = None) -> str:
        topic_path = self._ensure_topic(self._get_topic_path(endpoint_name, priority))

        data = job.model_dump_json().encode("utf-8")
        future = self.publisher_client.publish(topic_path, data)
        message_id = await asyncio.wrap_future(future)
        logger.debug(f"Job {job.inference_id} published with message ID {message_id}")
        return job.inference_id

    async def subscribe_job(
        self,
        endpoint_name: str,
        process_func: Awaitable[InferenceJob],
        priority: int = None,
        size: int = None,
    ) -> int:
        subscription_path = self._ensure_subscription(
            topic_path=self._get_topic_path(endpoint_name, priority),
            subscription_path=self._get_subscription_path(endpoint_name, priority),
        )
        logger.debug(f"Polling job from subscription: {subscription_path}")
        try:
            return await self._pull_messages_and_execute(process_func, subscription_path, size)
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            await asyncio.sleep(5)
            return 0

    async def _pull_messages_and_execute(
        self,
        process_func: Awaitable[InferenceJob],
        subscription_path: str,
        max_messages: int,
    ) -> int:

        response = await run_in_threadpool(
            self.subscriber_client.pull,
            request={
                "subscription": subscription_path,
                "max_messages": max_messages,
                "return_immediately": False  # Ensures that pull waits until messages are available
            }
        )

        received_messages = response.received_messages
        if not received_messages:
            logger.info("No messages received.")
            return 0

        tasks = []
        processed_count = 0

        for received_message in received_messages:
            ack_id = received_message.ack_id
            message = received_message.message
            try:
                message_data = message.data.decode("utf-8")
                job = InferenceJob.model_validate_json(message_data)
                logger.debug(f"Prepared InferenceJob for inference ID: {job.inference_id}")
            except Exception as e:
                logger.error(f"Error parsing message {message_data}: {e}")
                continue

            # Create a task for processing and acknowledging the message
            task = asyncio.create_task(
                self._process_and_ack(
                    result=job,
                    subscription_path=subscription_path,
                    process_func=process_func,
                    ack_id=ack_id
                )
            )
            tasks.append(task)

        if tasks:
            await asyncio.gather(*tasks)
            processed_count = len(tasks)

        return processed_count

    async def publish_result(self, result: InferenceResult) -> str:
        invocation_status = (
            "Completed" if result.status == InferenceResultStatus.COMPLETED else "Failed"
        )
        # response_body = {
        #     "content": base64.b64decode(result.payload.encode()).decode(),
        #     "message": result.message,
        #     "encoding": "BASE64",
        # }
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
            response_body["content"] = "Content too long, will upload to GCS"
            if "encoding" in response_body:
                del response_body["encoding"]

        message_content = {
            "invocationStatus": invocation_status,
            "eventName": "InferenceResult",
            "responseBody": response_body,
            "inferenceId": result.inference_id,
        }

        if self.output_bucket:
            output_location = await self._upload_to_gcs(result.payload)
            logger.info(f"payload uploaded to gcs {output_location}")
            message_content["responseParameters"] = {
                "contentType": "text/plain; charset=utf-8",
                "outputLocation": output_location,
            }

        data = json.dumps(message_content).encode("utf-8")
        # data = {"Message": json.dumps(message_content)}
        future = self.publisher_client.publish(self.bridge_result_topic_path, data)
        # future = self.publisher_client.publish(self.bridge_result_topic_path, json.dumps(data))
        message_id = await asyncio.wrap_future(future)
        logger.debug(f"Result {result.inference_id} published with message ID: {message_id}")

        return result.inference_id

    async def subscribe_result(self, process_func: Awaitable[InferenceResult], size: int = 10) -> int:
        subscription_path = self._ensure_subscription(
            topic_path=self.bridge_result_topic_path,
            subscription_path=self.bridge_result_subscription_path,
        )

        logger.debug(f"Polling result from subscription: {subscription_path}")
        try:
            return await self._pull_results_and_execute(process_func, subscription_path, size)
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            await asyncio.sleep(5)  # Backoff before retrying
            return 0

    async def _pull_results_and_execute(
        self,
        process_func: Awaitable[InferenceResult],
        subscription_path: str,
        max_messages: int,
    ) -> int:
        response = await run_in_threadpool(
            self.subscriber_client.pull,
            request={
                "subscription": subscription_path,
                "max_messages": max_messages,
                "return_immediately": False  # Ensures that pull waits until messages are available
            }
        )
        received_messages = response.received_messages
        if not response.received_messages:
            logger.info("No messages received.")
            return 0

        tasks = []
        processed_count = 0

        for received_message in received_messages:
            ack_id = received_message.ack_id
            message = received_message.message
            try:
                message_data = json.loads(message.data.decode("utf-8"))

                try:
                    payload = base64.b64decode(message["responseBody"]["content"]).decode("utf-8")
                except Exception as e:
                    logger.info(f"Cannot parse inference result {message_data["inferenceId"]}, fetching from GCS.")
                    output_location = message_data["responseParameters"].get("outputLocation")
                    logger.info(f"output_location: {output_location}")
                    payload = await self._get_gcs_content(output_location)

                result = InferenceResult(
                    inference_id = message_data["inferenceId"],
                    status=InferenceResultStatus.COMPLETED if message_data["invocationStatus"] == "Completed" else InferenceResultStatus.FAILED,
                    message=message_data["responseBody"]["message"],
                    # payload=base64.b64decode(message_data["responseBody"]["content"]).decode("utf-8"),
                    # payload=message_data["responseBody"]["content"],
                    payload=payload,
                )
                logger.debug(f"Prepared InferenceResult for inference ID: {result.inference_id}")
            except Exception as e:
                logger.error(f"Error parsing message {message_data}: {e}")
                continue

            # Create a task for processing and acknowledging the message
            task = asyncio.create_task(
                self._process_and_ack(
                    result=result,
                    subscription_path=subscription_path,
                    process_func=process_func,
                    ack_id=ack_id
                )
            )
            tasks.append(task)

        if tasks:
            await asyncio.gather(*tasks)
            processed_count = len(tasks)

        return processed_count

    async def _process_and_ack(
        self,
        result: InferenceResult,
        subscription_path: str,
        process_func: Awaitable[InferenceResult],
        ack_id: str,
        additional_seconds: int=120,
    ):
        try:
            if "invocationStatus" in result and result.status == InferenceResultStatus.FAILED:
                logger.debug(f"Inference ID {result.inference_id} failed with message: {result.message}")
            elif "eventName" in result and result.eventName != "InferenceResult":
                logger.warning(f"Unexpected message {result}. Message will be ignored.")
            else:
                await process_func(result)
                logger.info(f"Processed message ID: {result.inference_id}")

            # Acknowledge the message after successful processing
            await run_in_threadpool(
                self.subscriber_client.acknowledge,
                request={
                    "subscription": subscription_path,
                    "ack_ids": [ack_id],
                }
            )
            logger.info(f"Acknowledged message ID: {result.inference_id}")

        except Exception as e:
            logger.error(f"Error processing message {result.inference_id}: {e}")

            try:
                # Extend the ack deadline by additional_seconds
                await run_in_threadpool(
                    self.subscriber_client.modify_ack_deadline,
                    request={
                        "subscription": subscription_path,
                        "ack_ids": [ack_id],
                        "ack_deadline_seconds": additional_seconds
                    }
                )
                logger.info(f"Extended ack deadline by {additional_seconds} seconds for ack_ids: {[ack_id]}")
            except InvalidArgument:
                logger.warning(f"Cannot extend ack deadline for already acknowledged ack_id: {ack_id}")
            except GoogleAPICallError as api_error:
                logger.error(f"API error when extending ack deadline for ack_id {ack_id}: {api_error}")
            except Exception as ex:
                logger.error(f"Unexpected error when extending ack deadline for ack_id {ack_id}: {ex}")


    async def _upload_to_gcs(self, payload: str) -> str:
        storage_client = storage.Client()
        bucket = storage_client.bucket(self.output_bucket)
        blob_name = f"moriarty-async-inference/output/{uuid.uuid4().hex}.out"
        blob = bucket.blob(blob_name)
        blob.upload_from_string(payload)
        return f"gs://{self.output_bucket}/{blob_name}"

    async def _get_gcs_content(self, output_location: str) -> str:
        storage_client = storage.Client()
        bucket_name, blob_name = parse_gcs_path(output_location)
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        return blob.download_as_text()

    @hookimpl
    def register(manager):
        manager.register(PubSubBridge)

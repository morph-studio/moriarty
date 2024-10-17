from __future__ import annotations

import asyncio
import base64
import io
import os
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

from google.api_core.exceptions import (
    AlreadyExists,
    NotFound,
    GoogleAPICallError,
    InvalidArgument,
)
from google.cloud import pubsub_v1


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
        bridge_result_queue_url: str = None,
        project_id: str = None,
        *args,
        **kwargs,
    ):
        super().__init__(bridge_result_queue_url, *args, **kwargs)

        bridge_result_topic = bridge_result_queue_url

        self.publisher_client = pubsub_v1.PublisherClient()
        self.subscriber_client = pubsub_v1.SubscriberClient()

        if not bridge_result_topic:
            raise ValueError("Bridge result topic must be provided.")

        self.project_id = project_id or os.environ.get("GOOGLE_CLOUD_PROJECT")
        if not self.project_id:
            raise ValueError("Project ID must be provided or set in the environment variable GOOGLE_CLOUD_PROJECT")

        self.bridge_result_topic_path, self.bridge_result_subscription_path = self._format_pubsub_paths(self.project_id, bridge_result_topic)
        
        self.bridge_result_topic_path = self._ensure_topic(self.bridge_result_topic_path)
        self.bridge_result_subscription_path = self._ensure_subscription(
            topic_path = self.bridge_result_topic_path,
            subscription_path = self.bridge_result_subscription_path,
        )

    def _format_pubsub_paths(self, project_id, bridge_result_topic):
        expected_topic_prefix = f"projects/{project_id}/topics/"

        if bridge_result_topic.startswith(expected_topic_prefix):
            topic_path = bridge_result_topic
            topic_name = bridge_result_topic[len(expected_topic_prefix):]
        else:
            topic_path = f"{expected_topic_prefix}{bridge_result_topic}"
            topic_name = bridge_result_topic

        subscription_path = f"projects/{project_id}/subscriptions/{topic_name}-sub"

        return topic_path, subscription_path

    def _get_topic_prefix(self, endpoint_name: str) -> str:
        return f"moriarty-bridge-{endpoint_name}"

    def _get_subscription_prefix(self, topic_id: str) -> str:
        return f"{topic_id}-sub"

    def _get_topic_path(self, endpoint_name: str = None, priority: int = None) -> str:
        priority = priority or 100
        topic_id = f"{self._get_topic_prefix(endpoint_name)}-{priority}"
        return self.publisher_client.topic_path(self.project_id, topic_id)

    def _get_subscription_path(self, endpoint_name: str = None, priority: int = None) -> str:
        priority = priority or 100
        topic_id = f"{self._get_topic_prefix(endpoint_name)}-{priority}"
        subscription_id = self._get_subscription_prefix(topic_id)
        return self.subscriber_client.subscription_path(self.project_id, subscription_id)
    
    def make_queue_name(self, endpoint_name: str, priority: int = None) -> str:
        priority = priority or 100
        return self._get_topic_path(endpoint_name, priority)
    
    @cache
    def _ensure_topic(self, topic_path: str = None) -> str:
        try:
            self.publisher_client.get_topic(request={"topic": topic_path})
            logger.info(f"Topic already exists: {topic_path}")
        except NotFound:
            topic = self.publisher_client.create_topic(request={"name": topic_path})
            logger.info(f"Created topic: {topic.name}")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
        return topic_path

    @cache
    def _ensure_subscription(self, topic_path: str = None, subscription_path: str = None) -> str:
        try:
            self.subscriber_client.get_subscription(request={"subscription": subscription_path})
            logger.info(f"Subscription already exists: {subscription_path}")
        except NotFound:
            subscription = self.subscriber_client.create_subscription(request={"name": subscription_path, "topic": topic_path})
            logger.info(f"Created subscription: {subscription.name}")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
        return subscription_path
    
    def make_job_queue_url(self, endpoint_name: str, priority: int = None) -> str:
        return self._ensure_topic(self.make_queue_name(endpoint_name, priority))

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

    def remove_job_queue(self, endpoint_name: str, priority: int = None) -> None:
        topic_path = self._get_topic_path(endpoint_name, priority)
        try:
            project_id, topic_id = self._parse_topic_path(topic_path)
            subscription_id = self._get_subscription_prefix(topic_id)
            subscription_path = self.subscriber_client.subscription_path(project_id, subscription_id)
            self._delete_subscription(subscription_path)
            logger.info(f"deleted associated subscription_path {subscription_path}")
        except Exception as e:
            logger.info(f"Error {e} while removing subscription path {subscription_path}")
        self._delete_topic(self._get_topic_path(endpoint_name, priority))

    async def list_avaliable_priorities(self, endpoint_name: str) -> list[int]:
        def _():
            priorities = []
            topic_prefix = self._get_topic_prefix(endpoint_name)
            try:
                # List all topics in the project
                project_path = f"projects/{self.project_id}"
                topics = self.publisher_client.list_topics(request={"project": project_path})
                
                for topic in topics:
                    topic_name = topic.name.split('/')[-1]
                    if topic_name.startswith(topic_prefix):
                        # Extract priority from topic name
                        try:
                            priority_str = topic_name.split('-')[-1]
                            priority = int(priority_str)
                        except ValueError:
                            logger.warning(f"Invalid priority format in topic name: {topic_name}")
                            continue

                        # Construct corresponding subscription path
                        subscription_id = f"moriarty-bridge-{endpoint_name}-{priority}-sub"
                        subscription_path = self.subscriber_client.subscription_path(
                            self.project_id, subscription_id
                        )

                        try:
                            # Attempt to pull a single message to check for pending messages
                            response = self.subscriber_client.pull(
                                request={
                                    "subscription": subscription_path,
                                    "max_messages": 1,
                                }
                            )
                            if response.received_messages:
                                priorities.append(priority)
                        except NotFound:
                            logger.warning(f"Subscription not found: {subscription_path}")
                            continue
                        except GoogleAPICallError as e:
                            logger.error(f"Error pulling messages from {subscription_path}: {e}")
                            continue

                return sorted(priorities)
            except GoogleAPICallError as e:
                logger.error(f"Error listing topics: {e}")
                return priorities

        return await asyncio.to_thread(_)

    async def enqueue_job(self, endpoint_name: str, job: InferenceJob, priority: int = None) -> str:
        topic_path = self._ensure_topic(self._get_topic_path(endpoint_name, priority))

        data = job.model_dump_json().encode("utf-8")
        future = self.publisher_client.publish(topic_path, data)
        message_id = await asyncio.wrap_future(future)
        logger.debug(f"Job {job.inference_id} published with message ID {message_id}")
        return job.inference_id

    async def dequeue_job(
        self,
        endpoint_name: str,
        process_func: Awaitable[InferenceJob],
        size: int = None,
        priority: int = None,
    ) -> int:
        size = size or 1
        subscription_path = self._ensure_subscription(
            topic_path=self._get_topic_path(endpoint_name, priority),
            subscription_path=self._get_subscription_path(endpoint_name, priority),
        )
        logger.info(f"Polling job from subscription: {subscription_path}")
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
            output_location = await self._upload_to_s3(result.payload)
            logger.info(f"payload uploaded to gcs {output_location}")
            message_content["responseParameters"] = {
                "contentType": "text/plain; charset=utf-8",
                "outputLocation": output_location,
            }

        data = json.dumps(message_content).encode("utf-8")
        future = self.publisher_client.publish(self.bridge_result_topic_path, data)
        message_id = await asyncio.wrap_future(future)
        logger.debug(f"Result {result.inference_id} published with message ID: {message_id}")

        return result.inference_id

    async def dequeue_result(self, process_func: Awaitable[InferenceResult], size: int = 10) -> int:
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
            return []

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
                # "return_immediately": False  # Ensures that pull waits until messages are available
            }
        )
        received_messages = response.received_messages
        if not response.received_messages:
            logger.info("No messages received.")
            return []

        tasks = []
        valid_messages = []
        processed_count = 0
        invalid_ack_ids = []

        for received_message in received_messages:
            ack_id = received_message.ack_id
            message = received_message.message
            try:
                message_data = json.loads(message.data.decode("utf-8"))
            except Exception as e:
                logger.error(f"Error parsing message {message_data}: {e}")
                continue
            if "eventName" in message_data and message_data["eventName"] != "InferenceResult":
                logger.warning(f"Unexpected message {message_data}. Message will be ignored.")
                invalid_ack_ids.append(ack_id)
                continue

            if "invocationStatus" in message_data and message_data["invocationStatus"] == "Failed":
                valid_messages.append((
                    InferenceResult(
                        inference_id=message_data["inferenceId"],
                        status=InferenceResultStatus.FAILED,
                        message=message_data["responseBody"]["message"],
                        payload=message_data["responseBody"]["content"],
                    ),
                    ack_id,
                ))
                continue
            try:
                logger.info(f"Result {message_data['inferenceId']} completed")
                valid_messages.append((
                    InferenceResult(
                        inference_id=message_data["inferenceId"],
                        status=InferenceResultStatus.COMPLETED,
                        message=message_data["responseBody"]["message"],
                        payload=base64.b64decode(message_data["responseBody"]["content"]).decode("utf-8"),
                    ),
                    ack_id,
                ))
                continue
            except Exception as e:
                logger.info(f"Cannot parse inference result {message_data}, try output location.")
                output_location = message_data["responseParameters"].get("outputLocation")
                if not output_location:
                    logger.error(
                        f"Neither response body parsed nor output location, can't handle now: {message_data}"
                    )
                    logger.exception(e)
                    invalid_ack_ids.append(ack_id)
                    continue

                valid_messages.append((
                    InferenceResult(
                        inference_id = message_data["inferenceId"],
                        status=InferenceResultStatus.COMPLETED,
                        message=message_data["responseBody"]["message"],
                        payload=await self._get_s3_content(output_location),
                    ),
                    ack_id,
                ))
                continue
            
        if invalid_ack_ids:
            await run_in_threadpool(
                self.subscriber_client.acknowledge,
                request={
                    "subscription": subscription_path,
                    "ack_ids": invalid_ack_ids,
                }
            )
            logger.info(f"Acknowledged {len(invalid_ack_ids)} invalid messages")

        processed_inference_ids = []
        for result, ack_id in valid_messages:
            processed_id = await self._process_and_ack(
                result=result,
                subscription_path=subscription_path,
                process_func=process_func,
                ack_id=ack_id
            )
            if processed_id is not None:
                processed_inference_ids.append(processed_id)

        return processed_inference_ids

    async def _process_and_ack(
        self,
        result: InferenceResult,
        subscription_path: str,
        process_func: Awaitable[InferenceResult],
        ack_id: str,
        additional_seconds: int=120,
    ):
        try:
            await process_func(result)
            logger.info(f"Processed message: {result.inference_id}")
            # Acknowledge the message after successful processing
            await run_in_threadpool(
                self.subscriber_client.acknowledge,
                request={
                    "subscription": subscription_path,
                    "ack_ids": [ack_id],
                }
            )
            logger.info(f"Acknowledged message: {result.inference_id}")
            return result.inference_id
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
            return None

    def _parse_topic_path(topic_path):
        parts = topic_path.split('/')
        if len(parts) != 4 or parts[0] != 'projects' or parts[2] != 'topics':
            raise ValueError("Invalid topic path format")
        return parts[1], parts[3]

    def _parse_subscription_path(subscription_path):
        parts = subscription_path.split('/')
        if len(parts) != 4 or parts[0] != 'projects' or parts[2] != 'subscriptions':
            raise ValueError("Invalid subscription path format")
        return parts[1], parts[3]

    # async def _upload_to_gcs(self, payload: str) -> str:
    #     storage_client = storage.Client()
    #     bucket = storage_client.bucket(self.output_bucket)
    #     blob_name = f"moriarty-async-inference/output/{uuid.uuid4().hex}.out"
    #     blob = bucket.blob(blob_name)
    #     blob.upload_from_string(payload)
    #     return f"gs://{self.output_bucket}/{blob_name}"

    # async def _get_gcs_content(self, output_location: str) -> str:
    #     storage_client = storage.Client()
    #     bucket_name, blob_name = parse_gcs_path(output_location)
    #     bucket = storage_client.bucket(bucket_name)
    #     blob = bucket.blob(blob_name)
    #     return blob.download_as_text()

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

    @hookimpl
    def register(manager):
        manager.register(PubSubBridge)

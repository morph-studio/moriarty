import asyncio
import sys
from functools import partial
from urllib.parse import urljoin

import httpx
import redis.asyncio as redis
from brq.consumer import Consumer
from brq.daemon import Daemon

from moriarty.log import logger
from moriarty.sidecar.params import InferenceProxyStatus, MatrixCallback
from moriarty.sidecar.producer import JobProducer

if sys.version_info >= (3, 11):
    import asyncio as async_timeout
else:
    import async_timeout

from .vars import GROUP_NAME, REDIS_PREFIX


class InferencerConsumer:
    REDIS_PREFIX = REDIS_PREFIX
    GROUP_NAME = GROUP_NAME

    def __init__(
        self,
        redis_client: redis.Redis | redis.RedisCluster,
        endpoint_name: str,
        invoke_host: str = "localhost",
        invoke_port: int = "8080",
        invoke_path: str = "/invocations",
        health_check_path: str = "/ping",
        callbacl_url: str = "http://moriarty-operator-callback:8999/callback",
        enable_retry: bool = True,
        enable_ssl: bool = False,
        concurrency: int = 1,
        process_timeout: int = 3600,
        health_check_timeout: int = 1200,
        health_check_interval: int = 5,
        **kwargs,
    ):
        self.redis_client = redis_client
        self.process_timeout = process_timeout
        self.health_check_timeout = health_check_timeout
        self.health_check_interval = health_check_interval

        scheme = "https" if enable_ssl else "http"
        proxy_url = f"{scheme}://{invoke_host}:{invoke_port}"
        self.invoke_url = urljoin(proxy_url, invoke_path)
        self.health_check_path = urljoin(proxy_url, health_check_path)
        self.callback_url = callbacl_url
        self.endpoint_name = endpoint_name
        self.enable_retry = enable_retry

        self._producer = JobProducer(
            redis_client=self.redis_client,
        )
        self._consumer_builder = partial(
            Consumer,
            self.redis_client,
            self._proxy,
            register_function_name=self.endpoint_name,
            redis_prefix=self.REDIS_PREFIX,
            group_name=self.GROUP_NAME,
            enable_enque_deferred_job=False,  # No deferred job for inferencer
            enable_reprocess_timeout_job=self.enable_retry,
            enable_dead_queue=False,
            process_timeout=self.process_timeout,
            delete_message_after_process=True,
            **kwargs,
        )
        self.daemon = Daemon(*[self._consumer_builder() for _ in range(concurrency)])

    async def _ping_or_exit(self, payload: dict, reenqueue: bool = True) -> None:
        async with httpx.AsyncClient() as client:
            try:
                await client.get(self.health_check_path, timeout=60)
            except httpx.HTTPError as ping_error:
                logger.error(f"(INTERNAL ERROR)Endpoint is not reachable: {ping_error}")
                # May container stopping, just re-enqueue and exit
                if reenqueue:
                    await self._producer.invoke(
                        endpoint_name=self.endpoint_name,
                        params=payload,
                    )
                    logger.info(f"Re-enqueue inference job: {payload} and exit")
                exit(1)

    async def _proxy(self, payload: dict) -> dict | None:
        inference_id = payload.get("inference_id") or payload.get("inferenceId")
        if not inference_id:
            return await self.return_no_inference_id_error(payload)
        await self._ping_or_exit(payload, reenqueue=True)
        async with httpx.AsyncClient() as client:
            logger.info(f"Invoke endpoint: {self.invoke_url}")
            try:
                response = await client.post(
                    self.invoke_url,
                    json=payload,
                    timeout=self.process_timeout,
                    follow_redirects=True,
                )
            except httpx.HTTPError as invoke_error:
                logger.error(f"(HTTP FAIL)Invoke endpoint failed: {invoke_error}")
                logger.exception(invoke_error)
                await self._callback(MatrixCallback.from_exception(inference_id, invoke_error))
                await self._ping_or_exit(
                    payload, reenqueue=False
                )  # If server is down, restart sidecar and do init again
                return None
            except Exception as internal_error:
                logger.error(f"(INTERNAL ERROR)Invoke endpoint failed: {internal_error}")
                logger.exception(internal_error)
                await self._callback(MatrixCallback.from_exception(inference_id, internal_error))
                return None
            else:
                logger.info(f"Invoke endpoint response status: {response.status_code}")
                await self._callback(MatrixCallback.from_response(inference_id, response))
                return response.json()

    async def return_no_inference_id_error(self, payload: dict) -> None:
        await self._callback(
            MatrixCallback(
                status=InferenceProxyStatus.INTERNAL_ERROR,
                msg=f"Missing 'inference_id' or 'inferenceId' in payload: {payload}",
            )
        )

    async def _callback(self, callback_params: MatrixCallback) -> None:
        async with httpx.AsyncClient(follow_redirects=True) as client:
            try:
                logger.info(f"Callback for finished inference: {self.callback_url}")
                await client.post(
                    self.callback_url,
                    data=callback_params.model_dump_json(),
                    timeout=60,
                )
            except Exception as e:
                logger.error(f"Callback failed: {e}")
                logger.exception(e)

    async def run_forever(self) -> None:
        try:
            async with async_timeout.timeout(self.health_check_timeout):
                logger.info(
                    f"Wait for invoke endpoint online...timeout: {self.health_check_timeout}s"
                )
                await self.wait_for_health_check()
        except TimeoutError:
            logger.error(f"Invoke endpoint not online in {self.health_check_timeout}s. Exit...")
            raise

        logger.info(f"Healthy check passed. Start consumer...")
        await self.daemon.run_forever()

    async def wait_for_health_check(self) -> None:
        async with httpx.AsyncClient() as client:
            while True:
                try:
                    await client.get(self.health_check_path, timeout=60)
                    logger.info(f"Ping {self.health_check_path} succeeded")
                    break
                except httpx.ConnectError as e:
                    logger.info(f"Ping {self.health_check_path} failed: {e}")
                    await asyncio.sleep(self.health_check_interval)

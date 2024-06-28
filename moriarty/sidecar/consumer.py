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

if sys.version_info >= (3, 11):
    import asyncio as async_timeout
else:
    import async_timeout


class InferencerConsumer:
    REDIS_PREFIX = "moriarty-brq"
    GROUP_NAME = "moriarty-inference-proxy"

    def __init__(
        self,
        redis_client: redis.Redis | redis.RedisCluster,
        endpoint_name: str,
        invoke_host: str = "localhost",
        invoke_port: int = "8080",
        invoke_path: str = "/invocations",
        health_check_url: str = "/ping",
        callbacl_url: str = "http://moriarty-operator:8999/api/callback",
        enable_retry: bool = False,
        enable_ssl: bool = False,
        concurrency: int = 1,
        process_timeout: int = 3600,
        healthy_check_timeout: int = 1200,
        healthy_check_interval: int = 5,
        **kwargs,
    ):
        self.redis_client = redis_client
        self.process_timeout = process_timeout
        self.healthy_check_timeout = healthy_check_timeout
        self.healthy_check_interval = healthy_check_interval

        scheme = "https" if enable_ssl else "http"
        proxy_url = f"{scheme}://{invoke_host}:{invoke_port}"
        self.invoke_url = urljoin(proxy_url, invoke_path)
        self.health_check_url = urljoin(proxy_url, health_check_url)
        self.callback_url = callbacl_url

        self._consumer_builder = partial(
            Consumer,
            self.redis_client,
            self._proxy,
            register_function_name=endpoint_name,
            redis_prefix=self.REDIS_PREFIX,
            group_name=self.GROUP_NAME,
            enable_enque_deferred_job=False,  # No deferred job for inferencer
            enable_reprocess_timeout_job=enable_retry,
            enable_dead_queue=False,
            process_timeout=self.process_timeout,
            delete_message_after_process=True,
            **kwargs,
        )
        self.daemon = Daemon(*[self._consumer_builder() for _ in range(concurrency)])

    async def _proxy(self, payload: dict) -> dict | None:
        inference_id = payload.get("inference_id") or payload.get("inferenceId")
        if not inference_id:
            return await self.return_no_inference_id_error(payload)
        async with httpx.AsyncClient() as client:
            logger.debug(f"Invoke endpoint: {self.invoke_url}")
            try:
                response = await client.post(
                    self.invoke_url,
                    json=payload,
                    timeout=self.process_timeout,
                    follow_redirects=True,
                )
            except httpx.ConnectError as e:
                logger.error(f"Invoke endpoint failed: {e}")
                logger.exception(e)
                await self._callback(MatrixCallback.from_exception(inference_id, e))
                return None
            else:
                logger.debug(f"Invoke endpoint response status: {response.status_code}")
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
        transport = httpx.HTTPTransport(
            retries=5,
        )
        async with httpx.AsyncClient(transport=transport, follow_redirects=True) as client:
            try:
                await client.post(self.callback_url, data=callback_params.model_dump_json())
            except httpx.ConnectError as e:
                logger.error(f"Callback failed: {e}")

    async def run_forever(self) -> None:
        try:
            async with async_timeout.timeout(self.healthy_check_timeout):
                logger.info(
                    f"Wait for invoke endpoint online...timeout: {self.healthy_check_timeout}s"
                )
                await self.wait_for_health_check()
        except TimeoutError:
            logger.error(f"Invoke endpoint not online in {self.healthy_check_timeout}s. Exit...")
            raise

        logger.info(f"Healthy check passed. Start consumer...")
        await self.daemon.run_forever()

    async def wait_for_health_check(self) -> None:
        async with httpx.AsyncClient() as client:
            while True:
                try:
                    await client.get(self.health_check_url, timeout=60)
                    logger.debug(f"Ping {self.health_check_url} succeeded")
                    break
                except httpx.ConnectError as e:
                    logger.debug(f"Ping {self.health_check_url} failed: {e}")
                    await asyncio.sleep(self.healthy_check_interval)

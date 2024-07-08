import os
import string
from typing import Optional

import escapism
from kubernetes_asyncio import client
from kubernetes_asyncio.client.api_client import ApiClient
from kubernetes_asyncio.client.models import V1DeploymentStatus

from moriarty.log import logger
from moriarty.matrix.operator_.orm import EndpointORM
from moriarty.matrix.operator_.spawner.plugin import (
    EndpointRuntimeInfo,
    Spawner,
    hookimpl,
)
from moriarty.matrix.operator_.tools import load_kube_config


def get_current_namespace() -> str | None:
    if not os.path.exists("/var/run/secrets/kubernetes.io/serviceaccount/namespace"):
        return None
    return open("/var/run/secrets/kubernetes.io/serviceaccount/namespace").read()


class DeploymentMixin:
    namespace: str

    def _escape_string(self, s: str) -> str:
        safe_chars = set(string.ascii_lowercase + string.digits)
        return escapism.escape(s, safe=safe_chars, escape_char="-")

    def get_deployment_name(self, endpoint_name: str) -> str:
        # Escape the endpoint name
        return self._escape_string(f"moriarty-{endpoint_name}")

    async def inspect_deployment_status(self, endpoint_name: str) -> V1DeploymentStatus | None:
        deployment_name = self.get_deployment_name(endpoint_name)

        async with ApiClient() as api:
            v1 = client.AppsV1Api(api)

            try:
                deployment = await v1.read_namespaced_deployment_status(
                    name=deployment_name, namespace=self.namespace
                )
            except client.rest.ApiException as e:
                if e.status == 404:
                    return None
                raise

            return deployment.status


class KubeSpawner(Spawner, DeploymentMixin):
    register_name = "kube"

    def __init__(
        self,
        namespace: Optional[str] = os.getenv("MORIARTY_KUBE_NAMESPACE_ENV"),
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.namespace = namespace or get_current_namespace() or "moriarty"

    async def prepare(self) -> None:
        await load_kube_config()

    async def count_avaliable_instances(self, endpoint_name: str) -> int:
        status = await self.inspect_deployment_status(endpoint_name)
        if status is None:
            logger.warning(f"Deployment not found: {endpoint_name}.")
            return 0

        return status.ready_replicas or 0

    async def create(self, endpoint_orm: EndpointORM) -> None:
        raise NotImplementedError

    async def update(self, endpoint_orm: EndpointORM, need_restart: bool = False) -> None:
        raise NotImplementedError

    async def delete(self, endpoint_name: str) -> None:
        raise NotImplementedError

    async def get_runtime_info(self, endpoint_name: str) -> EndpointRuntimeInfo:
        raise NotImplementedError

    async def scale(self, endpoint_name: str, target_replicas: int) -> None:
        raise NotImplementedError


@hookimpl
def register(manager):
    manager.register(KubeSpawner)

import datetime
import os
import string
from typing import Optional

import escapism
from kubernetes_asyncio import client
from kubernetes_asyncio.client.api_client import ApiClient

from moriarty.__init__ import __version__
from moriarty.log import logger
from moriarty.matrix.operator_.orm import EndpointORM
from moriarty.matrix.operator_.params import EndpointRuntimeInfo
from moriarty.matrix.operator_.spawner.plugin import (
    EnvironmentBuilder,
    Spawner,
    hookimpl,
)
from moriarty.matrix.operator_.tools import load_kube_config


def get_current_namespace() -> str | None:
    if not os.path.exists("/var/run/secrets/kubernetes.io/serviceaccount/namespace"):
        return None
    return open("/var/run/secrets/kubernetes.io/serviceaccount/namespace").read()


class DeploymentMixin(EnvironmentBuilder):
    namespace: str
    init_image = os.getenv("INIT_IMAGE", "peakcom/s5cmd")
    sidecar_image = os.getenv("SIDECAR_IMAGE", "wh1isper/moriarty-sidecar")
    image_pull_secrets = os.getenv("IMAGE_PULL_SECRETS")
    pull_policy = os.getenv("PULL_POLICY", "Always")
    S5_NUMWORKS = int(os.getenv("S5_NUMWORKERS", 3))
    S5_CONCURRENCY = int(os.getenv("S5_CONCURRENCY", 1))

    def _escape_string(self, s: str) -> str:
        safe_chars = set(string.ascii_lowercase + string.digits)
        return escapism.escape(s, safe=safe_chars, escape_char="-", allow_collisions=True).lower()

    def get_deployment_name(self, endpoint_name: str) -> str:
        # Escape the endpoint name
        return self._escape_string(f"endpoint-{endpoint_name}")

    async def inspect_deployment_status(
        self, endpoint_name: str
    ) -> client.V1DeploymentStatus | None:
        deployment_name = self.get_deployment_name(endpoint_name)
        logger.debug(f"Inspect deployment status: {deployment_name}(as endpoint {endpoint_name}).")

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

    async def make_and_apply_deployment(self, endpoint_orm: EndpointORM) -> None:
        deployment = self._make_deployment(endpoint_orm)
        try:
            await self._apply_deployment(deployment)
        except client.rest.ApiException as e:
            logger.error(
                f"Apply deployment failed: {e}. Input: {client.ApiClient().sanitize_for_serialization(deployment)}"
            )
            raise

    async def make_and_update_deployment(self, endpoint_orm: EndpointORM) -> None:
        deployment = self._make_deployment(endpoint_orm)
        try:
            await self._replace_deployment(deployment)
        except client.rest.ApiException as e:
            logger.error(
                f"Update deployment failed: {e}. Input: {client.ApiClient().sanitize_for_serialization(deployment)}"
            )
            raise

    async def delete_deployment(self, endpoint_name: str) -> None:
        deployment_name = self.get_deployment_name(endpoint_name)
        async with ApiClient() as api:
            v1 = client.AppsV1Api(api)
            try:
                await v1.delete_namespaced_deployment(
                    name=deployment_name, namespace=self.namespace
                )
            except client.rest.ApiException as e:
                if e.status == 404:
                    return
                raise

    async def restart_deployment(self, endpoint_name: str) -> None:
        now = datetime.datetime.now()
        now = str(now.isoformat("T") + "Z")
        body = {
            "spec": {
                "template": {
                    "metadata": {"annotations": {"kubectl.kubernetes.io/restartedAt": now}}
                }
            }
        }
        deployment = self.get_deployment_name(endpoint_name)
        namespace = self.namespace

        async with ApiClient() as api:
            v1_apps = client.AppsV1Api(api)
            await v1_apps.patch_namespaced_deployment(deployment, namespace, body, pretty="true")

    async def scale_deployment(self, endpoint_name: str, target_replicas: int) -> None:
        deployment = self.get_deployment_name(endpoint_name)
        namespace = self.namespace
        async with ApiClient() as api:
            v1 = client.AppsV1Api(api)
            await v1.patch_namespaced_deployment_scale(
                name=deployment,
                namespace=namespace,
                body={"spec": {"replicas": target_replicas}},
            )

    async def _apply_deployment(self, deployment: client.V1Deployment) -> None:
        async with ApiClient() as api:
            v1 = client.AppsV1Api(api)
            await v1.create_namespaced_deployment(namespace=self.namespace, body=deployment)

    async def _replace_deployment(self, deployment: client.V1Deployment) -> None:
        async with ApiClient() as api:
            v1 = client.AppsV1Api(api)
            await v1.replace_namespaced_deployment(
                name=deployment.metadata.name, namespace=self.namespace, body=deployment
            )

    def _make_deployment(
        self,
        endpoint_orm: EndpointORM,
    ) -> client.V1Deployment:
        deployment_name = self.get_deployment_name(endpoint_orm.endpoint_name)

        deployment = client.V1Deployment()

        deployment.api_version = "apps/v1"
        deployment.kind = "Deployment"

        deployment.metadata = client.V1ObjectMeta()
        deployment.metadata.name = deployment_name
        deployment.metadata.namespace = self.namespace
        deployment.metadata.labels = {
            "moriarty.version": __version__,
            "moriarty.kubespawner.version": __version__,
            "moriarty.kubespawner.gpu_nums": str(endpoint_orm.gpu_nums or 0),
            "moriarty.kubespawner.gpu_type": str(endpoint_orm.gpu_type or "").replace("/", "-"),
        }

        if endpoint_orm.model_path and endpoint_orm.model_path.startswith("host://"):
            host_path = endpoint_orm.model_path.replace("host:/", "")
            model_volumn = client.V1Volume(
                name="modeldir",
                host_path=client.V1HostPathVolumeSource(
                    path=host_path,
                    type="DirectoryOrCreate",
                ),
            )
        else:
            model_volumn = client.V1Volume(
                name="modeldir",
                empty_dir=client.V1EmptyDirVolumeSource(),
            )

        deployment.spec = client.V1DeploymentSpec(
            replicas=endpoint_orm.replicas,
            selector=client.V1LabelSelector(
                match_labels={
                    "app": "moriarty",
                    "moriarty.endpoint.deployment": deployment_name,
                }
            ),
            strategy=client.V1DeploymentStrategy(
                type="RollingUpdate",
                rolling_update=client.V1RollingUpdateDeployment(
                    max_unavailable=0,
                    max_surge=1,
                ),
            ),
            template=client.V1PodTemplateSpec(
                metadata=client.V1ObjectMeta(
                    labels={
                        "app": "moriarty",
                        "moriarty.endpoint.deployment": deployment_name,
                    },
                ),
                spec=client.V1PodSpec(
                    restart_policy="Always",
                    affinity=self._make_affinity(endpoint_orm),
                    termination_grace_period_seconds=endpoint_orm.process_timeout,
                    node_selector=(
                        client.V1NodeSelector(
                            match_labels={k: v for k, v in endpoint_orm.node_labels.items() if v}
                        )
                        if endpoint_orm.node_labels
                        else None
                    ),
                    init_containers=[self._make_init_container(endpoint_orm)],
                    containers=[
                        self._make_sidecar_container(endpoint_orm),
                        self._make_compute_contaienr(endpoint_orm),
                    ],
                    volumes=[
                        model_volumn,
                    ],
                    image_pull_secrets=(
                        [
                            client.V1LocalObjectReference(
                                name=self.image_pull_secrets,
                            )
                        ]
                        if self.image_pull_secrets
                        else None
                    ),
                ),
            ),
        )

        return deployment

    def _make_affinity(self, endpoint_orm: EndpointORM) -> client.V1Affinity | None:
        if not endpoint_orm.node_affinity:
            return None
        return client.V1Affinity(
            node_affinity=client.V1NodeAffinity(
                required_during_scheduling_ignored_during_execution=client.V1NodeSelector(
                    node_selector_terms=[
                        client.V1NodeSelectorTerm(
                            match_expressions=[
                                {
                                    "key": k,
                                    "operator": "In",
                                    "values": v,
                                }
                                for k, v in endpoint_orm.node_affinity.items()
                            ]
                        )
                    ]
                )
            )
        )

    def _make_init_container(
        self,
        endpoint_orm: EndpointORM,
    ) -> client.V1Container:
        # Use init container to /opt/ml/model
        command = [
            "mkdir",
            "-p",
            "/opt/ml/model",
        ]

        if endpoint_orm.model_path and endpoint_orm.model_path.startswith("s3://"):
            logger.debug(f"Using s5cmd to copy model from {endpoint_orm.model_path}.")
            command.extend(
                [
                    "&&",
                    "/s5cmd",
                    "--numworkers",
                    f"{self.S5_NUMWORKS}",
                    "cp",
                    "--concurrency",
                    f"{self.S5_CONCURRENCY}",
                    (
                        f"{endpoint_orm.model_path}*"
                        if endpoint_orm.model_path.endswith("/")
                        else endpoint_orm.model_path
                    ),
                    "/opt/ml/model/",
                ]
            )

        # Make sure the model dir is read-only
        command.extend(
            [
                "&&",
                "chmod",
                "-R",
                "444",
                "/opt/ml/model",
            ]
        )
        return client.V1Container(
            name="init",
            image=self.init_image,
            command=["/bin/sh"],
            args=["-c", " ".join(command)],
            volume_mounts=[
                client.V1VolumeMount(
                    name="modeldir",
                    mount_path="/opt/ml/model",
                ),
            ],
            env=[
                client.V1EnvVar(name=k, value=v) for k, v in self.build_init_environment().items()
            ],
        )

    def _make_sidecar_container(
        self,
        endpoint_orm: EndpointORM,
    ) -> client.V1Container:
        return client.V1Container(
            name="sidecar",
            image=self.sidecar_image,
            resources=client.V1ResourceRequirements(
                requests={
                    "cpu": "100m",
                    "memory": "256Mi",
                },
                limits={
                    "cpu": "100m",
                    "memory": "256Mi",
                },
            ),
            env=[
                client.V1EnvVar(name=k, value=v)
                for k, v in self.build_sidecar_environment(endpoint_orm).items()
            ],
            image_pull_policy=self.pull_policy,
        )

    def _make_compute_contaienr(
        self,
        endpoint_orm: EndpointORM,
    ) -> client.V1Container:
        cpu_limit = {}
        memory_limit = {}
        gpu_limit = {}
        if endpoint_orm.gpu_nums:
            gpu_limit = {
                endpoint_orm.gpu_type: f"{endpoint_orm.gpu_nums}",
            }
        if endpoint_orm.cpu_limit:
            cpu_limit = {
                "cpu": f"{endpoint_orm.cpu_limit}",
            }
        if endpoint_orm.memory_limit:
            memory_limit = {
                "memory": f"{endpoint_orm.memory_limit}Mi",
            }

        return client.V1Container(
            name="compute",
            image=endpoint_orm.image,
            command=endpoint_orm.commands,
            args=endpoint_orm.args,
            resources=client.V1ResourceRequirements(
                requests={
                    "cpu": f"{endpoint_orm.cpu_request}",
                    "memory": f"{endpoint_orm.memory_request}Mi",
                    **gpu_limit,
                },
                limits={
                    **cpu_limit,
                    **memory_limit,
                    **gpu_limit,
                },
            ),
            env=[
                client.V1EnvVar(name=k, value=v)
                for k, v in self.build_compute_environment(endpoint_orm).items()
            ],
            env_from=[
                client.V1EnvFromSource(secret_ref=client.V1SecretEnvSource(name=secret_ref))
                for secret_ref in endpoint_orm.environment_variables_secret_refs
            ],
            ports=[
                client.V1ContainerPort(
                    name="http",
                    container_port=endpoint_orm.invoke_port,
                ),
            ],
            readiness_probe=client.V1Probe(
                http_get=client.V1HTTPGetAction(
                    path=endpoint_orm.health_check_path,
                    port=endpoint_orm.invoke_port,
                ),
                initial_delay_seconds=endpoint_orm.health_check_interval,
                period_seconds=endpoint_orm.health_check_interval,
                timeout_seconds=endpoint_orm.health_check_timeout,
                failure_threshold=10,
                success_threshold=1,
            ),
            volume_mounts=[
                client.V1VolumeMount(
                    name="modeldir",
                    mount_path="/opt/ml/model",
                ),
            ],
            image_pull_policy=self.pull_policy,
        )


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
        logger.info(f"Create deployment: {endpoint_orm.endpoint_name}.")
        await self.make_and_apply_deployment(endpoint_orm)

    async def update(self, endpoint_orm: EndpointORM, need_restart: bool = True) -> None:
        logger.info(f"Update deployment: {endpoint_orm.endpoint_name}.")
        await self.make_and_update_deployment(endpoint_orm)
        if need_restart:
            logger.info(f"Restart deployment: {endpoint_orm.endpoint_name}.")
            await self.restart_deployment(endpoint_orm.endpoint_name)

    async def delete(self, endpoint_name: str) -> None:
        logger.info(f"Delete deployment: {endpoint_name}.")
        await self.delete_deployment(endpoint_name)

    async def get_runtime_info(self, endpoint_name: str) -> EndpointRuntimeInfo:
        status = await self.inspect_deployment_status(endpoint_name)
        if not status:
            logger.warning(f"Deployment not found: {endpoint_name}.")
            return EndpointRuntimeInfo(
                total_replicas_nums=0,
                updated_replicas_nums=0,
                avaliable_replicas_nums=0,
                unavailable_replicas_nums=0,
            )

        return EndpointRuntimeInfo(
            total_replicas_nums=status.replicas or 0,
            updated_replicas_nums=status.updated_replicas or 0,
            avaliable_replicas_nums=status.ready_replicas or 0,
            unavailable_replicas_nums=status.unavailable_replicas or 0,
        )

    async def scale(self, endpoint_name: str, target_replicas: int) -> None:
        logger.info(f"Scale deployment: {endpoint_name}.")
        await self.scale_deployment(endpoint_name, target_replicas)


@hookimpl
def register(manager):
    manager.register(KubeSpawner)

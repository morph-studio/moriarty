from __future__ import annotations

from typing import TYPE_CHECKING

import pluggy

from moriarty.envs import *

if TYPE_CHECKING:
    from moriarty.matrix.operator_.orm import EndpointORM
    from moriarty.matrix.operator_.params import EndpointRuntimeInfo


project_name = "moriarty.matrix.spawner"
"""
The entry-point name of this extension.

Should be used in ``pyproject.toml`` as ``[project.entry-points."{project_name}"]``
"""
hookimpl = pluggy.HookimplMarker(project_name)
"""
Hookimpl marker for this extension, extension module should use this marker

Example:

    .. code-block:: python

        @hookimpl
        def register(manager):
            ...
"""

hookspec = pluggy.HookspecMarker(project_name)


@hookspec
def register(manager):
    """
    For more information about this function, please check the :ref:`manager`

    We provided an example package for you in ``{project_root}/example/extension/custom-spawner``.

    Example:

    .. code-block:: python

        class CustomSpawner(Spawner):
            register_name = "example"

        from moriarty.matrix.operator_.spawner.plugin import hookimpl

        @hookimpl
        def register(manager):
            manager.register(CustomSpawner)


    Config ``project.entry-points`` so that we can find it

    .. code-block:: toml

        [project.entry-points."moriarty.matrix.spawner"]
        {whatever-name} = "{package}.{path}.{to}.{file-with-hookimpl-function}"


    You can verify it by `moriarty-matrix list-plugins`.
    """


class EnvironmentBuilder:
    model_s3_access_key_id = os.getenv(MODEL_S3_ACCESS_KEY_ID_ENV) or os.getenv("AWS_ACCESS_KEY_ID")
    model_s3_secret_access_key = os.getenv(MODEL_S3_SECRET_ACCESS_KEY_ENV) or os.getenv(
        "AWS_SECRET_ACCESS_KEY"
    )
    model_s3_endpoint_url = os.getenv(MODEL_S3_ENDPOINT_URL_ENV) or os.getenv("AWS_S3_ENDPOINT_URL")
    model_aws_region_name = os.getenv(MODEL_AWS_REGION_ENV) or os.getenv("AWS_REGION_NAME")

    def build_sidecar_environment(self, endpoint_orm: "EndpointORM") -> dict[str, str]:
        return {
            **{
                k: str(v)
                for k, v in {
                    # For brq
                    REDIS_URL_ENV: os.getenv(REDIS_URL_ENV),
                    REDIS_HOST_ENV: os.getenv(REDIS_HOST_ENV),
                    REDIS_PORT_ENV: os.getenv(REDIS_PORT_ENV),
                    REDIS_DB_ENV: os.getenv(REDIS_DB_ENV),
                    REDIS_CLUSTER_ENV: os.getenv(REDIS_CLUSTER_ENV),
                    REDIS_TLS_ENV: os.getenv(REDIS_TLS_ENV),
                    REDIS_USERNAME_ENV: os.getenv(REDIS_USERNAME_ENV),
                    REDIS_PASSWORD_ENV: os.getenv(REDIS_PASSWORD_ENV),
                    # For sidecar
                    ENDPOINT_NAME_ENV: endpoint_orm.endpoint_name,
                    INVOKE_HOST_ENV: "localhost",
                    INVOKE_PORT_ENV: endpoint_orm.invoke_port,
                    INVOKE_PATH_ENV: endpoint_orm.invoke_path,
                    HEALTH_CHECK_PATH_ENV: endpoint_orm.health_check_path,
                    ENABLE_RETRY_ENV: endpoint_orm.allow_retry,
                    CONCURRENCY_ENV: endpoint_orm.concurrency,
                    PROCESS_TIMEOUT_ENV: endpoint_orm.process_timeout,
                    HEALTH_CHECK_TIMEOUT_ENV: endpoint_orm.health_check_timeout,
                    HEALTH_CHECK_INTERVAL_ENV: endpoint_orm.health_check_interval,
                }.items()
                if v is not None
            },
        }

    def build_compute_environment(self, endpoint_orm: "EndpointORM") -> dict[str, str]:
        return {
            **{str(k): str(v) for k, v in endpoint_orm.environment_variables.items()},
        }

    def build_init_environment(self) -> dict[str, str]:
        return {
            **{
                k: str(v)
                for k, v in {
                    "AWS_ACCESS_KEY_ID": self.model_s3_access_key_id,
                    "AWS_SECRET_ACCESS_KEY": self.model_s3_secret_access_key,
                    "AWS_S3_ENDPOINT_URL": self.model_s3_endpoint_url,
                    "AWS_REGION_NAME": self.model_aws_region_name,
                    "S3_ENDPOINT_URL": self.model_s3_endpoint_url,
                }.items()
                if v is not None
            },
        }


class Spawner:
    register_name: str

    async def prepare(self) -> None:
        pass

    async def count_avaliable_instances(self, endpoint_name: str) -> int:
        raise NotImplementedError

    async def create(self, endpoint_orm: "EndpointORM") -> None:
        raise NotImplementedError

    async def update(self, endpoint_orm: "EndpointORM", need_restart: bool = True) -> None:
        raise NotImplementedError

    async def delete(self, endpoint_name: str) -> None:
        raise NotImplementedError

    async def get_runtime_info(self, endpoint_name: str) -> "EndpointRuntimeInfo":
        raise NotImplementedError

    async def scale(self, endpoint_name: str, target_replicas: int) -> None:
        raise NotImplementedError

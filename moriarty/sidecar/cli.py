import click
from brq.tools import get_redis_client, get_redis_url

from moriarty.envs import *
from moriarty.sidecar.consumer import InferencerConsumer
from moriarty.tools import coro


@click.command()
@click.option(
    "--redis-host",
    default="localhost",
    help="Redis host",
    type=str,
    envvar=REDIS_HOST_ENV,
)
@click.option(
    "--redis-port",
    default=6379,
    help="Redis port",
    type=int,
    envvar=REDIS_PORT_ENV,
)
@click.option(
    "--redis-db",
    default=0,
    help="Redis db",
    type=int,
    envvar=REDIS_DB_ENV,
)
@click.option(
    "--redis-cluster",
    default=False,
    help="Redis cluster",
    type=bool,
    envvar=REDIS_CLUSTER_ENV,
)
@click.option(
    "--redis-tls",
    default=False,
    help="Redis TLS",
    type=bool,
    envvar=REDIS_TLS_ENV,
)
@click.option(
    "--redis-username",
    default="",
    help="Redis username",
    type=str,
    envvar=REDIS_USERNAME_ENV,
)
@click.option(
    "--redis-password",
    default="",
    help="Redis password",
    type=str,
    envvar=REDIS_PASSWORD_ENV,
)
@click.option("--redis-url", required=False, help="Redis URL", type=str, envvar=REDIS_URL_ENV)
@click.option(
    "--endpoint-name",
    required=True,
    help="Endpoint name",
    type=str,
    envvar=ENDPOINT_NAME_ENV,
)
@click.option(
    "--invoke-host",
    required=False,
    help="Invoke host",
    type=str,
    envvar=INVOKE_HOST_ENV,
)
@click.option(
    "--invoke-port",
    required=False,
    help="Invoke port",
    type=int,
    envvar=INVOKE_PORT_ENV,
)
@click.option(
    "--invoke-path",
    required=False,
    help="Invoke path",
    type=str,
    envvar=INVOKE_PATH_ENV,
)
@click.option(
    "--health-check-path",
    required=False,
    help="Health check url",
    type=str,
    envvar=HEALTH_CHECK_PATH_ENV,
)
@click.option(
    "--enable-retry",
    required=False,
    help="Enable retry",
    type=bool,
    envvar=ENABLE_RETRY_ENV,
)
@click.option(
    "--enable-ssl",
    required=False,
    help="Enable SSL",
    type=bool,
    envvar=ENABLE_SSL_ENV,
)
@click.option(
    "--concurrency",
    required=False,
    help="Concurrency",
    type=int,
    envvar=CONCURRENCY_ENV,
)
@click.option(
    "--process-timeout",
    required=False,
    help="Process timeout",
    type=int,
    envvar=PROCESS_TIMEOUT_ENV,
)
@click.option(
    "--health-check-timeout",
    required=False,
    help="Health check timeout",
    type=int,
    envvar=HEALTH_CHECK_TIMEOUT_ENV,
)
@click.option(
    "--health-check-interval",
    required=False,
    help="Health check interval",
    type=int,
    envvar=HEALTH_CHECK_INTERVAL_ENV,
)
@coro
async def start(
    redis_host,
    redis_port,
    redis_db,
    redis_cluster,
    redis_tls,
    redis_username,
    redis_password,
    redis_url,
    endpoint_name,
    invoke_host,
    invoke_port,
    invoke_path,
    health_check_path,
    enable_retry,
    enable_ssl,
    concurrency,
    process_timeout,
    health_check_timeout,
    health_check_interval,
):
    """
    Start moriarty sidecar and run forever
    """
    redis_url = redis_url or get_redis_url(
        host=redis_host,
        port=redis_port,
        db=redis_db,
        cluster=redis_cluster,
        tls=redis_tls,
        username=redis_username,
        password=redis_password,
    )
    kwargs = {
        k: v
        for k, v in {
            "invoke_host": invoke_host,
            "invoke_port": invoke_port,
            "invoke_path": invoke_path,
            "health_check_path": health_check_path,
            "enable_retry": enable_retry,
            "enable_ssl": enable_ssl,
            "concurrency": concurrency,
            "process_timeout": process_timeout,
            "health_check_timeout": health_check_timeout,
            "health_check_interval": health_check_interval,
        }.items()
        if v is not None
    }

    async with get_redis_client(redis_url, is_cluster=redis_cluster) as redis_client:
        consumer = InferencerConsumer(
            redis_client=redis_client, endpoint_name=endpoint_name, **kwargs
        )
        await consumer.run_forever()


@click.group()
def cli():
    pass


cli.add_command(start)

import click

from moriarty.deploy.config import ConfigLoader
from moriarty.deploy.endpoint import Endpoint
from moriarty.envs import MORIARTY_MATRIX_API_URL_ENV, MORIARTY_MATRIX_TOKEN_ENV
from moriarty.log import logger
from moriarty.matrix.operator_.enums_ import MetricType
from moriarty.matrix.operator_.params import CreateEndpointParams, UpdateEndpointParams

from .sdk import (
    request_create_endpoint_with_params,
    request_delete_autoscale,
    request_delete_endpoint,
    request_exist_endpoint,
    request_query_autoscale,
    request_query_endpoint,
    request_scan_autoscale_log,
    request_scan_endpoint,
    request_set_autoscale,
    request_update_endpoint_with_params,
)


@click.command()
@click.option(
    "--limit",
    default=10,
    help="Limit number of results",
    type=int,
)
@click.option(
    "--cursor",
    default=None,
    help="Cursor",
    type=str,
)
@click.option(
    "--keyword",
    default=None,
    help="Keyword",
    type=str,
)
@click.option(
    "--order_by",
    default="created_at",
    help="Order by",
    type=str,
)
@click.option(
    "--order",
    default="desc",
    help="Order",
    type=str,
)
def scan(
    limit,
    cursor,
    keyword,
    order_by,
    order,
):
    config = ConfigLoader()
    api_url = config.get_api_url()
    if not api_url:
        print(
            f"Please config {MORIARTY_MATRIX_API_URL_ENV} and {MORIARTY_MATRIX_TOKEN_ENV} for API URL and token, or edit {config.config_path}"
        )

    token = config.get_api_token()

    cursor = cursor
    while True:
        response = request_scan_endpoint(
            api_url=api_url,
            limit=limit,
            cursor=cursor,
            keyword=keyword,
            order_by=order_by,
            order=order,
            token=token,
        )

        print(
            f"Count: {len(response.endpoints)} | Total: {response.total} [Cursor: {response.cursor}]"
        )
        cursor = response.cursor

        for endpoint in response.endpoints:
            autoscale = request_query_autoscale(
                endpoint_name=endpoint.endpoint_name, api_url=api_url, token=token
            )
            print(f"Endpoint: {endpoint} | Autoscale: {autoscale}")

        if not response.endpoints or len(response.endpoints) < limit:
            print("\nNo more results...")
            return
        input("Press Enter to continue...")


@click.command()
@click.argument("endpoint_name")
@click.option(
    "--api-url",
    help="Moriarty Operator API URL",
    type=str,
    envvar=MORIARTY_MATRIX_API_URL_ENV,
    required=True,
)
@click.option(
    "--token",
    help="Moriarty Operator API token",
    type=str,
    envvar=MORIARTY_MATRIX_TOKEN_ENV,
)
def query(
    endpoint_name,
    api_url,
    token,
):
    response = request_query_endpoint(
        endpoint_name=endpoint_name,
        api_url=api_url,
        token=token,
    )
    print(response)

    autoscale = request_query_autoscale(endpoint_name=endpoint_name, api_url=api_url, token=token)
    if autoscale:
        print(autoscale)
    else:
        print(f"No autoscale found for `{endpoint_name}`")


@click.command()
@click.option(
    "--endpoint-config-file",
    help="Endpoint config file",
    type=str,
    required=True,
)
@click.option(
    "--api-url",
    help="Moriarty Operator API URL",
    type=str,
    envvar=MORIARTY_MATRIX_API_URL_ENV,
    required=True,
)
@click.option(
    "--token",
    help="Moriarty Operator API token",
    type=str,
    envvar=MORIARTY_MATRIX_TOKEN_ENV,
)
@click.option(
    "--restart",
    default=False,
    help="Restart endpoint",
    type=bool,
    is_flag=True,
    required=False,
)
def deploy_or_update(
    api_url,
    endpoint_config_file,
    token,
    restart,
):
    with open(endpoint_config_file) as f:
        endpoint: Endpoint = Endpoint.model_validate_json(
            f.read(),
        )

    endpoint_name = endpoint.endpoint_name
    if request_exist_endpoint(
        endpoint_name=endpoint_name,
        api_url=api_url,
        token=token,
    ):
        logger.info(f"Endpoint: {endpoint_name} exists, update params will be applied")
        update_params = UpdateEndpointParams.model_validate(endpoint)
        update_params.need_restart = restart

        if request_query_autoscale(endpoint_name=endpoint_name, api_url=api_url, token=token):
            logger.info(f"Endpoint: {endpoint_name} has autoscale, update params will be ignored")
            update_params.replicas = None

        request_update_endpoint_with_params(
            endpoint_name=endpoint_name,
            api_url=api_url,
            params=update_params,
            token=token,
        )
        print(f"Updated endpoint: {endpoint_name}")
    else:
        request_create_endpoint_with_params(
            api_url=api_url,
            params=CreateEndpointParams.model_validate(endpoint),
            token=token,
        )
        print(f"Created endpoint: {endpoint_name}")

    if endpoint.config_autoscale:
        request_set_autoscale(
            endpoint_name=endpoint_name,
            min_replicas=endpoint.min_replicas,
            max_replicas=endpoint.max_replicas,
            scale_in_cooldown=endpoint.scale_in_cooldown,
            scale_out_cooldown=endpoint.scale_out_cooldown,
            metrics=endpoint.metrics,
            metrics_threshold=endpoint.metrics_threshold,
            api_url=api_url,
            token=token,
        )
        print(f"Set autoscale for endpoint: {endpoint_name}")
    else:
        request_delete_autoscale(endpoint_name=endpoint_name, api_url=api_url, token=token)
        print(f"No autoscale for endpoint: {endpoint_name}")


@click.command()
@click.argument("endpoint_name")
@click.option(
    "--api-url",
    help="Moriarty Operator API URL",
    type=str,
    envvar=MORIARTY_MATRIX_API_URL_ENV,
    required=True,
)
@click.option(
    "--token",
    help="Moriarty Operator API token",
    type=str,
    envvar=MORIARTY_MATRIX_TOKEN_ENV,
)
def delete(
    endpoint_name,
    api_url,
    token,
):
    request_delete_endpoint(endpoint_name=endpoint_name, api_url=api_url, token=token)
    print(f"Endpoint `{endpoint_name}` deleted")


@click.command()
@click.argument("endpoint_name")
@click.option(
    "--min-replicas",
    required=True,
    help="Minimum number of replicas",
    type=int,
)
@click.option(
    "--max-replicas",
    required=True,
    help="Minimum number of replicas",
    type=int,
)
@click.option(
    "--scale-in-cooldown",
    required=False,
    help="Scale in cooldown",
    type=int,
)
@click.option(
    "--scale-out-cooldown",
    required=False,
    help="Scale out cooldown",
    type=int,
)
@click.option(
    "--metrics",
    required=True,
    default=MetricType.pending_jobs_per_instance.value,
    help="Metrics",
    type=click.Choice(MetricType.__members__.keys()),
)
@click.option(
    "--metrics-threshold",
    required=True,
    help="Metrics threshold",
    type=int,
)
@click.option(
    "--api-url",
    help="Moriarty Operator API URL",
    type=str,
    envvar=MORIARTY_MATRIX_API_URL_ENV,
    required=True,
)
@click.option(
    "--token",
    help="Moriarty Operator API token",
    type=str,
    envvar=MORIARTY_MATRIX_TOKEN_ENV,
)
def autoscale(
    endpoint_name,
    min_replicas,
    max_replicas,
    scale_in_cooldown,
    scale_out_cooldown,
    metrics,
    metrics_threshold,
    api_url,
    token,
):
    autoscale = request_set_autoscale(
        endpoint_name=endpoint_name,
        min_replicas=min_replicas,
        max_replicas=max_replicas,
        scale_in_cooldown=scale_in_cooldown,
        scale_out_cooldown=scale_out_cooldown,
        metrics=metrics,
        metrics_threshold=metrics_threshold,
        api_url=api_url,
        token=token,
    )
    print(autoscale)


@click.command()
@click.argument("endpoint_name")
@click.option(
    "--api-url",
    help="Moriarty Operator API URL",
    type=str,
    envvar=MORIARTY_MATRIX_API_URL_ENV,
    required=True,
)
@click.option(
    "--token",
    help="Moriarty Operator API token",
    type=str,
    envvar=MORIARTY_MATRIX_TOKEN_ENV,
)
def delete_autoscale(
    endpoint_name,
    api_url,
    token,
):
    request_query_endpoint(
        endpoint_name=endpoint_name,
        api_url=api_url,
        token=token,
    )

    request_delete_autoscale(endpoint_name=endpoint_name, api_url=api_url, token=token)
    print(f"Autoscale for `{endpoint_name}` deleted")
    response = request_query_endpoint(
        endpoint_name=endpoint_name,
        api_url=api_url,
        token=token,
    )
    print(response)


@click.command()
@click.argument("endpoint_name")
@click.option(
    "--limit",
    default=10,
    help="Limit number of results",
    type=int,
)
@click.option(
    "--cursor",
    default=None,
    help="Cursor",
    type=int,
)
@click.option(
    "--keyword",
    default=None,
    help="Keyword",
    type=str,
)
@click.option(
    "--order_by",
    default="created_at",
    help="Order by",
    type=str,
)
@click.option(
    "--order",
    default="desc",
    help="Order",
    type=str,
)
@click.option(
    "--api-url",
    help="Moriarty Operator API URL",
    type=str,
    envvar=MORIARTY_MATRIX_API_URL_ENV,
    required=True,
)
@click.option(
    "--token",
    help="Moriarty Operator API token",
    type=str,
    envvar=MORIARTY_MATRIX_TOKEN_ENV,
)
def query_autoscale_log(
    endpoint_name,
    api_url,
    token,
    limit,
    cursor,
    keyword,
    order_by,
    order,
):
    cursor = cursor
    while True:
        response = request_scan_autoscale_log(
            api_url=api_url,
            endpoint_name=endpoint_name,
            limit=limit,
            cursor=cursor,
            keyword=keyword,
            order_by=order_by,
            order=order,
            token=token,
        )

        print(f"Count: {len(response.logs)} | Total: {response.total} [Cursor: {response.cursor}]")
        cursor = response.cursor

        for log in response.logs:
            print(log)

        if not response.logs or len(response.logs) < limit:
            print("\nNo more results...")
            return
        input("Press Enter to continue...")


@click.group()
def cli():
    pass


cli.add_command(scan)
cli.add_command(query)
cli.add_command(deploy_or_update)
cli.add_command(delete)
cli.add_command(autoscale)
cli.add_command(delete_autoscale)
cli.add_command(query_autoscale_log)

import click

from moriarty.envs import MORIARTY_MATRIX_API_URL_ENV, MORIARTY_MATRIX_TOKEN_ENV
from moriarty.matrix.operator_.enums_ import MetricType
from moriarty.matrix.operator_.params import CreateEndpointParams, UpdateEndpointParams

from .sdk import (
    request_create_endpoint_with_params,
    request_delete_autoscale,
    request_delete_endpoint,
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
def scan(
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

        if not response.endpoints:
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
        print(f"No autoscale found for {endpoint_name}")


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
def deploy(
    api_url,
    endpoint_config_file,
    token,
):
    with open(endpoint_config_file) as f:
        params: CreateEndpointParams = CreateEndpointParams.model_validate_json(
            f.read(),
        )

    request_create_endpoint_with_params(api_url=api_url, params=params, token=token)
    print(f"Created endpoint: {params.endpoint_name}")


@click.command()
@click.argument("endpoint_name")
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
def update(
    api_url,
    endpoint_name,
    endpoint_config_file,
    token,
):
    with open(endpoint_config_file) as f:
        params: UpdateEndpointParams = UpdateEndpointParams.model_validate_json(
            f.read(),
        )

    autoscale = request_query_autoscale(endpoint_name=endpoint_name, api_url=api_url, token=token)
    if autoscale:
        print(f"{endpoint_name} has autoscale: {autoscale}, set replicas to None")
        params.replicas = None

    response = request_update_endpoint_with_params(
        api_url=api_url,
        endpoint_name=endpoint_name,
        params=params,
        token=token,
    )
    print(f"Updated endpoint: {endpoint_name}")
    print(response)


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
    print(f"Endpoint {endpoint_name} deleted")


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
    required=True,
    help="Scale in cooldown",
    type=int,
)
@click.option(
    "--scale-out-cooldown",
    required=True,
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
    request_set_autoscale(
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

    autoscale = request_query_autoscale(endpoint_name=endpoint_name, api_url=api_url, token=token)
    if autoscale:
        print("Autoscale set successfully")
        print(autoscale)
    else:
        print(f"No autoscale found for {endpoint_name}")


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
    print(f"Autoscale for {endpoint_name} deleted")
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

        if not response.logs:
            print("\nNo more results...")
            return
        input("Press Enter to continue...")


@click.group()
def cli():
    pass


cli.add_command(scan)
cli.add_command(query)
cli.add_command(deploy)
cli.add_command(update)
cli.add_command(delete)
cli.add_command(autoscale)
cli.add_command(delete_autoscale)
cli.add_command(query_autoscale_log)

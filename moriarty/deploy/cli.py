import click

from moriarty.envs import MORIARTY_MATRIX_API_URL_ENV, MORIARTY_MATRIX_TOKEN_ENV

from .sdk import delete_endpoint, query_endpoint, scan_endpoint


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
        response = scan_endpoint(
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
            print(endpoint)

        if not response.endpoints:
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
    response = query_endpoint(
        endpoint_name=endpoint_name,
        api_url=api_url,
        token=token,
    )
    print(response)


@click.command()
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
    token,
):
    pass


@click.command()
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
    token,
):
    pass


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
    delete_endpoint(endpoint_name=endpoint_name, api_url=api_url, token=token)


@click.command()
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
    api_url,
    token,
):
    pass


@click.command()
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
    api_url,
    token,
):
    pass


@click.command()
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
def query_log(
    api_url,
    token,
):
    pass


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
cli.add_command(query_log)

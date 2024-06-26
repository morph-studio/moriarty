from moriarty.matrix.connector.params import InvokeParams
from moriarty.matrix.connector.sdk import invoke


def test_hello(client):
    response = client.get("/")
    assert response.status_code == 200


def test_invoke(client):
    response = client.post(
        "invoke",
        data=InvokeParams(
            inference_id="test",
            endpoint_name="hello",
            invoke_params={"name": "World"},
            priority=9,
        ).model_dump_json(),
    )
    assert response.status_code == 200
    assert response.json() == {"inference_id": "test"}


async def test_sdk_invoke(async_client):
    response = await invoke(
        endpoint_name="hello",
        inference_id="test",
        invoke_params={"name": "World"},
        async_client=async_client,
    )
    assert response.status_code == 200
    assert response.json() == {"inference_id": "test"}

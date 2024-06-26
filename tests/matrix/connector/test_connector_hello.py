from moriarty.matrix.connector.params import InvokeParams


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

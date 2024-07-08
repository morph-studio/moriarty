from moriarty.matrix.operator_.params import CreateEndpointParams, UpdateEndpointParams


def test_hello(client):
    response = client.get("/")
    assert response.status_code == 200


def test_manage_endpoints(client):
    endpoint_name = "hello"

    response = client.get("/endpoint/list")
    assert response.status_code == 200
    assert len(response.json()["endpoints"]) == 0

    response = client.get(f"/endpoint/{endpoint_name}/info")
    assert response.status_code == 404

    response = client.post(
        "/endpoint/create",
        data=CreateEndpointParams(endpoint_name=endpoint_name, image="busybox").model_dump_json(),
    )
    assert response.status_code == 201

    response = client.post(
        "/endpoint/create",
        data=CreateEndpointParams(endpoint_name=endpoint_name, image="busybox").model_dump_json(),
    )
    assert response.status_code == 409

    response = client.get(f"/endpoint/{endpoint_name}/info")
    assert response.status_code == 200

    response = client.get("/endpoint/list")
    assert response.status_code == 200
    assert len(response.json()["endpoints"]) == 1

    response = client.post(
        f"/endpoint/{endpoint_name}/update",
        data=UpdateEndpointParams(image="busybox", queue_capacity=10).model_dump_json(),
    )
    assert response.status_code == 200

    response = client.post(f"/endpoint/{endpoint_name}/delete")
    assert response.status_code == 204
    response = client.get(f"/endpoint/{endpoint_name}/info")
    assert response.status_code == 404


def test_manage_autoscale(client):
    pass

from moriarty.matrix.operator_.params import (
    CreateEndpointParams,
    SetAutoscaleParams,
    UpdateEndpointParams,
)


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

    response = client.get("/endpoint/list", params={"cursor": endpoint_name})
    assert response.status_code == 200
    assert len(response.json()["endpoints"]) == 0

    response = client.get("/endpoint/list", params={"keyword": endpoint_name})
    assert response.status_code == 200
    assert len(response.json()["endpoints"]) == 1

    response = client.get("/endpoint/list", params={"keyword": "not-exist"})
    assert response.status_code == 200
    assert len(response.json()["endpoints"]) == 0

    for order_by in ["created_at", "updated_at", "endpoint_name"]:
        for order in ["asc", "desc"]:
            response = client.get(
                "/endpoint/list",
                params={"orderBy": order_by, "order": order},
            )
            assert response.status_code == 200

    response = client.post(
        f"/endpoint/{endpoint_name}/update",
        data=UpdateEndpointParams(image="busybox", queue_capacity=10).model_dump_json(),
    )
    assert response.status_code == 200

    response = client.post(
        "/endpoint/not-exist/update",
        data=UpdateEndpointParams(image="busybox", queue_capacity=10).model_dump_json(),
    )
    assert response.status_code == 404

    response = client.post(f"/endpoint/{endpoint_name}/delete")
    assert response.status_code == 204
    response = client.get(f"/endpoint/{endpoint_name}/info")
    assert response.status_code == 404


def test_manage_autoscale(client):
    endpoint_name = "hello-scaleable"

    response = client.post(
        "/endpoint/create",
        data=CreateEndpointParams(endpoint_name=endpoint_name, image="busybox").model_dump_json(),
    )
    assert response.status_code == 201

    response = client.get(f"/autoscale/{endpoint_name}/info")
    assert response.status_code == 404

    response = client.post(
        f"/autoscale/{endpoint_name}/set", data=SetAutoscaleParams().model_dump_json()
    )
    assert response.status_code == 200
    response = client.get(f"/autoscale/{endpoint_name}/info")
    assert response.status_code == 200

    response = client.post(f"/autoscale/{endpoint_name}/delete")
    assert response.status_code == 204

    response = client.get(f"/autoscale/{endpoint_name}/info")
    assert response.status_code == 404

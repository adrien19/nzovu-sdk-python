import grpc
import pytest
from api.application import create_app
from fastapi.testclient import TestClient

from nzovu import RpcOperationError

CART = {"items": [{"name": "apple", "quantity": 2, "price": 3.5}]}


class Failure(grpc.RpcError):
    def __init__(self, status):
        self.status = status

    def code(self):
        return self.status

    def details(self):
        return "fixture failure"


def failure(status):
    return RpcOperationError("fixture failure", cause=Failure(status))


@pytest.fixture
def web(sdk):
    app = create_app(sdk.asynchronous, client_factory=lambda **kwargs: sdk, workers_enabled=False)
    with TestClient(app) as client:
        yield client
    sdk.close.assert_called_once()
    assert app.state.nzovu is None


def test_shared_client_and_queue_setup(web, sdk):
    assert sdk.create_queue.call_count == 2
    response = web.post("/store/cart", params={"cart_id": "cart"}, json=CART)
    assert response.status_code == 200 and response.json()["status"] == "queued"
    assert sdk.post_message.call_args.args[0].message_id == "cart"
    assert web.get("/store/health").json()["active_heartbeats"] == 0


def test_enqueue_failure_is_reported(web, sdk):
    sdk.post_message.side_effect = failure(grpc.StatusCode.UNAVAILABLE)
    assert web.post("/store/cart?cart_id=cart", json=CART).status_code == 503


def test_queue_stats_and_pagination(web, sdk):
    assert web.get("/store/queue/store-cart/stats").json()["stats"]["stateCounts"]["PENDING"] == "3"
    assert web.get("/store/queue/store-cart/messages/pending").json()["total_messages"] == 4
    result = web.get("/store/queues?prefix=store&page_size=1&page_token=next")
    assert result.json()["nextPageToken"] == "cursor"
    sdk.list_queues.assert_called_once_with(prefix="store", page_size=1, page_token="next")
    assert web.get("/store/queues?page_size=1001").status_code == 422


def test_delete_and_not_found(web, sdk):
    assert web.delete("/store/queue/example").json()["success"]
    sdk.delete_queue.assert_called_once_with(name="example")
    sdk.get_queue_state.side_effect = failure(grpc.StatusCode.NOT_FOUND)
    assert web.get("/store/queue/missing/stats").status_code == 404


@pytest.mark.parametrize(
    "cart_id,cart",
    [("bad id", CART), ("cart", {"items": []}), ("cart", {"items": [{"name": "apple", "price": -1, "quantity": 1}]})],
)
def test_invalid_requests_do_not_enqueue(web, sdk, cart_id, cart):
    assert web.post("/store/cart", params={"cart_id": cart_id}, json=cart).status_code == 422
    sdk.post_message.assert_not_called()


def test_startup_failure_closes_client(sdk):
    sdk.create_queue.side_effect = failure(grpc.StatusCode.UNAVAILABLE)
    app = create_app(sdk.asynchronous, client_factory=lambda **kwargs: sdk)
    with pytest.raises(RpcOperationError):
        with TestClient(app):
            pass
    sdk.close.assert_called_once()


def test_existing_queues_are_accepted(sdk):
    sdk.create_queue.side_effect = failure(grpc.StatusCode.ALREADY_EXISTS)
    app = create_app(sdk.asynchronous, client_factory=lambda **kwargs: sdk, workers_enabled=False)
    with TestClient(app) as client:
        assert client.get("/store/health").status_code == 200

import asyncio
import threading
from unittest.mock import AsyncMock, Mock

import grpc
import pytest
from api.application import create_app
from api.sdk import invoke
from api.workers.process_store_cart_worker import process_once
from google.protobuf.json_format import ParseDict

from nzovu import Claim, ResponseWrapper, RpcOperationError
from nzovu.api.queueservice.v1 import request_response_pb2 as rpc

from .test_store import failure


@pytest.fixture
def claimed(sdk):
    proto = ParseDict(
        {
            "message": {
                "messageId": "cart",
                "metadata": {"payload": {"data": {"items": [{"name": "apple", "quantity": 2, "price": 3.5}]}}},
            },
            "workerId": "worker",
            "attemptId": "attempt",
        },
        rpc.GetNextMessageResponse(),
    )
    response = ResponseWrapper(proto)
    response.claim = Claim("store-cart", "cart", "worker", "attempt")
    sdk.get_next_message = (AsyncMock if sdk.asynchronous else Mock)(return_value=response)
    return response.claim


async def test_processing_keeps_original_claim(sdk, claimed):
    assert await process_once(sdk)
    posted = sdk.post_message.call_args.args[0]
    assert posted.message_id == "cart-checkout" and posted.data["total"] == 7
    ack = sdk.acknowledge_message.call_args.args[0]
    assert (ack.queue_name, ack.message_id, ack.worker_id, ack.attempt_id) == (
        claimed.queue_name,
        claimed.message_id,
        claimed.worker_id,
        claimed.attempt_id,
    )


async def test_uncertain_forward_does_not_ack(sdk, claimed):
    sdk.post_message.side_effect = failure(grpc.StatusCode.UNAVAILABLE)
    with pytest.raises(RpcOperationError):
        await process_once(sdk)
    sdk.acknowledge_message.assert_not_called()
    sdk.stop_heartbeat.assert_called_once_with(claimed)


async def test_duplicate_forward_can_finish_ack(sdk, claimed):
    sdk.post_message.side_effect = failure(grpc.StatusCode.ALREADY_EXISTS)
    assert await process_once(sdk)
    sdk.acknowledge_message.assert_called_once()


async def test_worker_tasks_join_on_shutdown(sdk):
    sdk.get_next_message = (AsyncMock if sdk.asynchronous else Mock)(side_effect=failure(grpc.StatusCode.NOT_FOUND))
    app = create_app(sdk.asynchronous, client_factory=lambda **kwargs: sdk)
    async with app.router.lifespan_context(app):
        tasks = app.state.worker_tasks
        assert len(tasks) == 2
        await asyncio.sleep(0.02)
    assert all(task.done() for task in tasks)
    sdk.close.assert_called_once()


async def test_sync_rpc_does_not_block_and_cancellation_joins():
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()

    def blocked():
        entered.set()
        release.wait(1)
        finished.set()

    task = asyncio.create_task(invoke(blocked))
    while not entered.is_set():
        await asyncio.sleep(0.001)
    task.cancel()
    await asyncio.sleep(0.01)
    assert not task.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert finished.is_set()

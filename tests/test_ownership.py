import asyncio
import inspect
import threading
import time
from unittest.mock import AsyncMock, Mock

import grpc
import pytest

from nzovu import AsyncNzovuClient, Claim, HeartbeatCapacityError, MessageState, NzovuClient, RpcOperationError
from nzovu.api.message.v1.message_pb2 import Message
from nzovu.api.queueservice.v1 import request_response_pb2 as rpc
from nzovu.exceptions import InitializationError, NzovuError


class RpcFailure(grpc.RpcError):
    def __init__(self, status=grpc.StatusCode.UNAVAILABLE):
        self.status = status

    def code(self):
        return self.status

    def details(self):
        return "server detail"

    def trailing_metadata(self):
        return (("grpc-status-details-bin", b"rich-status"),)


async def invoke(method, *args, **kwargs):
    result = method(*args, **kwargs)
    return await result if inspect.isawaitable(result) else result


async def until(predicate):
    deadline = time.monotonic() + 3
    while not predicate():
        assert time.monotonic() < deadline, "heartbeat condition timed out"
        await asyncio.sleep(0.005)


@pytest.fixture(params=[False, True], ids=["sync", "async"])
async def client(request):
    asynchronous = request.param
    cls = AsyncNzovuClient if asynchronous else NzovuClient
    c = cls(
        "localhost",
        use_tls=False,
        heartbeat_interval=0.01,
        **({"heartbeat_task_limit": 2} if asynchronous else {"heartbeat_thread_pool_size": 2}),
    )
    c.stub = AsyncMock() if asynchronous else Mock()
    c.stub.GetNextMessage.return_value = rpc.GetNextMessageResponse(
        message=Message(message_id="message"),
        worker_id="worker",
        attempt_id="attempt",
    )
    c.stub.SendMessageHeartBeat.return_value = rpc.SendMessageHeartBeatResponse(state=Message.Metadata.RUNNING)
    c.stub.AcknowledgeMessage.return_value = rpc.AcknowledgeMessageResponse(success=True)
    yield c
    await invoke(c.close)


async def claim(client, queue="queue", heartbeat=True):
    return (await invoke(client.get_next_message, queue, "30s", enable_heartbeat=heartbeat)).claim


async def test_claim_without_heartbeat_and_explicit_ownership(client):
    owner = await claim(client, heartbeat=False)
    assert owner == Claim("queue", "message", "worker", "attempt")
    assert owner.to_dict() == dict(queue_name="queue", message_id="message", worker_id="worker", attempt_id="attempt")
    assert client.get_active_heartbeat_count() == 0
    await invoke(client.acknowledge_message, owner.acknowledge(MessageState.COMPLETED))
    request = client.stub.AcknowledgeMessage.call_args.args[0]
    assert request.worker_id == "worker" and request.attempt_id == "attempt"
    await invoke(client.renew_message_lease, **owner.to_dict(), new_lease_duration="30s")
    await invoke(client.send_message_heartbeat, **owner.to_dict())


@pytest.mark.parametrize("method", ["acknowledge", "heartbeat", "renew"])
async def test_missing_ownership_never_hits_server(client, method):
    with pytest.raises(ValueError, match="required"):
        if method == "acknowledge":
            from nzovu import AcknowledgeMessageParams

            await invoke(client.acknowledge_message, AcknowledgeMessageParams("message", MessageState.COMPLETED))
        elif method == "heartbeat":
            await invoke(client.send_message_heartbeat, "queue", "message")
        else:
            await invoke(client.renew_message_lease, "queue", "message", "30s")
    assert not client.stub.mock_calls


async def test_ack_failure_retains_heartbeat_and_status(client):
    owner = await claim(client)
    worker = client._heartbeats.workers[owner]
    error = RpcFailure()
    client.stub.AcknowledgeMessage.side_effect = error
    with pytest.raises(RpcOperationError) as raised:
        await invoke(client.acknowledge_message, owner.acknowledge(MessageState.COMPLETED))
    assert raised.value.code() == error.code()
    assert raised.value.details() == "server detail"
    assert raised.value.trailing_metadata() == error.trailing_metadata()
    assert raised.value.__cause__ is error
    assert not worker.stop.is_set()
    client.stub.AcknowledgeMessage.side_effect = None
    client.stub.AcknowledgeMessage.return_value = rpc.AcknowledgeMessageResponse(success=False)
    await invoke(client.acknowledge_message, owner.acknowledge(MessageState.COMPLETED))
    assert not worker.stop.is_set()
    client.stub.AcknowledgeMessage.return_value.success = True
    await invoke(client.acknowledge_message, owner.acknowledge(MessageState.COMPLETED))
    await until(lambda: client.get_active_heartbeat_count() == 0)


@pytest.mark.parametrize(
    "status", [grpc.StatusCode.NOT_FOUND, grpc.StatusCode.FAILED_PRECONDITION, grpc.StatusCode.PERMISSION_DENIED]
)
async def test_terminal_ack_only_stops_matching_claim(client, status):
    old = await claim(client)
    client.stub.GetNextMessage.return_value.attempt_id = "replacement"
    replacement = await claim(client)
    client.stub.AcknowledgeMessage.side_effect = RpcFailure(status)
    errors = []
    await invoke(client.acknowledge_message, old.acknowledge(MessageState.COMPLETED), error_handler=errors.append)
    assert errors[0].code() == status
    await until(lambda: old not in client.get_active_heartbeats())
    assert replacement in client.get_active_heartbeats()
    assert not client._heartbeats.workers[replacement].stop.is_set()


async def test_duplicate_ids_across_queues_and_bounded_admission(client):
    first = await claim(client, "first")
    second = await claim(client, "second")
    assert set(client.get_active_heartbeats()) == {first, second}
    with pytest.raises(HeartbeatCapacityError):
        await claim(client, "third")
    assert client.stub.GetNextMessage.call_count == 2
    await invoke(client.stop_heartbeat, first)
    await until(lambda: first not in client.get_active_heartbeats())
    third = await claim(client, "third")
    assert set(client.get_active_heartbeats()) == {second, third}
    stats = client.get_heartbeat_stats()
    stats[second]["heartbeats_sent"] = -1
    assert client.get_heartbeat_stats()[second]["heartbeats_sent"] >= 0


@pytest.mark.parametrize("outcome", ["empty", "error", "invalid-owner"])
async def test_failed_claim_releases_capacity(client, outcome):
    if outcome == "empty":
        client.stub.GetNextMessage.return_value = rpc.GetNextMessageResponse()
        assert await claim(client) is None
    else:
        if outcome == "error":
            client.stub.GetNextMessage.side_effect = RpcFailure()
        else:
            client.stub.GetNextMessage.return_value.worker_id = ""
        with pytest.raises((RpcOperationError, ValueError)):
            await claim(client)
    assert client._heartbeats.reservations == 0


async def test_retry_reconnect_uses_exact_claim(client):
    client.stub.SendMessageHeartBeat.side_effect = [
        RpcFailure(),
        rpc.SendMessageHeartBeatResponse(state=Message.Metadata.RUNNING),
        rpc.SendMessageHeartBeatResponse(state=Message.Metadata.COMPLETED),
    ]
    owner = await claim(client)
    await until(lambda: client.get_active_heartbeat_count() == 0)
    assert client.stub.SendMessageHeartBeat.call_count == 3
    for call in client.stub.SendMessageHeartBeat.call_args_list:
        request = call.args[0]
        assert (request.queue_name, request.message_id, request.worker_id, request.attempt_id) == (
            owner.queue_name,
            owner.message_id,
            owner.worker_id,
            owner.attempt_id,
        )
        assert 0 < call.kwargs["timeout"] <= 5
    assert client._heartbeats.reservations == 0


async def test_terminal_heartbeat_is_not_retried(client):
    client.stub.SendMessageHeartBeat.side_effect = RpcFailure(grpc.StatusCode.FAILED_PRECONDITION)
    callback = Mock()
    client._heartbeats.callback = callback
    owner = await claim(client)
    await until(lambda: client.get_active_heartbeat_count() == 0)
    assert client.stub.SendMessageHeartBeat.call_count == 1
    assert callback.call_args.args[0]["attempt_id"] == owner.attempt_id


async def test_close_interrupts_backoff_and_joins_workers(client):
    client.stub.SendMessageHeartBeat.side_effect = RpcFailure()
    await claim(client)
    await until(lambda: client.stub.SendMessageHeartBeat.call_count > 0)
    start = time.monotonic()
    await invoke(client.close, timeout=1)
    assert time.monotonic() - start < 1
    assert client.get_active_heartbeats() == {}
    assert client._heartbeats.reservations == 0
    with pytest.raises(NzovuError, match="closed"):
        await claim(client)
    if isinstance(client, NzovuClient):
        assert all(not thread.is_alive() for thread in client._heartbeats.executor._threads)


@pytest.mark.parametrize("limit", ["max_count", "max_duration"])
async def test_worker_limits_release_slots(client, limit):
    setattr(client._heartbeats, limit, 1 if limit == "max_count" else 0.03)
    await claim(client)
    await until(lambda: client.get_active_heartbeat_count() == 0)
    assert client._heartbeats.reservations == 0


async def test_async_cancellation_before_worker_starts_releases_slot():
    c = AsyncNzovuClient("localhost", use_tls=False)
    c.stub = AsyncMock()
    owner = Claim("q", "m", "w", "a")
    c._heartbeats.reserve()
    c._heartbeats.start(owner, c.stub)
    task = c._heartbeats.workers[owner].future
    task.cancel()
    await c.close()
    assert task.done()
    assert c.get_active_heartbeats() == {}
    assert c._heartbeats.reservations == 0


async def test_async_cancellation_during_claim_releases_slot():
    c = AsyncNzovuClient("localhost", use_tls=False)
    started = asyncio.Event()

    async def pending(*args):
        started.set()
        await asyncio.Event().wait()

    c.stub = Mock(GetNextMessage=pending)
    task = asyncio.create_task(c.get_next_message("q", "30s", enable_heartbeat=True))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert c._heartbeats.reservations == 0
    await c.close()


async def test_async_close_cancels_inflight_heartbeat():
    c = AsyncNzovuClient("localhost", use_tls=False)
    started = asyncio.Event()
    finished = asyncio.Event()

    async def pending(*args, **kwargs):
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            finished.set()

    c.stub = Mock(SendMessageHeartBeat=pending)
    c._heartbeats.reserve()
    c._heartbeats.start(Claim("q", "m", "w", "a"), c.stub)
    await started.wait()
    await c.close(timeout=1)
    assert finished.is_set()
    assert c.get_active_heartbeats() == {}


def test_sync_close_cancels_inflight_rpc_before_join():
    c = NzovuClient("localhost", use_tls=False)
    c.channel.close()
    started, cancelled = threading.Event(), threading.Event()

    def pending(*args, **kwargs):
        started.set()
        assert cancelled.wait(1), "channel must close before worker join"
        raise RpcFailure(grpc.StatusCode.CANCELLED)

    c.channel = Mock(close=cancelled.set)
    c.stub = Mock(SendMessageHeartBeat=pending)
    c._heartbeats.reserve()
    c._heartbeats.start(Claim("q", "m", "w", "a"), c.stub)
    assert started.wait(1)
    c.close(timeout=1)
    assert c.get_active_heartbeats() == {}


@pytest.mark.parametrize(
    "kwargs",
    [
        {"heartbeat_interval": 0},
        {"heartbeat_max_count": 0},
        {"heartbeat_max_duration": float("nan")},
        {"heartbeat_task_limit": 0},
    ],
)
def test_invalid_worker_configuration(kwargs):
    with pytest.raises(InitializationError):
        AsyncNzovuClient("localhost", **kwargs)


async def test_renewal_limit_does_not_imply_ownership_loss(client):
    owner = await claim(client)
    client.stub.RenewMessageLease.side_effect = RpcFailure(grpc.StatusCode.FAILED_PRECONDITION)
    with pytest.raises(RpcOperationError):
        await invoke(client.renew_message_lease, **owner.to_dict(), new_lease_duration="30s")
    assert not client._heartbeats.workers[owner].stop.is_set()

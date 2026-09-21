from unittest.mock import AsyncMock, Mock

import grpc
import pytest

from nzovu import AsyncNzovuClient, NzovuClient, models
from nzovu.api.message.v1.message_pb2 import Message
from nzovu.api.queueservice.v1 import request_response_pb2
from nzovu.api.schedule.v1 import schedule_pb2
from nzovu.exceptions import RpcOperationError
from nzovu.utils import AcknowledgeMessageParams, MessageState, PeekQueueMessagesParams, ScheduleOptions, ScheduleState

PAGED_METHODS = [
    ("list_queues", "ListQueues", {"prefix": "orders_"}),
    ("list_schedules", "ListSchedules", {"prefix": "daily_"}),
    ("list_schemas", "ListSchemas", {"prefix": "orders_", "active_only": True}),
    ("get_schedule_history", "GetScheduleHistory", {"schedule_id": "daily"}),
    ("get_dlq_messages", "GetDLQMessages", {"dlq_name": "orders_dlq"}),
    ("peek_queue_messages", "PeekQueueMessages", {"queue_name": "orders"}),
]


def client_with_stub(asynchronous):
    client = object.__new__(AsyncNzovuClient if asynchronous else NzovuClient)
    client.stub = AsyncMock() if asynchronous else Mock()
    client._heartbeat_control = {}
    client._heartbeat_stop_events = {}
    return client


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("method,rpc,filters", PAGED_METHODS)
@pytest.mark.parametrize("page_size,page_token,next_token", [(0, "", ""), (25, "cursor-in", "cursor-out")])
async def test_pagination_preserves_filters_and_response_tokens(
    asynchronous, method, rpc, filters, page_size, page_token, next_token
):
    client = client_with_stub(asynchronous)
    response_type = getattr(request_response_pb2, rpc + "Response")
    response = response_type(next_page_token=next_token)
    stub_method = getattr(client.stub, rpc)
    stub_method.return_value = response
    kwargs = dict(filters, page_size=page_size, page_token=page_token)
    if method == "peek_queue_messages":
        kwargs = {"params": PeekQueueMessagesParams(**kwargs)}
    result = getattr(client, method)(**kwargs)
    if asynchronous:
        result = await result
    request = stub_method.call_args.args[0]
    for name, value in filters.items():
        assert getattr(request, name) == value
    assert request.page_size == page_size
    assert request.page_token == page_token
    stub_method.assert_called_once()
    assert result.to_proto() == response
    assert result.to_dict().get("nextPageToken", "") == next_token
    if models.PYDANTIC_AVAILABLE:
        assert result.to_model().next_page_token == next_token
    else:
        with pytest.raises(ImportError):
            result.to_model()


class FailedRpc(grpc.RpcError):
    def details(self):
        return "invalid continuation token"


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("method,rpc,filters", PAGED_METHODS)
async def test_pagination_rpc_errors_remain_visible(asynchronous, method, rpc, filters):
    client = client_with_stub(asynchronous)
    getattr(client.stub, rpc).side_effect = FailedRpc()
    kwargs = dict(filters, page_token="invalid")
    if method == "peek_queue_messages":
        kwargs = {"params": PeekQueueMessagesParams(**kwargs)}
    with pytest.raises(RpcOperationError, match="invalid continuation token"):
        result = getattr(client, method)(**kwargs)
        if asynchronous:
            await result


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("state", [MessageState.COMPLETED, MessageState.COMPLETED.value])
async def test_ack_accepts_public_enum_and_preserves_claim(asynchronous, state):
    client = client_with_stub(asynchronous)
    client.stub.AcknowledgeMessage.return_value = request_response_pb2.AcknowledgeMessageResponse()
    params = AcknowledgeMessageParams("message", state, "orders", "worker", "attempt")
    result = client.acknowledge_message(params)
    if asynchronous:
        await result
    request = client.stub.AcknowledgeMessage.call_args.args[0]
    assert request.state == Message.Metadata.COMPLETED
    assert (request.queue_name, request.message_id, request.worker_id, request.attempt_id) == (
        "orders",
        "message",
        "worker",
        "attempt",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_schedule_state_and_model_use_current_descriptor(asynchronous):
    client = client_with_stub(asynchronous)
    client.stub.CreateSchedule.return_value = request_response_pb2.CreateScheduleResponse(success=True)
    options = ScheduleOptions(
        payload={"task": "report"}, queue_name="reports", cron_schedule="0 * * * *", state=ScheduleState.PAUSED
    )
    result = client.create_schedule("hourly", options)
    if asynchronous:
        await result
    request = client.stub.CreateSchedule.call_args.args[0]
    assert request.schedule.metadata.state == schedule_pb2.Schedule.Metadata.PAUSED
    if not models.PYDANTIC_AVAILABLE:
        return
    from nzovu.models import Schedule

    model = Schedule.from_proto(request.schedule)
    assert model.metadata.state == "PAUSED"
    assert "exclusivity_key" not in model.metadata.model_dump()


def test_removed_schedule_option_is_rejected():
    with pytest.raises(TypeError, match="exclusivity_key"):
        ScheduleOptions(payload={}, queue_name="orders", cron_schedule="0 * * * *", exclusivity_key="removed")

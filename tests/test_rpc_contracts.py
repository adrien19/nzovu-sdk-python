import inspect
from unittest.mock import AsyncMock, Mock

import grpc
import pytest
from google.protobuf import json_format

from nzovu import AsyncNzovuClient, NzovuClient, models
from nzovu.api.queueservice.v1 import request_response_pb2 as rpc
from nzovu.api.queueservice.v1 import service_pb2
from nzovu.exceptions import RpcOperationError
from nzovu.heartbeat import AsyncHeartbeats, SyncHeartbeats
from nzovu.utils import (
    AcknowledgeMessageParams,
    MessageState,
    PeekQueueMessagesParams,
    PostMessageParams,
    ScheduleOptions,
    SchemaOptions,
)

CALENDAR = {
    "type": "DAILY",
    "timezone": "UTC",
    "rules": [{"daily": {"day_interval": 1}, "execution_times": [{"hour": 8}]}],
}
POST = {"messageId": "msg", "metadata": {"payload": {"data": {"task": "report"}}}}
CASES = [
    ("CreateQueue", "create_queue", {"name": "orders"}, {"name": "orders"}),
    ("DeleteQueue", "delete_queue", {"name": "orders"}, {"name": "orders"}),
    (
        "ListQueues",
        "list_queues",
        {"prefix": "orders", "page_size": 3, "page_token": "next"},
        {"prefix": "orders", "pageSize": 3, "pageToken": "next"},
    ),
    (
        "PostMessage",
        "post_message",
        {"msg_params": PostMessageParams("msg", {"task": "report"}, "orders")},
        {"queueName": "orders", "message": POST},
    ),
    (
        "PostMessagesBulk",
        "post_messages_bulk",
        {
            "queue_name": "orders",
            "messages": [PostMessageParams("msg", {"task": "report"}, "orders")],
            "transaction_mode": "BEST_EFFORT",
        },
        {"queueName": "orders", "messages": [POST], "transactionMode": "BEST_EFFORT"},
    ),
    (
        "GetNextMessage",
        "get_next_message",
        {"queue_name": "orders", "lease_duration": "1.000000001s", "worker_id": "worker", "attempt_id": "attempt"},
        {"queueName": "orders", "leaseDuration": "1.000000001s", "workerId": "worker", "attemptId": "attempt"},
    ),
    (
        "AcknowledgeMessage",
        "acknowledge_message",
        {"params": AcknowledgeMessageParams("msg", MessageState.COMPLETED, "orders", "worker", "attempt")},
        {"queueName": "orders", "messageId": "msg", "state": "COMPLETED", "workerId": "worker", "attemptId": "attempt"},
    ),
    (
        "CancelMessage",
        "cancel_message",
        {"queue_name": "orders", "message_id": "msg", "reason": "cancel"},
        {"queueName": "orders", "messageId": "msg", "reason": "cancel"},
    ),
    (
        "RenewMessageLease",
        "renew_message_lease",
        {
            "queue_name": "orders",
            "message_id": "msg",
            "new_lease_duration": "0.000000001s",
            "worker_id": "worker",
            "attempt_id": "attempt",
        },
        {
            "queueName": "orders",
            "messageId": "msg",
            "leaseDuration": "0.000000001s",
            "workerId": "worker",
            "attemptId": "attempt",
        },
    ),
    (
        "SendMessageHeartBeat",
        "send_message_heartbeat",
        {"queue_name": "orders", "message_id": "msg", "worker_id": "worker", "attempt_id": "attempt"},
        {"queueName": "orders", "messageId": "msg", "workerId": "worker", "attemptId": "attempt"},
    ),
    (
        "PeekQueueMessages",
        "peek_queue_messages",
        {"params": PeekQueueMessagesParams("orders", 5, None, "next")},
        {"queueName": "orders", "pageSize": 5, "pageToken": "next"},
    ),
    ("GetQueueState", "get_queue_state", {"queue_name": "orders"}, {"queueName": "orders"}),
    (
        "CreateSchedule",
        "create_schedule",
        {"schedule_id": "daily", "options": ScheduleOptions({"task": "report"}, "orders", calendar_schedule=CALENDAR)},
        {
            "schedule": {
                "scheduleId": "daily",
                "metadata": {
                    "payload": {"data": {"task": "report"}},
                    "queueName": "orders",
                    "calendarSchedule": CALENDAR,
                },
            }
        },
    ),
    ("DeleteSchedule", "delete_schedule", {"schedule_id": "daily"}, {"scheduleId": "daily"}),
    ("GetSchedule", "get_schedule", {"schedule_id": "daily"}, {"scheduleId": "daily"}),
    ("PauseSchedule", "pause_schedule", {"schedule_id": "daily"}, {"scheduleId": "daily"}),
    ("ResumeSchedule", "resume_schedule", {"schedule_id": "daily"}, {"scheduleId": "daily"}),
    (
        "ListSchedules",
        "list_schedules",
        {"prefix": "daily", "page_size": 3, "page_token": "next"},
        {"prefix": "daily", "pageSize": 3, "pageToken": "next"},
    ),
    (
        "GetScheduleHistory",
        "get_schedule_history",
        {"schedule_id": "daily", "page_size": 3, "page_token": "next"},
        {"scheduleId": "daily", "pageSize": 3, "pageToken": "next"},
    ),
    (
        "ValidateCalendarSchedule",
        "validate_calendar_schedule",
        {"calendar_schedule": CALENDAR},
        {"calendarSchedule": CALENDAR},
    ),
    (
        "PreviewCalendarSchedule",
        "preview_calendar_schedule",
        {"calendar_schedule": CALENDAR, "count": 4},
        {"calendarSchedule": CALENDAR, "count": 4},
    ),
    (
        "RegisterSchema",
        "register_schema",
        {
            "schema_id": "order",
            "options": SchemaOptions("Order", "description", '{"type":"object"}', metadata={"owner": "sales"}),
        },
        {
            "schemaId": "order",
            "name": "Order",
            "description": "description",
            "content": '{"type":"object"}',
            "contentType": "json-schema",
            "metadata": {"owner": "sales"},
        },
    ),
    ("GetSchema", "get_schema", {"schema_id": "order", "version": 2}, {"schemaId": "order", "version": 2}),
    (
        "ListSchemas",
        "list_schemas",
        {"prefix": "order", "page_size": 3, "page_token": "next", "active_only": True},
        {"prefix": "order", "pageSize": 3, "pageToken": "next", "activeOnly": True},
    ),
    ("DeleteSchema", "delete_schema", {"schema_id": "order", "version": 2}, {"schemaId": "order", "version": 2}),
    (
        "ValidatePayload",
        "validate_payload",
        {"schema_id": "order", "version": 2, "payload": "{}"},
        {"schemaId": "order", "version": 2, "payload": "{}"},
    ),
    (
        "GetDLQMessages",
        "get_dlq_messages",
        {"dlq_name": "dlq", "page_size": 3, "page_token": "next"},
        {"dlqName": "dlq", "pageSize": 3, "pageToken": "next"},
    ),
    (
        "RequeueFromDLQ",
        "requeue_from_dlq",
        {"dlq_name": "dlq", "message_id": "msg", "target_queue": "orders"},
        {"dlqName": "dlq", "messageId": "msg", "targetQueue": "orders"},
    ),
    (
        "DeleteFromDLQ",
        "delete_from_dlq",
        {"dlq_name": "dlq", "message_id": "msg"},
        {"dlqName": "dlq", "messageId": "msg"},
    ),
    ("PurgeDLQ", "purge_dlq", {"dlq_name": "dlq"}, {"dlqName": "dlq"}),
    ("GetDLQStats", "get_dlq_stats", {"dlq_name": "dlq"}, {"dlqName": "dlq"}),
]


def make_client(asynchronous):
    client = object.__new__(AsyncNzovuClient if asynchronous else NzovuClient)
    client.stub = AsyncMock() if asynchronous else Mock()
    client._worker_id = "default-worker"
    client._heartbeats = (AsyncHeartbeats if asynchronous else SyncHeartbeats)(20, 300, 1000, 1, None, None)
    return client


def test_all_rpc_contracts_and_signatures_are_covered():
    service = service_pb2.DESCRIPTOR.services_by_name["QueueService"]
    assert {item[0] for item in CASES} == {method.name for method in service.methods}
    for _, method, _, _ in CASES:

        def parameters(cls):
            return [(p.name, p.kind, p.default) for p in inspect.signature(getattr(cls, method)).parameters.values()]

        assert parameters(NzovuClient) == parameters(AsyncNzovuClient), method


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("rpc_name,method,kwargs,expected", CASES, ids=[case[0] for case in CASES])
async def test_every_rpc_request_and_response(asynchronous, rpc_name, method, kwargs, expected):
    client = make_client(asynchronous)
    response = getattr(rpc, rpc_name + "Response")()
    getattr(client.stub, rpc_name).return_value = response
    result = getattr(client, method)(**kwargs)
    if asynchronous:
        result = await result
    stub = getattr(client.stub, rpc_name)
    stub.assert_called_once()
    expected_request = json_format.ParseDict(expected, getattr(rpc, rpc_name + "Request")())
    assert stub.call_args.args[0] == expected_request
    assert result.to_proto() is response
    assert result.to_dict() == json_format.MessageToDict(response)
    if models.PYDANTIC_AVAILABLE:
        model = result.to_model()
        assert type(model).__name__ == rpc_name + "Response"
        for field in response.DESCRIPTOR.fields:
            assert field.name in type(model).model_fields
            if field.type == field.TYPE_BOOL:
                assert getattr(model, field.name) is False
    else:
        with pytest.raises(ImportError):
            result.to_model()


class FailedRpc(grpc.RpcError):
    def details(self):
        return "server rejected request"

    def code(self):
        return grpc.StatusCode.INVALID_ARGUMENT

    def trailing_metadata(self):
        return (("grpc-status-details-bin", b"detail"),)


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("rpc_name,method,kwargs,expected", CASES, ids=[case[0] for case in CASES])
async def test_every_rpc_propagates_errors_and_invokes_handler(asynchronous, rpc_name, method, kwargs, expected):
    client = make_client(asynchronous)
    getattr(client.stub, rpc_name).side_effect = FailedRpc()
    with pytest.raises(RpcOperationError, match="server rejected request") as raised:
        result = getattr(client, method)(**kwargs)
        if asynchronous:
            await result
    assert raised.value.code() == grpc.StatusCode.INVALID_ARGUMENT
    assert raised.value.details() == "server rejected request"
    assert raised.value.trailing_metadata() == (("grpc-status-details-bin", b"detail"),)
    handler = Mock()
    result = getattr(client, method)(**kwargs, error_handler=handler)
    if asynchronous:
        result = await result
    assert result is None
    handler.assert_called_once()
    assert isinstance(handler.call_args.args[0], RpcOperationError)
    assert handler.call_args.args[0].code() == grpc.StatusCode.INVALID_ARGUMENT
    assert handler.call_args.args[0].trailing_metadata() == (("grpc-status-details-bin", b"detail"),)


def populated_message(message):
    name = message.DESCRIPTOR.full_name
    if name in {"google.protobuf.Timestamp", "google.protobuf.Duration"}:
        message.seconds = 0
        message.nanos = 1
        return message
    if name in {"google.protobuf.Struct", "google.protobuf.Value"}:
        return json_format.ParseDict({"sample": [None, True, 3]}, message)
    oneofs = set()
    for field in message.DESCRIPTOR.fields:
        if field.containing_oneof:
            if field.containing_oneof.name in oneofs:
                continue
            oneofs.add(field.containing_oneof.name)
        if field.type == field.TYPE_MESSAGE:
            if field.label == field.LABEL_REPEATED:
                if field.message_type.GetOptions().map_entry:
                    value_field = field.message_type.fields_by_name["value"]
                    key_field = field.message_type.fields_by_name["key"]
                    key = "sample" if key_field.type == key_field.TYPE_STRING else 2
                    target = getattr(message, field.name)
                    if value_field.type == value_field.TYPE_MESSAGE:
                        populated_message(target[key])
                    else:
                        target[key] = scalar_value(value_field)
                else:
                    populated_message(getattr(message, field.name).add())
            else:
                populated_message(getattr(message, field.name))
        elif field.label == field.LABEL_REPEATED:
            getattr(message, field.name).append(scalar_value(field))
        else:
            setattr(message, field.name, scalar_value(field))
    return message


def scalar_value(field):
    if field.type == field.TYPE_STRING:
        return "sample"
    if field.type == field.TYPE_BYTES:
        return b"\xff\x00"
    if field.type == field.TYPE_BOOL:
        return True
    if field.type == field.TYPE_ENUM:
        return field.enum_type.values[0].number
    if field.type == field.TYPE_INT64:
        return 2**60 + 7
    return 7


def assert_model_matches_proto(model, proto):
    if proto.DESCRIPTOR.full_name in {"google.protobuf.Timestamp", "google.protobuf.Duration"}:
        assert model == proto.ToJsonString()
        return
    if proto.DESCRIPTOR.full_name in {"google.protobuf.Struct", "google.protobuf.Value"}:
        assert model == json_format.MessageToDict(proto)
        return
    for field in proto.DESCRIPTOR.fields:
        if field.has_presence and not proto.HasField(field.name):
            continue
        value = getattr(proto, field.name)
        actual = model[field.name] if isinstance(model, dict) else getattr(model, field.name)
        if field.label == field.LABEL_REPEATED:
            if field.message_type and field.message_type.GetOptions().map_entry:
                assert set(actual) == set(value)
                entry = field.message_type.fields_by_name["value"]
                for key, item in value.items():
                    if entry.type == entry.TYPE_MESSAGE:
                        assert_model_matches_proto(actual[key], item)
                    else:
                        assert actual[key] == item
            else:
                assert len(actual) == len(value)
                for item, original in zip(actual, value):
                    if field.type == field.TYPE_MESSAGE:
                        assert_model_matches_proto(item, original)
                    else:
                        assert item == original
        elif field.type == field.TYPE_MESSAGE:
            assert_model_matches_proto(actual, value)
        elif field.type == field.TYPE_ENUM:
            assert actual == field.enum_type.values_by_number[value].name
        else:
            assert actual == value


@pytest.mark.skipif(not models.PYDANTIC_AVAILABLE, reason="Pydantic not installed")
@pytest.mark.parametrize("rpc_name", [case[0] for case in CASES])
def test_populated_response_preserves_every_nested_protocol_field(rpc_name):
    from nzovu.utils import ResponseWrapper

    response = populated_message(getattr(rpc, rpc_name + "Response")())
    assert_model_matches_proto(ResponseWrapper(response).to_model(), response)

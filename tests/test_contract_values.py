from unittest.mock import AsyncMock, Mock

import pytest
from google.protobuf import json_format

from nzovu import AsyncNzovuClient, NzovuClient, models
from nzovu.api.common.v1 import common_pb2
from nzovu.api.message.v1 import message_pb2
from nzovu.api.queueservice.v1 import request_response_pb2 as rpc
from nzovu.api.schedule.v1 import schedule_pb2
from nzovu.utils import (
    Header,
    LeasePolicyOptions,
    MessagePriorityRange,
    PeekQueueMessagesParams,
    PostMessageOptions,
    PostMessageParams,
    ResponseWrapper,
    ScheduleOptions,
    _create_post_message_request,
    build_headers,
    build_lease_policy,
    build_payload,
    build_schedule_request,
    string_to_duration,
)


@pytest.mark.parametrize(
    "text,seconds,nanos",
    [("0.000000001s", 0, 1), ("1.123456789s", 1, 123456789), ("0.000000001m", 0, 60), ("1d", 86400, 0)],
)
def test_exact_duration(text, seconds, nanos):
    duration = string_to_duration(text)
    assert (duration.seconds, duration.nanos) == (seconds, nanos)


@pytest.mark.parametrize("text", ["-1s", "1.0000000001s", "NaNs", "315576000001s"])
def test_invalid_duration(text):
    with pytest.raises(ValueError):
        string_to_duration(text)


@pytest.mark.parametrize("maximum", [None, 0, 5])
def test_lease_override_presence(maximum):
    policy = build_lease_policy(LeasePolicyOptions(base_lease="0s", max_renewals=maximum))
    assert policy.HasField("base_lease")
    assert policy.HasField("max_renewals") == (maximum is not None)
    if maximum is not None:
        assert policy.max_renewals == maximum
    if models.PYDANTIC_AVAILABLE:
        assert models.LeasePolicy.from_proto(policy).max_renewals == maximum
    assert build_lease_policy(LeasePolicyOptions()) is None
    assert build_lease_policy(LeasePolicyOptions(max_renewals=0)).HasField("max_renewals")


@pytest.mark.parametrize("value", [-1, 5, True, 1.5])
@pytest.mark.parametrize("kind", ["message", "schedule", "range"])
def test_priority_rejected(value, kind):
    with pytest.raises(ValueError):
        if kind == "message":
            PostMessageOptions(priority=value)
        elif kind == "schedule":
            ScheduleOptions({}, "orders", cron_schedule="* * * * *", priority=value)
        else:
            MessagePriorityRange(min=value)


@pytest.mark.parametrize("value", [0, 4])
def test_priority_endpoints_and_binary_headers(value):
    headers = [Header("trace", b"\xff\x00"), Header("trace", b"second")]
    options = PostMessageOptions(
        priority=value,
        headers=headers,
        data_metadata={"flag": True, "count": 7, "list": [None, {"a": "b"}]},
        schema_id="order",
        schema_version=2,
        content_type="application/json",
        scheduled_time="1970-01-01T00:00:00.000000001Z",
        lease_policy=LeasePolicyOptions(max_renewals=0),
    )
    request = _create_post_message_request(PostMessageParams("message", {"task": "report"}, "orders", options))
    metadata = request.message.metadata
    assert metadata.priority == value
    assert [(h.key, h.value) for h in metadata.headers] == [("trace", b"\xff\x00"), ("trace", b"second")]
    assert metadata.scheduled_time.nanos == 1
    assert metadata.lease_policy.HasField("max_renewals")
    assert json_format.MessageToDict(metadata.payload.metadata["list"]) == [None, {"a": "b"}]
    assert metadata.payload.schema_id == "order" and metadata.payload.schema_version == 2
    if models.PYDANTIC_AVAILABLE:
        model = models.Message.from_proto(request.message)
        assert model.metadata.headers[0].value == b"\xff\x00"
        assert model.metadata.scheduled_time == "1970-01-01T00:00:00.000000001Z"
        assert model.metadata.payload.metadata["count"] == 7
        assert model.metadata.lease_policy.max_renewals == 0
        assert model.metadata.current_attempt is None
        model.model_dump_json()


@pytest.mark.parametrize(
    "headers",
    [
        [Header("Upper", b"")],
        [Header("bad_key", b"")],
        [Header("x-nzovu-a", b"")],
        [Header("x-internal-a", b"")],
        [Header("x-system-a", b"")],
        [Header("", b"")],
        [Header("key", "not-bytes")],
        [Header("key", b"a" * 4097)],
        [Header("key", b"a" * 4096)] * 8,
        [None],
    ],
)
def test_invalid_headers(headers):
    with pytest.raises(ValueError):
        build_headers(headers)


def test_header_size_boundaries_and_schedule_payload():
    assert len(build_headers([Header("a", b"a" * 4096)])[0].value) == 4096
    headers = [Header("a", b"a" * 4095)] * 8
    assert len(build_headers(headers)) == 8
    request = build_schedule_request(
        "daily",
        ScheduleOptions(
            {},
            "orders",
            calendar_schedule={"type": "DAILY", "timezone": "UTC"},
            headers=headers,
            data_metadata={"owner": "sales"},
            max_messages=0,
            lease_duration="0.000000001s",
        ),
    )
    metadata = request.schedule.metadata
    assert metadata.has_max_messages and metadata.max_messages == 0
    assert len(metadata.headers) == 8
    assert metadata.payload.metadata["owner"].string_value == "sales"
    assert metadata.calendar_schedule.timezone == "UTC"
    if models.PYDANTIC_AVAILABLE:
        model = models.Schedule.from_proto(request.schedule)
        assert model.metadata.calendar_schedule["timezone"] == "UTC"
        assert model.metadata.payload.metadata == {"owner": "sales"}
        assert model.metadata.lease_duration == "0.000000001s"


@pytest.mark.parametrize("message_id", ["", "a" * 257, "bad:id", "spaced id", "café"])
def test_message_ids_follow_server_validator(message_id):
    with pytest.raises(ValueError):
        _create_post_message_request(PostMessageParams(message_id, {}, "orders"))


@pytest.mark.parametrize("content_type", ["application/json", "application/x-json; charset=utf-8", ""])
def test_json_payload_types(content_type):
    assert build_payload({}, {"n": None}, content_type).content_type == content_type


@pytest.mark.parametrize(
    "data,metadata,content_type",
    [({}, {}, "application/xml"), ({"n": float("nan")}, {}, ""), ([], {}, ""), ({}, [], "")],
)
def test_invalid_payload(data, metadata, content_type):
    with pytest.raises(ValueError):
        build_payload(data, metadata, content_type)


@pytest.mark.skipif(not models.PYDANTIC_AVAILABLE, reason="Pydantic not installed")
def test_schedule_history_and_calendar_results_preserve_every_field():
    history = schedule_pb2.ScheduleHistory(schedule_id="daily")
    history.created_at.FromJsonString("1970-01-01T00:00:00Z")
    history.updated_at.FromJsonString("1970-01-01T00:00:00.000000001Z")
    execution = history.executions.add(message_id="msg", success=False, error_message="failed")
    execution.executed_at.FromJsonString("1970-01-01T00:00:00.123456789Z")
    execution.message.CopyFrom(message_pb2.Message(message_id="msg"))
    response = ResponseWrapper(
        rpc.GetScheduleHistoryResponse(schedule_history=history, next_page_token="next")
    ).to_model()
    assert response.next_page_token == "next"
    assert response.schedule_history.created_at == "1970-01-01T00:00:00Z"
    assert response.schedule_history.updated_at.endswith("000000001Z")
    entry = response.schedule_history.executions[0]
    assert entry.executed_at.endswith("123456789Z")
    assert entry.message_id == entry.message.message_id == "msg"
    assert entry.error_message == "failed" and entry.success is False
    preview = rpc.PreviewCalendarScheduleResponse(timezone="UTC", total_count=1)
    preview.preview_start.FromJsonString("1970-01-01T00:00:00Z")
    preview.execution_times.add().FromJsonString("1970-01-01T00:00:00.000000001Z")
    model = ResponseWrapper(preview).to_model()
    assert model.preview_start.endswith("00Z") and model.total_count == 1 and model.timezone == "UTC"
    assert model.execution_times[0].endswith("000000001Z")
    validation = rpc.ValidateCalendarScheduleResponse(valid=False, error_message="bad")
    validation.validation_issues.add(severity="error", rule_index=-1, field="timezone", message="bad", suggestion="UTC")
    assert ResponseWrapper(validation).to_model().validation_issues[0].rule_index == -1


@pytest.mark.skipif(not models.PYDANTIC_AVAILABLE, reason="Pydantic not installed")
def test_ownership_counts_bulk_outcomes_and_unknown_enums():
    response = rpc.GetNextMessageResponse(worker_id="", attempt_id="attempt")
    response.message.metadata.current_attempt.worker_id = "worker"
    response.message.metadata.current_attempt.lease_started_at.FromJsonString("1970-01-01T00:00:00Z")
    response.message.metadata.state = 999
    model = ResponseWrapper(response).to_model()
    assert model.worker_id == "" and model.attempt_id == "attempt"
    assert model.message.metadata.current_attempt.lease_started_at.endswith("00Z")
    assert model.message.metadata.state == 999
    assert ResponseWrapper(rpc.GetNextMessageResponse()).to_model().worker_id is None
    count = 2**63 - 1
    assert (
        ResponseWrapper(rpc.GetQueueStateResponse(state_counts={"PENDING": count})).to_model().state_counts["PENDING"]
        == count
    )
    assert ResponseWrapper(rpc.GetDLQStatsResponse(message_count=count)).to_model().message_count == count
    response = rpc.PostMessagesBulkResponse(success=True, successful_count=1, failed_count=1)
    response.results.add(message_id="a", success=True)
    response.results.add(message_id="b", error="duplicate", error_code=2)
    bulk = ResponseWrapper(response).to_model()
    assert [result.message_id for result in bulk.results] == ["a", "b"]
    assert bulk.results[1].success is False and bulk.results[1].error_code == "DUPLICATE_MESSAGE_ID"
    assert bulk.results[0].error_code == "SUCCESS"
    assert bulk.successful_count == bulk.failed_count == 1
    empty_payload = models.MessagePayload.from_proto(common_pb2.Payload())
    assert empty_payload.data is None


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_page_iterator_is_lazy_and_bounded(asynchronous):
    client = object.__new__(AsyncNzovuClient if asynchronous else NzovuClient)
    responses = [
        ResponseWrapper(rpc.ListQueuesResponse(next_page_token="next")),
        ResponseWrapper(rpc.ListQueuesResponse()),
    ]
    client.list_queues = (AsyncMock if asynchronous else Mock)(side_effect=responses)
    pages = client.iter_pages("list_queues", prefix="orders", page_size=2, max_pages=1)
    client.list_queues.assert_not_called()
    result = [page async for page in pages] if asynchronous else list(pages)
    assert result == responses[:1]
    client.list_queues.assert_called_once_with(prefix="orders", page_size=2, page_token="")


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_page_iterator_continuation_does_not_mutate_params(asynchronous):
    client = object.__new__(AsyncNzovuClient if asynchronous else NzovuClient)
    responses = [
        ResponseWrapper(rpc.PeekQueueMessagesResponse(next_page_token="next")),
        ResponseWrapper(rpc.PeekQueueMessagesResponse()),
    ]
    client.peek_queue_messages = (AsyncMock if asynchronous else Mock)(side_effect=responses)
    params = PeekQueueMessagesParams("orders")
    pages = client.iter_pages("peek_queue_messages", params)
    result = [page async for page in pages] if asynchronous else list(pages)
    assert result == responses and params.page_token == ""
    assert client.peek_queue_messages.call_args.kwargs["params"].page_token == "next"


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("page_size", [-1, 1001, True, 1.5])
async def test_page_size_boundaries_rejected_before_rpc(asynchronous, page_size):
    client = object.__new__(AsyncNzovuClient if asynchronous else NzovuClient)
    client.stub = AsyncMock() if asynchronous else Mock()
    with pytest.raises(ValueError, match="page_size"):
        result = client.list_queues(page_size=page_size)
        if asynchronous:
            await result
    client.stub.ListQueues.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_page_iterator_detects_repeated_cursor(asynchronous):
    client = object.__new__(AsyncNzovuClient if asynchronous else NzovuClient)
    response = ResponseWrapper(rpc.ListQueuesResponse(next_page_token="same"))
    client.list_queues = (AsyncMock if asynchronous else Mock)(return_value=response)
    with pytest.raises(ValueError, match="repeated"):
        pages = client.iter_pages("list_queues")
        if asynchronous:
            _ = [page async for page in pages]
        else:
            list(pages)
    assert client.list_queues.call_count == 2


@pytest.mark.parametrize("kwargs", [{"method": "delete_queue"}, {"method": "list_queues", "max_pages": 0}])
def test_invalid_page_iterator(kwargs):
    client = object.__new__(NzovuClient)
    with pytest.raises(ValueError):
        list(client.iter_pages(**kwargs))


@pytest.mark.parametrize("maximum", [-1, 2**31, True])
def test_invalid_max_renewals(maximum):
    with pytest.raises(ValueError, match="max_renewals"):
        LeasePolicyOptions(max_renewals=maximum)


def test_unset_message_lease_inherits_and_explicit_zero_remains_present():
    request = _create_post_message_request(PostMessageParams("msg", {}, "orders"))
    assert not request.message.metadata.HasField("lease_duration")
    request = _create_post_message_request(
        PostMessageParams("msg", {}, "orders", PostMessageOptions(lease_duration="0s"))
    )
    assert request.message.metadata.HasField("lease_duration")
    with pytest.raises(ValueError, match="precision"):
        string_to_duration("1.000000000000000000000000000000001s")


def test_payload_metadata_rejects_non_string_keys():
    with pytest.raises(ValueError, match="keys"):
        build_payload({}, {1: "bad"})

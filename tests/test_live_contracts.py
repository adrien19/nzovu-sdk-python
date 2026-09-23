"""Installed-package compatibility gates for every RPC on both storage backends."""

import asyncio
import json
import time
from uuid import uuid4

import grpc
import pytest

from nzovu import (
    Header,
    LeasePolicyOptions,
    MessageRetentionPolicy,
    MessageState,
    PeekQueueMessagesParams,
    PostMessageOptions,
    PostMessageParams,
    QueueOptions,
    RetentionMode,
    RpcOperationError,
    ScheduleOptions,
    SchemaOptions,
    TransactionMode,
    models,
)
from tests.test_live_ownership import connect
from tests.test_ownership import invoke


@pytest.fixture(params=[False, True], ids=["sync", "async"])
async def api(live, request):
    client = await connect(live, request.param)
    yield client
    await invoke(client.close)


async def call(api, method, *args, **kwargs):
    result = await invoke(getattr(api, method), *args, **kwargs)
    proto = result.to_proto()
    assert isinstance(result.to_dict(), dict)
    if models.PYDANTIC_AVAILABLE:
        typed = result.to_model()
        assert typed is not None
    else:
        with pytest.raises(ImportError):
            result.to_model()
    return proto


def unique(prefix):
    return prefix + uuid4().hex


async def eventually(operation, predicate, timeout=30):
    deadline = time.monotonic() + timeout
    while True:
        result = await operation()
        if predicate(result):
            return result
        assert time.monotonic() < deadline, f"condition timed out: {result}"
        await asyncio.sleep(0.2)


async def test_live_queue_message_priority_pagination(api):
    prefix = unique("queue_")
    queues = [prefix + str(i) for i in range(3)]
    for queue in queues:
        await call(
            api,
            "create_queue",
            queue,
            QueueOptions(
                lease_policy=LeasePolicyOptions(base_lease="10s", max_extension="30s", extend_step="1s"),
                retention_policy=MessageRetentionPolicy(RetentionMode.RETAIN_FOREVER),
            ),
        )
    first = await call(api, "list_queues", prefix=prefix, page_size=1)
    assert len(first.queues) == 1 and first.next_page_token
    second = await call(api, "list_queues", prefix=prefix, page_size=1, page_token=first.next_page_token)
    assert second.queues[0].name != first.queues[0].name
    queue = queues[0]
    for priority in (0, 2, 4):
        await call(
            api,
            "post_message",
            PostMessageParams(
                f"priority_{priority}",
                {"priority": priority},
                queue,
                PostMessageOptions(
                    priority=priority, headers=[Header("trace", b"\x00\xff")], data_metadata={"nested": {"ok": True}}
                ),
            ),
        )
    peek = await call(api, "peek_queue_messages", PeekQueueMessagesParams(queue, page_size=1))
    assert len(peek.messages) == 1 and peek.next_page_token
    for priority in (4, 2, 0):
        response = await invoke(api.get_next_message, queue, "10s")
        owner = response.claim
        message = response.to_proto().message
        assert message.message_id == f"priority_{priority}"
        assert message.metadata.headers[0].value == b"\x00\xff"
        assert response.to_dict()["workerId"] == owner.worker_id
        await call(api, "send_message_heartbeat", **owner.to_dict())
        await call(api, "renew_message_lease", **owner.to_dict(), new_lease_duration="1s")
        assert (await call(api, "acknowledge_message", owner.acknowledge(MessageState.COMPLETED))).success
    await call(api, "post_message", PostMessageParams("cancel", {}, queue))
    assert (await call(api, "cancel_message", queue, "cancel", reason="fixture")).success
    state = await call(api, "get_queue_state", queue)
    assert state is not None
    for queue in queues:
        await call(api, "delete_queue", queue)
        await call(api, "delete_queue", queue + "_dlq")
    assert not (await call(api, "list_queues", prefix=prefix)).queues
    with pytest.raises(RpcOperationError) as error:
        await call(api, "get_queue_state", queues[0])
    assert error.value.code() == grpc.StatusCode.NOT_FOUND


async def test_live_bulk_atomic_and_partial(api):
    queue = unique("bulk_")
    await call(api, "create_queue", queue)
    await call(api, "post_message", PostMessageParams("duplicate", {}, queue))
    atomic = [PostMessageParams("atomic_new", {}, queue), PostMessageParams("duplicate", {}, queue)]
    with pytest.raises(RpcOperationError):
        await call(api, "post_messages_bulk", queue, atomic, TransactionMode.ALL_OR_NOTHING)
    peek = await call(api, "peek_queue_messages", PeekQueueMessagesParams(queue))
    assert [message.message_id for message in peek.messages] == ["duplicate"]
    result = await call(api, "post_messages_bulk", queue, atomic, TransactionMode.BEST_EFFORT)
    assert result.successful_count == 1 and result.failed_count == 1
    assert len(result.results) == 2
    successful = [item for item in result.results if item.success]
    assert successful[0].message_id == "atomic_new"
    await call(api, "delete_queue", queue)


async def test_live_schema_validation_and_versions(api):
    schema = unique("schema_")
    options = SchemaOptions(
        "Order",
        "live fixture",
        json.dumps({"type": "object", "properties": {"value": {"type": "integer"}}, "required": ["value"]}),
    )
    first = await call(api, "register_schema", schema, options)
    second = await call(api, "register_schema", schema, options)
    assert second.version > first.version
    assert (await call(api, "get_schema", schema, first.version)).schema.schema_id == schema
    assert (await call(api, "list_schemas", prefix=schema, page_size=1)).schemas
    assert (await call(api, "validate_payload", schema, '{"value":1}', version=first.version)).valid
    invalid = await call(api, "validate_payload", schema, '{"value":"wrong"}', version=first.version)
    assert not invalid.valid and invalid.errors
    queue = unique("validated_")
    await call(api, "create_queue", queue, QueueOptions(schema_id=schema, schema_required=True))
    await call(
        api,
        "post_message",
        PostMessageParams(
            "valid",
            {"value": 1},
            queue,
            PostMessageOptions(schema_id=schema, schema_version=first.version, content_type="application/json"),
        ),
    )
    with pytest.raises(RpcOperationError) as error:
        await call(api, "post_message", PostMessageParams("invalid", {"value": "wrong"}, queue))
    assert error.value.code() in {grpc.StatusCode.INVALID_ARGUMENT, grpc.StatusCode.FAILED_PRECONDITION}
    await call(api, "delete_queue", queue)
    await call(api, "delete_schema", schema, first.version)
    await call(api, "delete_schema", schema, second.version)


async def test_live_schedules_calendar_history(api):
    queue, schedule = unique("scheduled_"), unique("schedule_")
    await call(api, "create_queue", queue)
    calendar = {
        "type": "DAILY",
        "timezone": "UTC",
        "rules": [{"daily": {"day_interval": 1}, "execution_times": [{"hour": 8}]}],
    }
    assert (await call(api, "validate_calendar_schedule", calendar)).valid
    assert len((await call(api, "preview_calendar_schedule", calendar, count=2)).execution_times) == 2
    await call(api, "create_schedule", schedule, ScheduleOptions({"job": "fixture"}, queue, cron_schedule="@every 1s"))
    assert (await call(api, "get_schedule", schedule)).schedule.schedule_id == schedule
    assert (await call(api, "list_schedules", prefix=schedule, page_size=1)).schedules
    history = await eventually(
        lambda: call(api, "get_schedule_history", schedule, page_size=1),
        lambda result: bool(result.schedule_history.executions),
    )
    assert history.schedule_history.executions[0].success
    await call(api, "pause_schedule", schedule)
    await call(api, "resume_schedule", schedule)
    await call(api, "delete_schedule", schedule)
    await call(api, "delete_queue", queue)


async def test_live_dlq_requeue_delete_purge(api):
    queue = unique("source_")
    dlq = queue + "_dlq"
    await call(api, "create_queue", queue, QueueOptions(max_attempts=1, dead_letter_queue_name=dlq))
    for index in range(3):
        await call(api, "post_message", PostMessageParams(f"failed_{index}", {"index": index}, queue))
        owner = (await invoke(api.get_next_message, queue, "10s")).claim
        await call(api, "acknowledge_message", owner.acknowledge(MessageState.ERRORED))
    result = await eventually(lambda: call(api, "get_dlq_messages", dlq), lambda result: len(result.messages) == 3)
    assert (await call(api, "get_dlq_stats", dlq)).message_count == 3
    await call(api, "requeue_from_dlq", dlq, result.messages[0].message_id, target_queue=queue)
    owner = (await invoke(api.get_next_message, queue, "10s")).claim
    await call(api, "acknowledge_message", owner.acknowledge(MessageState.COMPLETED))
    await call(api, "delete_from_dlq", dlq, result.messages[1].message_id)
    await call(api, "purge_dlq", dlq)
    assert not (await call(api, "get_dlq_messages", dlq)).messages
    await call(api, "delete_queue", queue)
    await call(api, "delete_queue", dlq)

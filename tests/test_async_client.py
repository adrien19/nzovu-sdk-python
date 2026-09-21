"""
Tests for AsyncNzovuClient - async/await version of the client.

These tests verify that all async operations work correctly and match
the functionality of the synchronous client.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from nzovu.async_client import AsyncNzovuClient
from nzovu.utils import (
    AcknowledgeMessageParams,
    LeasePolicyOptions,
    MessageRetentionPolicy,
    PeekQueueMessagesParams,
    PostMessageParams,
    QueueOptions,
    RetentionMode,
    ScheduleOptions,
    SchemaOptions,
    TransactionMode,
)


@pytest.fixture
async def async_client():
    """Create an AsyncNzovuClient instance for testing."""
    client = AsyncNzovuClient(
        host="localhost",
        port=50051,
        use_tls=False,
        heartbeat_max_duration=120,
        heartbeat_max_count=500,
    )
    yield client
    # Cleanup
    if client.channel:
        await client.close()


@pytest.mark.asyncio
async def test_async_client_initialization():
    """Test async client can be initialized without TLS."""
    client = AsyncNzovuClient(
        host="localhost",
        port=50051,
        use_tls=False,
    )
    assert client.host == "localhost"
    assert client.port == 50051
    assert client._use_tls is False
    assert client._heartbeat_max_duration == 300  # default
    assert client._heartbeat_max_count == 1000000000  # default (effectively unlimited)


@pytest.mark.asyncio
async def test_async_client_context_manager():
    """Test async client works as an async context manager."""
    async with AsyncNzovuClient(host="localhost", port=50051, use_tls=False) as client:
        assert client is not None
        assert isinstance(client, AsyncNzovuClient)


@pytest.mark.asyncio
async def test_create_queue_success(async_client):
    """Test create_queue async method."""
    # Mock the stub
    async_client.stub = AsyncMock()
    mock_response = MagicMock()
    async_client.stub.CreateQueue = AsyncMock(return_value=mock_response)

    options = QueueOptions(
        max_attempts=5,
        lease_duration="5m",
        exclusivity_key="test_key",
    )

    response = await async_client.create_queue("test_queue", options)
    assert response is not None
    async_client.stub.CreateQueue.assert_called_once()


@pytest.mark.asyncio
async def test_create_queue_with_retention_duration(async_client):
    """Test create_queue passes retention_policy to the proto request."""

    async_client.stub = AsyncMock()
    async_client.stub.CreateQueue = AsyncMock(return_value=MagicMock())

    options = QueueOptions(
        retention_policy=MessageRetentionPolicy(mode=RetentionMode.RETAIN_DURATION, retention_seconds=86400)
    )
    await async_client.create_queue("test_queue", options)

    call_args = async_client.stub.CreateQueue.call_args[0][0]
    assert call_args.metadata.message_retention_policy.mode == 1  # RETAIN_DURATION
    assert call_args.metadata.message_retention_policy.retention_seconds == 86400


@pytest.mark.asyncio
async def test_create_queue_with_retention_forever(async_client):
    """Test create_queue passes RETAIN_FOREVER mode to the proto request."""
    async_client.stub = AsyncMock()
    async_client.stub.CreateQueue = AsyncMock(return_value=MagicMock())

    options = QueueOptions(retention_policy=MessageRetentionPolicy(mode=RetentionMode.RETAIN_FOREVER))
    await async_client.create_queue("test_queue", options)

    call_args = async_client.stub.CreateQueue.call_args[0][0]
    assert call_args.metadata.message_retention_policy.mode == 2  # RETAIN_FOREVER


@pytest.mark.asyncio
async def test_create_queue_with_lease_policy(async_client):
    """Test create_queue passes lease_policy to the proto request."""
    async_client.stub = AsyncMock()
    async_client.stub.CreateQueue = AsyncMock(return_value=MagicMock())

    options = QueueOptions(lease_policy=LeasePolicyOptions(base_lease="30s", heartbeat_timeout="10s"))
    await async_client.create_queue("test_queue", options)

    call_args = async_client.stub.CreateQueue.call_args[0][0]
    assert call_args.metadata.lease_policy.base_lease.seconds == 30
    assert call_args.metadata.lease_policy.heartbeat_timeout.seconds == 10


@pytest.mark.asyncio
async def test_delete_queue_success(async_client):
    """Test delete_queue async method."""
    async_client.stub = AsyncMock()
    mock_response = MagicMock()
    async_client.stub.DeleteQueue = AsyncMock(return_value=mock_response)

    response = await async_client.delete_queue("test_queue")
    assert response is not None
    async_client.stub.DeleteQueue.assert_called_once()


@pytest.mark.asyncio
async def test_post_message_success(async_client):
    """Test post_message async method."""
    async_client.stub = AsyncMock()
    mock_response = MagicMock()
    async_client.stub.PostMessage = AsyncMock(return_value=mock_response)

    params = PostMessageParams(
        queue_name="test_queue",
        message_id="msg123",
        data={"key": "value"},
    )

    response = await async_client.post_message(params)
    assert response is not None
    async_client.stub.PostMessage.assert_called_once()


@pytest.mark.asyncio
async def test_acknowledge_message_success(async_client):
    """Test acknowledge_message async method."""
    async_client.stub = AsyncMock()
    mock_response = MagicMock()
    async_client.stub.AcknowledgeMessage = AsyncMock(return_value=mock_response)

    params = AcknowledgeMessageParams(
        queue_name="test_queue",
        message_id="msg123",
        state=0,
    )

    response = await async_client.acknowledge_message(params)
    assert response is not None
    async_client.stub.AcknowledgeMessage.assert_called_once()


@pytest.mark.asyncio
async def test_renew_message_lease_success(async_client):
    """Test renew_message_lease async method."""
    async_client.stub = AsyncMock()
    mock_response = MagicMock()
    async_client.stub.RenewMessageLease = AsyncMock(return_value=mock_response)

    response = await async_client.renew_message_lease("my_queue", "msg123", "10m")
    assert response is not None
    async_client.stub.RenewMessageLease.assert_called_once()
    call_args = async_client.stub.RenewMessageLease.call_args[0][0]
    assert call_args.queue_name == "my_queue"
    assert call_args.message_id == "msg123"


@pytest.mark.asyncio
async def test_list_queues_success(async_client):
    """Test list_queues async method."""
    async_client.stub = AsyncMock()
    mock_response = MagicMock()
    async_client.stub.ListQueues = AsyncMock(return_value=mock_response)

    response = await async_client.list_queues(prefix="order_")

    assert response is not None
    async_client.stub.ListQueues.assert_called_once()
    call_args = async_client.stub.ListQueues.call_args[0][0]
    assert call_args.prefix == "order_"


@pytest.mark.asyncio
async def test_preview_calendar_schedule_success(async_client):
    """Test preview_calendar_schedule async method."""
    async_client.stub = AsyncMock()
    mock_response = MagicMock()
    async_client.stub.PreviewCalendarSchedule = AsyncMock(return_value=mock_response)

    calendar_config = {}
    response = await async_client.preview_calendar_schedule(calendar_config, count=5)

    assert response is not None
    async_client.stub.PreviewCalendarSchedule.assert_called_once()
    call_args = async_client.stub.PreviewCalendarSchedule.call_args[0][0]
    assert call_args.count == 5


@pytest.mark.asyncio
async def test_peek_queue_messages_success(async_client):
    """Test peek_queue_messages async method."""
    async_client.stub = AsyncMock()
    mock_response = MagicMock()
    async_client.stub.PeekQueueMessages = AsyncMock(return_value=mock_response)

    params = PeekQueueMessagesParams(
        queue_name="test_queue",
        page_size=10,
        priority_range=None,
    )

    response = await async_client.peek_queue_messages(params)
    assert response is not None
    async_client.stub.PeekQueueMessages.assert_called_once()


@pytest.mark.asyncio
async def test_peek_queue_messages_with_priority_range(async_client):
    """Test peek_queue_messages passes priority_range as proto type."""
    from nzovu.utils import MessagePriorityRange

    async_client.stub = AsyncMock()
    async_client.stub.PeekQueueMessages = AsyncMock(return_value=MagicMock())

    params = PeekQueueMessagesParams(
        queue_name="test_queue",
        page_size=10,
        priority_range=MessagePriorityRange(min=1, max=3),
    )
    await async_client.peek_queue_messages(params)

    call_args = async_client.stub.PeekQueueMessages.call_args[0][0]
    assert call_args.priority_range.min == 1
    assert call_args.priority_range.max == 3


@pytest.mark.asyncio
async def test_get_queue_state_success(async_client):
    """Test get_queue_state async method."""
    async_client.stub = AsyncMock()
    mock_response = MagicMock()
    async_client.stub.GetQueueState = AsyncMock(return_value=mock_response)

    response = await async_client.get_queue_state("test_queue")
    assert response is not None
    async_client.stub.GetQueueState.assert_called_once()


@pytest.mark.asyncio
async def test_send_message_heartbeat_success(async_client):
    """Test send_message_heartbeat async method."""
    async_client.stub = AsyncMock()
    mock_response = MagicMock()
    async_client.stub.SendMessageHeartbeat = AsyncMock(return_value=mock_response)

    response = await async_client.send_message_heartbeat("test_queue", "msg123")
    assert response is not None
    async_client.stub.SendMessageHeartbeat.assert_called_once()


@pytest.mark.asyncio
async def test_create_schedule_success(async_client):
    """Test create_schedule async method."""
    async_client.stub = AsyncMock()
    mock_response = MagicMock()
    async_client.stub.CreateSchedule = AsyncMock(return_value=mock_response)

    options = ScheduleOptions(
        queue_name="test_queue",
        payload={"data": "value"},
        cron_schedule="0 0 * * *",
    )

    response = await async_client.create_schedule("schedule123", options)
    assert response is not None
    async_client.stub.CreateSchedule.assert_called_once()


@pytest.mark.asyncio
async def test_delete_schedule_success(async_client):
    """Test delete_schedule async method."""
    async_client.stub = AsyncMock()
    mock_response = MagicMock()
    async_client.stub.DeleteSchedule = AsyncMock(return_value=mock_response)

    response = await async_client.delete_schedule("schedule123")
    assert response is not None
    async_client.stub.DeleteSchedule.assert_called_once()


@pytest.mark.asyncio
async def test_get_schedule_success(async_client):
    """Test get_schedule async method."""
    async_client.stub = AsyncMock()
    mock_response = MagicMock()
    async_client.stub.GetSchedule = AsyncMock(return_value=mock_response)

    response = await async_client.get_schedule("schedule123")
    assert response is not None
    async_client.stub.GetSchedule.assert_called_once()


@pytest.mark.asyncio
async def test_list_schedules_success(async_client):
    """Test list_schedules async method."""
    async_client.stub = AsyncMock()
    mock_response = MagicMock()
    async_client.stub.ListSchedules = AsyncMock(return_value=mock_response)

    response = await async_client.list_schedules(prefix="daily_")
    assert response is not None
    async_client.stub.ListSchedules.assert_called_once()


@pytest.mark.asyncio
async def test_get_schedule_history_success(async_client):
    """Test get_schedule_history async method."""
    async_client.stub = AsyncMock()
    mock_response = MagicMock()
    async_client.stub.GetScheduleHistory = AsyncMock(return_value=mock_response)

    response = await async_client.get_schedule_history("schedule123", page_size=20)
    assert response is not None
    async_client.stub.GetScheduleHistory.assert_called_once()


@pytest.mark.asyncio
async def test_pause_schedule_success(async_client):
    """Test pause_schedule async method."""
    async_client.stub = AsyncMock()
    mock_response = MagicMock()
    async_client.stub.PauseSchedule = AsyncMock(return_value=mock_response)

    response = await async_client.pause_schedule("schedule123")
    assert response is not None
    async_client.stub.PauseSchedule.assert_called_once()


@pytest.mark.asyncio
async def test_resume_schedule_success(async_client):
    """Test resume_schedule async method."""
    async_client.stub = AsyncMock()
    mock_response = MagicMock()
    async_client.stub.ResumeSchedule = AsyncMock(return_value=mock_response)

    response = await async_client.resume_schedule("schedule123")
    assert response is not None
    async_client.stub.ResumeSchedule.assert_called_once()


@pytest.mark.asyncio
async def test_register_schema_success(async_client):
    """Test register_schema async method."""
    async_client.stub = AsyncMock()
    mock_response = MagicMock()
    async_client.stub.RegisterSchema = AsyncMock(return_value=mock_response)

    options = SchemaOptions(
        name="Test Schema",
        description="A test schema",
        content='{"type": "object"}',
        content_type="json-schema",
    )

    response = await async_client.register_schema("schema123", options)
    assert response is not None
    async_client.stub.RegisterSchema.assert_called_once()


@pytest.mark.asyncio
async def test_get_schema_success(async_client):
    """Test get_schema async method."""
    async_client.stub = AsyncMock()
    mock_response = MagicMock()
    async_client.stub.GetSchema = AsyncMock(return_value=mock_response)

    response = await async_client.get_schema("schema123", version=1)
    assert response is not None
    async_client.stub.GetSchema.assert_called_once()


@pytest.mark.asyncio
async def test_list_schemas_success(async_client):
    """Test list_schemas async method."""
    async_client.stub = AsyncMock()
    mock_response = MagicMock()
    async_client.stub.ListSchemas = AsyncMock(return_value=mock_response)

    response = await async_client.list_schemas(prefix="test_", page_size=50)
    assert response is not None
    async_client.stub.ListSchemas.assert_called_once()


@pytest.mark.asyncio
async def test_delete_schema_success(async_client):
    """Test delete_schema async method."""
    async_client.stub = AsyncMock()
    mock_response = MagicMock()
    async_client.stub.DeleteSchema = AsyncMock(return_value=mock_response)

    response = await async_client.delete_schema("schema123", version=1)
    assert response is not None
    async_client.stub.DeleteSchema.assert_called_once()


@pytest.mark.asyncio
async def test_validate_payload_success(async_client):
    """Test validate_payload async method."""
    async_client.stub = AsyncMock()
    mock_response = MagicMock()
    async_client.stub.ValidatePayload = AsyncMock(return_value=mock_response)

    payload = '{"orderId": "123", "amount": 50.0}'
    response = await async_client.validate_payload("schema123", payload, version=1)
    assert response is not None
    async_client.stub.ValidatePayload.assert_called_once()


@pytest.mark.asyncio
async def test_get_dlq_messages_success(async_client):
    """Test get_dlq_messages async method."""
    async_client.stub = AsyncMock()
    mock_response = MagicMock()
    async_client.stub.GetDLQMessages = AsyncMock(return_value=mock_response)

    response = await async_client.get_dlq_messages("test_dlq", page_size=50)
    assert response is not None
    async_client.stub.GetDLQMessages.assert_called_once()


@pytest.mark.asyncio
async def test_requeue_from_dlq_success(async_client):
    """Test requeue_from_dlq async method."""
    async_client.stub = AsyncMock()
    mock_response = MagicMock()
    async_client.stub.RequeueFromDLQ = AsyncMock(return_value=mock_response)

    response = await async_client.requeue_from_dlq("test_dlq", "msg123", "target_queue")
    assert response is not None
    async_client.stub.RequeueFromDLQ.assert_called_once()


@pytest.mark.asyncio
async def test_delete_from_dlq_success(async_client):
    """Test delete_from_dlq async method."""
    async_client.stub = AsyncMock()
    mock_response = MagicMock()
    async_client.stub.DeleteFromDLQ = AsyncMock(return_value=mock_response)

    response = await async_client.delete_from_dlq("test_dlq", "msg123")
    assert response is not None
    async_client.stub.DeleteFromDLQ.assert_called_once()


@pytest.mark.asyncio
async def test_purge_dlq_success(async_client):
    """Test purge_dlq async method."""
    async_client.stub = AsyncMock()
    mock_response = MagicMock()
    async_client.stub.PurgeDLQ = AsyncMock(return_value=mock_response)

    response = await async_client.purge_dlq("test_dlq")
    assert response is not None
    async_client.stub.PurgeDLQ.assert_called_once()


@pytest.mark.asyncio
async def test_get_dlq_stats_success(async_client):
    """Test get_dlq_stats async method."""
    async_client.stub = AsyncMock()
    mock_response = MagicMock()
    async_client.stub.GetDLQStats = AsyncMock(return_value=mock_response)

    response = await async_client.get_dlq_stats("test_dlq")
    assert response is not None
    async_client.stub.GetDLQStats.assert_called_once()


@pytest.mark.asyncio
async def test_heartbeat_observability(async_client):
    """Test heartbeat observability methods."""
    # Initially no heartbeats
    assert async_client.get_active_heartbeat_count() == 0
    assert async_client.get_heartbeat_stats() == {}
    assert async_client.get_active_heartbeats() == []


@pytest.mark.asyncio
async def test_stop_heartbeat(async_client):
    """Test stop_heartbeat method."""
    # Create a fake heartbeat entry
    msg_id = "test_msg_123"
    async_client._heartbeat_stop_events[msg_id] = asyncio.Event()

    # Stop it
    result = await async_client.stop_heartbeat(msg_id)
    assert result is True

    # Stopping non-existent heartbeat returns False
    result = await async_client.stop_heartbeat("nonexistent")
    assert result is False


@pytest.mark.asyncio
async def test_cancel_message_success(async_client):
    """Test cancel_message async method."""
    async_client.stub = AsyncMock()
    mock_response = MagicMock()
    mock_response.success = True
    async_client.stub.CancelMessage = AsyncMock(return_value=mock_response)

    response = await async_client.cancel_message("test_queue", "msg-123", "Order cancelled")
    assert response is not None
    async_client.stub.CancelMessage.assert_called_once()

    # Verify the request was created with correct parameters
    call_args = async_client.stub.CancelMessage.call_args
    request = call_args[0][0]
    assert request.queue_name == "test_queue"
    assert request.message_id == "msg-123"
    assert request.reason == "Order cancelled"


@pytest.mark.asyncio
async def test_cancel_message_without_reason(async_client):
    """Test cancel_message without optional reason."""
    async_client.stub = AsyncMock()
    mock_response = MagicMock()
    mock_response.success = True
    async_client.stub.CancelMessage = AsyncMock(return_value=mock_response)

    response = await async_client.cancel_message("test_queue", "msg-456")
    assert response is not None
    async_client.stub.CancelMessage.assert_called_once()

    # Verify the request was created without reason
    call_args = async_client.stub.CancelMessage.call_args
    request = call_args[0][0]
    assert request.queue_name == "test_queue"
    assert request.message_id == "msg-456"


@pytest.mark.asyncio
async def test_post_messages_bulk_all_or_nothing(async_client):
    """Test post_messages_bulk with ALL_OR_NOTHING transaction mode."""
    from nzovu.api.queueservice.v1 import request_response_pb2

    async_client.stub = AsyncMock()
    mock_response = MagicMock()
    mock_response.success = True
    mock_response.successful_count = 3
    mock_response.failed_count = 0
    async_client.stub.PostMessagesBulk = AsyncMock(return_value=mock_response)

    # Create test messages
    messages = [
        PostMessageParams(message_id="msg1", data={"key": "value1"}, queue_name="test_queue"),
        PostMessageParams(message_id="msg2", data={"key": "value2"}, queue_name="test_queue"),
        PostMessageParams(message_id="msg3", data={"key": "value3"}, queue_name="test_queue"),
    ]

    # Call the client's method
    response = await async_client.post_messages_bulk("test_queue", messages, transaction_mode="ALL_OR_NOTHING")

    # Assert the expected behavior
    assert response is not None
    async_client.stub.PostMessagesBulk.assert_called_once()

    # Verify the request
    call_args = async_client.stub.PostMessagesBulk.call_args
    request = call_args[0][0]
    assert request.queue_name == "test_queue"
    assert request.transaction_mode == request_response_pb2.PostMessagesBulkRequest.ALL_OR_NOTHING
    assert len(request.messages) == 3


@pytest.mark.asyncio
async def test_post_messages_bulk_best_effort(async_client):
    """Test post_messages_bulk with BEST_EFFORT transaction mode."""
    from nzovu.api.queueservice.v1 import request_response_pb2

    async_client.stub = AsyncMock()
    mock_response = MagicMock()
    mock_response.success = True
    mock_response.successful_count = 2
    mock_response.failed_count = 1
    async_client.stub.PostMessagesBulk = AsyncMock(return_value=mock_response)

    # Create test messages
    messages = [
        PostMessageParams(message_id="msg1", data={"key": "value1"}, queue_name="test_queue"),
        PostMessageParams(message_id="msg2", data={"key": "value2"}, queue_name="test_queue"),
        PostMessageParams(message_id="msg3", data={"key": "value3"}, queue_name="test_queue"),
    ]

    # Call the client's method
    response = await async_client.post_messages_bulk("test_queue", messages, transaction_mode="BEST_EFFORT")

    # Assert the expected behavior
    assert response is not None
    async_client.stub.PostMessagesBulk.assert_called_once()

    # Verify the request
    call_args = async_client.stub.PostMessagesBulk.call_args
    request = call_args[0][0]
    assert request.queue_name == "test_queue"
    assert request.transaction_mode == request_response_pb2.PostMessagesBulkRequest.BEST_EFFORT
    assert len(request.messages) == 3


@pytest.mark.asyncio
async def test_post_messages_bulk_invalid_transaction_mode(async_client):
    """Test post_messages_bulk with invalid transaction mode."""
    from nzovu.exceptions import RpcOperationError

    async_client.stub = AsyncMock()

    messages = [
        PostMessageParams(message_id="msg1", data={"key": "value1"}, queue_name="test_queue"),
    ]

    # Should raise error for invalid transaction mode
    with pytest.raises(RpcOperationError):
        await async_client.post_messages_bulk("test_queue", messages, transaction_mode="INVALID_MODE")


@pytest.mark.asyncio
async def test_post_messages_bulk_empty_list(async_client):
    """Test post_messages_bulk with empty message list."""
    async_client.stub = AsyncMock()
    mock_response = MagicMock()
    mock_response.success = True
    mock_response.successful_count = 0
    mock_response.failed_count = 0
    async_client.stub.PostMessagesBulk = AsyncMock(return_value=mock_response)

    # Call with empty list
    response = await async_client.post_messages_bulk("test_queue", [])

    # Should still call the API
    assert response is not None
    async_client.stub.PostMessagesBulk.assert_called_once()


@pytest.mark.asyncio
async def test_post_messages_bulk_with_enum_all_or_nothing(async_client):
    """Test post_messages_bulk with TransactionMode enum (ALL_OR_NOTHING)."""
    from nzovu.api.queueservice.v1 import request_response_pb2

    async_client.stub = AsyncMock()
    mock_response = MagicMock()
    mock_response.success = True
    mock_response.successful_count = 2
    mock_response.failed_count = 0
    async_client.stub.PostMessagesBulk = AsyncMock(return_value=mock_response)

    messages = [
        PostMessageParams(message_id="msg1", data={"key": "value1"}, queue_name="test_queue"),
        PostMessageParams(message_id="msg2", data={"key": "value2"}, queue_name="test_queue"),
    ]

    # Use enum instead of string
    response = await async_client.post_messages_bulk(
        "test_queue", messages, transaction_mode=TransactionMode.ALL_OR_NOTHING
    )

    assert response is not None
    async_client.stub.PostMessagesBulk.assert_called_once()

    # Verify the request used correct enum value
    call_args = async_client.stub.PostMessagesBulk.call_args
    request = call_args[0][0]
    assert request.transaction_mode == request_response_pb2.PostMessagesBulkRequest.ALL_OR_NOTHING


@pytest.mark.asyncio
async def test_post_messages_bulk_with_enum_best_effort(async_client):
    """Test post_messages_bulk with TransactionMode enum (BEST_EFFORT)."""
    from nzovu.api.queueservice.v1 import request_response_pb2

    async_client.stub = AsyncMock()
    mock_response = MagicMock()
    mock_response.success = True
    mock_response.successful_count = 2
    mock_response.failed_count = 0
    async_client.stub.PostMessagesBulk = AsyncMock(return_value=mock_response)

    messages = [
        PostMessageParams(message_id="msg1", data={"key": "value1"}, queue_name="test_queue"),
        PostMessageParams(message_id="msg2", data={"key": "value2"}, queue_name="test_queue"),
    ]

    # Use enum instead of string
    response = await async_client.post_messages_bulk(
        "test_queue", messages, transaction_mode=TransactionMode.BEST_EFFORT
    )

    assert response is not None
    async_client.stub.PostMessagesBulk.assert_called_once()

    # Verify the request used correct enum value
    call_args = async_client.stub.PostMessagesBulk.call_args
    request = call_args[0][0]
    assert request.transaction_mode == request_response_pb2.PostMessagesBulkRequest.BEST_EFFORT

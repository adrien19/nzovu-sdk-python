from unittest.mock import Mock

import grpc
import pytest

from nzovu.api.queueservice.v1 import request_response_pb2, service_pb2_grpc
from nzovu.client import NzovuClient
from nzovu.exceptions import RpcOperationError
from nzovu.utils import (
    AcknowledgeMessageParams,
    MessageRetentionPolicy,
    MessageState,
    PeekQueueMessagesParams,
    PostMessageParams,
    QueueOptions,
    RetentionMode,
    TransactionMode,
)


@pytest.fixture
def mock_client():
    mock_channel = Mock(spec=grpc.Channel)
    mock_channel._channel = Mock()
    mock_channel._channel.check_connectivity_state = Mock()
    client = NzovuClient("localhost", 50051, use_tls=False)
    client.channel = mock_channel
    # Mock the QueueServiceStub
    client.stub = Mock(spec=service_pb2_grpc.QueueServiceStub(mock_channel))
    return client


class MockRpcError(grpc.RpcError):
    def __init__(self, details):
        self._details = details

    def details(self):
        return self._details


def test_create_queue(mock_client: NzovuClient):
    # Mock the gRPC response
    mock_response = request_response_pb2.CreateQueueResponse()
    mock_client.stub.CreateQueue.return_value = mock_response

    # Call the client's method
    response = mock_client.create_queue(name="test_queue")

    # Assert the expected behavior
    mock_client.stub.CreateQueue.assert_called_once()
    assert response.to_proto() == mock_response


def test_create_queue_with_retention_duration(mock_client: NzovuClient):
    mock_response = request_response_pb2.CreateQueueResponse()
    mock_client.stub.CreateQueue.return_value = mock_response

    options = QueueOptions(
        retention_policy=MessageRetentionPolicy(mode=RetentionMode.RETAIN_DURATION, retention_seconds=86400)
    )
    mock_client.create_queue(name="test_queue", options=options)

    call_args = mock_client.stub.CreateQueue.call_args[0][0]
    assert call_args.metadata.message_retention_policy.mode == 1  # RETAIN_DURATION
    assert call_args.metadata.message_retention_policy.retention_seconds == 86400


def test_create_queue_with_retention_forever(mock_client: NzovuClient):
    mock_response = request_response_pb2.CreateQueueResponse()
    mock_client.stub.CreateQueue.return_value = mock_response

    options = QueueOptions(retention_policy=MessageRetentionPolicy(mode=RetentionMode.RETAIN_FOREVER))
    mock_client.create_queue(name="test_queue", options=options)

    call_args = mock_client.stub.CreateQueue.call_args[0][0]
    assert call_args.metadata.message_retention_policy.mode == 2  # RETAIN_FOREVER


def test_create_queue_with_lease_policy(mock_client: NzovuClient):
    from nzovu.utils import LeasePolicyOptions

    mock_response = request_response_pb2.CreateQueueResponse()
    mock_client.stub.CreateQueue.return_value = mock_response

    options = QueueOptions(lease_policy=LeasePolicyOptions(base_lease="30s", heartbeat_timeout="10s"))
    mock_client.create_queue(name="test_queue", options=options)

    call_args = mock_client.stub.CreateQueue.call_args[0][0]
    assert call_args.metadata.lease_policy.base_lease.seconds == 30
    assert call_args.metadata.lease_policy.heartbeat_timeout.seconds == 10


def test_error_scenario(mock_client: NzovuClient):
    # Mock gRPC method to raise an RpcError
    mock_client.stub.CreateQueue.side_effect = MockRpcError("An error occurred")

    # Check if the error handler is properly invoked or the exception is raised
    with pytest.raises(RpcOperationError):
        mock_client.create_queue(name="test_queue")


def test_delete_queue(mock_client: NzovuClient):
    # Mock the gRPC response
    mock_response = request_response_pb2.DeleteQueueResponse()
    mock_client.stub.DeleteQueue.return_value = mock_response

    # Call the client's method
    response = mock_client.delete_queue("test_queue")

    # Assert the expected behavior
    mock_client.stub.DeleteQueue.assert_called_once()
    assert response.to_proto() == mock_response


def test_post_message(mock_client: NzovuClient):
    # Mock the gRPC response
    # mock_response = request_response_pb2.PostMessageResponse(success=True, message_id="12345")
    mock_response = request_response_pb2.PostMessageResponse()
    mock_client.stub.PostMessage.return_value = mock_response

    # create required params
    msg_params = PostMessageParams(
        message_id="message_id",
        data={"user": "test1", "user_id": "test1_userID"},
        queue_name="test_queue",
    )
    response = mock_client.post_message(msg_params=msg_params)

    # Assert the expected behavior
    mock_client.stub.PostMessage.assert_called_once()
    assert response.to_proto() == mock_response


def test_get_next_message(mock_client: NzovuClient):
    # Mock the gRPC response
    mock_response = request_response_pb2.GetNextMessageResponse()
    mock_client.stub.GetNextMessage.return_value = mock_response

    # Call the client's method
    response = mock_client.get_next_message(queue_name="test_queue", lease_duration="60s")

    # Assert the expected behavior
    mock_client.stub.GetNextMessage.assert_called_once()
    assert response.to_proto() == mock_response


def test_heartbeat_management(mock_client: NzovuClient):
    # Mock the gRPC response
    mock_response = request_response_pb2.SendMessageHeartBeatResponse()
    mock_client.stub.SendMessageHeartBeat.return_value = mock_response

    # Test data
    queue_name = "test_queue"
    message_id = "12345"

    # Call the method (Assuming you have a method that handles heartbeats)
    # This might involve threading and time.sleep which could be mocked for unit testing
    response = mock_client.send_message_heartbeat(queue_name, message_id, "attempt", "worker")

    # Assert the expected behavior
    mock_client.stub.SendMessageHeartBeat.assert_called_once()
    assert response.to_proto() == mock_response


def test_acknowledge_message(mock_client: NzovuClient):
    # Mock the gRPC response
    mock_response = request_response_pb2.AcknowledgeMessageResponse()
    mock_client.stub.AcknowledgeMessage.return_value = mock_response

    # Prepare params
    params = AcknowledgeMessageParams(
        message_id="12345",
        queue_name="test_queue",
        state=MessageState.COMPLETED.value,
        worker_id="worker",
        attempt_id="attempt",
    )

    # Call the client's method
    response = mock_client.acknowledge_message(params=params)

    # Assert the expected behavior
    mock_client.stub.AcknowledgeMessage.assert_called_once()
    assert response.to_proto() == mock_response


def test_renew_message_lease(mock_client: NzovuClient):
    # Mock the gRPC response
    mock_response = request_response_pb2.RenewMessageLeaseResponse()
    mock_client.stub.RenewMessageLease.return_value = mock_response

    # Call the client's method
    response = mock_client.renew_message_lease(
        queue_name="test_queue", message_id="12345", new_lease_duration="40s", worker_id="worker", attempt_id="attempt"
    )

    # Assert the expected behavior
    mock_client.stub.RenewMessageLease.assert_called_once()
    call_args = mock_client.stub.RenewMessageLease.call_args[0][0]
    assert call_args.queue_name == "test_queue"
    assert call_args.message_id == "12345"
    assert response.to_proto() == mock_response


def test_list_queues(mock_client: NzovuClient):
    mock_response = request_response_pb2.ListQueuesResponse()
    mock_client.stub.ListQueues.return_value = mock_response

    response = mock_client.list_queues(prefix="order_")

    mock_client.stub.ListQueues.assert_called_once()
    call_args = mock_client.stub.ListQueues.call_args[0][0]
    assert call_args.prefix == "order_"
    assert response.to_proto() == mock_response


def test_peek_queue_messages(mock_client: NzovuClient):
    # Mock the gRPC response
    mock_response = request_response_pb2.PeekQueueMessagesResponse()
    mock_client.stub.PeekQueueMessages.return_value = mock_response

    # Prepare params
    params = PeekQueueMessagesParams(queue_name="test_queue", page_size=5, priority_range=None)

    # Call the client's method
    response = mock_client.peek_queue_messages(params=params)

    # Assert the expected behavior
    mock_client.stub.PeekQueueMessages.assert_called_once()
    assert response.to_proto() == mock_response


def test_peek_queue_messages_with_priority_range(mock_client: NzovuClient):
    from nzovu.utils import MessagePriorityRange

    mock_response = request_response_pb2.PeekQueueMessagesResponse()
    mock_client.stub.PeekQueueMessages.return_value = mock_response

    params = PeekQueueMessagesParams(
        queue_name="test_queue", page_size=5, priority_range=MessagePriorityRange(min=2, max=4)
    )
    mock_client.peek_queue_messages(params=params)

    call_args = mock_client.stub.PeekQueueMessages.call_args[0][0]
    assert call_args.priority_range.min == 2
    assert call_args.priority_range.max == 4


def test_get_queue_state(mock_client: NzovuClient):
    # Mock the gRPC response
    mock_response = request_response_pb2.GetQueueStateResponse()
    mock_client.stub.GetQueueState.return_value = mock_response

    # Call the client's method
    response = mock_client.get_queue_state(queue_name="test_queue")

    # Assert the expected behavior
    mock_client.stub.GetQueueState.assert_called_once()
    assert response.to_proto() == mock_response


def test_close_and_succeed(mock_client: NzovuClient):
    mock_client.close()
    mock_client.channel.close.assert_called_once()


def test_close_channel_already_closed_or_none(mock_client: NzovuClient):
    channel = mock_client.channel
    mock_client.close()
    mock_client.close()
    assert channel.close.call_count == 2
    mock_client.channel = None
    mock_client.close()
    assert channel.close.call_count == 2


def test_close_with_error(mock_client: NzovuClient):
    mock_client.channel.close.side_effect = MockRpcError("Error occurred")
    with pytest.raises(RpcOperationError):
        mock_client.close()
    mock_client.channel.close.assert_called_once()


def test_cancel_message_success(mock_client: NzovuClient):
    """Test cancel_message method."""
    # Mock the gRPC response
    mock_response = request_response_pb2.CancelMessageResponse(success=True)
    mock_client.stub.CancelMessage.return_value = mock_response

    # Call the client's method
    response = mock_client.cancel_message("test_queue", "msg-123", "Order cancelled")

    # Assert the expected behavior
    mock_client.stub.CancelMessage.assert_called_once()
    assert response.to_proto() == mock_response

    # Verify the request was created with correct parameters
    call_args = mock_client.stub.CancelMessage.call_args
    request = call_args[0][0]
    assert request.queue_name == "test_queue"
    assert request.message_id == "msg-123"
    assert request.reason == "Order cancelled"


def test_cancel_message_without_reason(mock_client: NzovuClient):
    """Test cancel_message without optional reason."""
    # Mock the gRPC response
    mock_response = request_response_pb2.CancelMessageResponse(success=True)
    mock_client.stub.CancelMessage.return_value = mock_response

    # Call the client's method without reason
    response = mock_client.cancel_message("test_queue", "msg-456")

    # Assert the expected behavior
    mock_client.stub.CancelMessage.assert_called_once()
    assert response.to_proto() == mock_response

    # Verify the request was created without reason
    call_args = mock_client.stub.CancelMessage.call_args
    request = call_args[0][0]
    assert request.queue_name == "test_queue"
    assert request.message_id == "msg-456"


def test_cancel_message_error(mock_client: NzovuClient):
    """Test cancel_message error handling."""
    # Mock gRPC method to raise an RpcError
    mock_client.stub.CancelMessage.side_effect = MockRpcError("Message not found")

    # Check if the error handler is properly invoked or the exception is raised
    with pytest.raises(RpcOperationError):
        mock_client.cancel_message("test_queue", "msg-999")


def test_post_messages_bulk_all_or_nothing(mock_client: NzovuClient):
    """Test post_messages_bulk with ALL_OR_NOTHING transaction mode."""
    # Mock the gRPC response
    mock_response = request_response_pb2.PostMessagesBulkResponse(
        success=True,
        successful_count=3,
        failed_count=0,
    )
    mock_client.stub.PostMessagesBulk.return_value = mock_response

    # Create test messages
    messages = [
        PostMessageParams(message_id="msg1", data={"key": "value1"}, queue_name="test_queue"),
        PostMessageParams(message_id="msg2", data={"key": "value2"}, queue_name="test_queue"),
        PostMessageParams(message_id="msg3", data={"key": "value3"}, queue_name="test_queue"),
    ]

    # Call the client's method
    response = mock_client.post_messages_bulk("test_queue", messages, transaction_mode="ALL_OR_NOTHING")

    # Assert the expected behavior
    mock_client.stub.PostMessagesBulk.assert_called_once()
    assert response.to_proto() == mock_response

    # Verify the request
    call_args = mock_client.stub.PostMessagesBulk.call_args
    request = call_args[0][0]
    assert request.queue_name == "test_queue"
    assert request.transaction_mode == request_response_pb2.PostMessagesBulkRequest.ALL_OR_NOTHING
    assert len(request.messages) == 3


def test_post_messages_bulk_best_effort(mock_client: NzovuClient):
    """Test post_messages_bulk with BEST_EFFORT transaction mode."""
    # Mock the gRPC response with partial success
    mock_response = request_response_pb2.PostMessagesBulkResponse(
        success=True,
        successful_count=2,
        failed_count=1,
    )
    mock_client.stub.PostMessagesBulk.return_value = mock_response

    # Create test messages
    messages = [
        PostMessageParams(message_id="msg1", data={"key": "value1"}, queue_name="test_queue"),
        PostMessageParams(message_id="msg2", data={"key": "value2"}, queue_name="test_queue"),
        PostMessageParams(message_id="msg3", data={"key": "value3"}, queue_name="test_queue"),
    ]

    # Call the client's method
    response = mock_client.post_messages_bulk("test_queue", messages, transaction_mode="BEST_EFFORT")

    # Assert the expected behavior
    mock_client.stub.PostMessagesBulk.assert_called_once()
    assert response.to_proto() == mock_response

    # Verify the request
    call_args = mock_client.stub.PostMessagesBulk.call_args
    request = call_args[0][0]
    assert request.queue_name == "test_queue"
    assert request.transaction_mode == request_response_pb2.PostMessagesBulkRequest.BEST_EFFORT
    assert len(request.messages) == 3


def test_post_messages_bulk_invalid_transaction_mode(mock_client: NzovuClient):
    """Test post_messages_bulk with invalid transaction mode."""
    messages = [
        PostMessageParams(message_id="msg1", data={"key": "value1"}, queue_name="test_queue"),
    ]

    # Should raise error for invalid transaction mode
    with pytest.raises(RpcOperationError):
        mock_client.post_messages_bulk("test_queue", messages, transaction_mode="INVALID_MODE")


def test_post_messages_bulk_grpc_error(mock_client: NzovuClient):
    """Test post_messages_bulk error handling."""
    # Mock gRPC method to raise an RpcError
    mock_client.stub.PostMessagesBulk.side_effect = MockRpcError("Queue not found")

    messages = [
        PostMessageParams(message_id="msg1", data={"key": "value1"}, queue_name="test_queue"),
    ]

    # Check if the error handler is properly invoked or the exception is raised
    with pytest.raises(RpcOperationError):
        mock_client.post_messages_bulk("test_queue", messages)


def test_post_messages_bulk_empty_list(mock_client: NzovuClient):
    with pytest.raises(RpcOperationError, match="1-1000 messages"):
        mock_client.post_messages_bulk("test_queue", [])
    mock_client.stub.PostMessagesBulk.assert_not_called()


def test_post_messages_bulk_with_enum_all_or_nothing(mock_client: NzovuClient):
    """Test post_messages_bulk with TransactionMode enum (ALL_OR_NOTHING)."""
    mock_response = request_response_pb2.PostMessagesBulkResponse(
        success=True,
        successful_count=2,
        failed_count=0,
    )
    mock_client.stub.PostMessagesBulk.return_value = mock_response

    messages = [
        PostMessageParams(message_id="msg1", data={"key": "value1"}, queue_name="test_queue"),
        PostMessageParams(message_id="msg2", data={"key": "value2"}, queue_name="test_queue"),
    ]

    # Use enum instead of string
    response = mock_client.post_messages_bulk("test_queue", messages, transaction_mode=TransactionMode.ALL_OR_NOTHING)

    mock_client.stub.PostMessagesBulk.assert_called_once()
    assert response.to_proto() == mock_response

    # Verify the request used correct enum value
    call_args = mock_client.stub.PostMessagesBulk.call_args
    request = call_args[0][0]
    assert request.transaction_mode == request_response_pb2.PostMessagesBulkRequest.ALL_OR_NOTHING


def test_post_messages_bulk_with_enum_best_effort(mock_client: NzovuClient):
    """Test post_messages_bulk with TransactionMode enum (BEST_EFFORT)."""
    mock_response = request_response_pb2.PostMessagesBulkResponse(
        success=True,
        successful_count=2,
        failed_count=0,
    )
    mock_client.stub.PostMessagesBulk.return_value = mock_response

    messages = [
        PostMessageParams(message_id="msg1", data={"key": "value1"}, queue_name="test_queue"),
        PostMessageParams(message_id="msg2", data={"key": "value2"}, queue_name="test_queue"),
    ]

    # Use enum instead of string
    response = mock_client.post_messages_bulk("test_queue", messages, transaction_mode=TransactionMode.BEST_EFFORT)

    mock_client.stub.PostMessagesBulk.assert_called_once()
    assert response.to_proto() == mock_response

    # Verify the request used correct enum value
    call_args = mock_client.stub.PostMessagesBulk.call_args
    request = call_args[0][0]
    assert request.transaction_mode == request_response_pb2.PostMessagesBulkRequest.BEST_EFFORT

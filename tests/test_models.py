"""
Unit tests for Pydantic models.

Tests verify that:
1. Proto to Pydantic conversion works correctly
2. Models have correct field types and validation
3. Models can serialize/deserialize properly
4. ResponseWrapper.to_model() works as expected
"""

import pytest
from google.protobuf.struct_pb2 import Struct

from nzovu.api.common.v1 import common_pb2
from nzovu.api.message.v1 import message_pb2
from nzovu.api.queueservice.v1 import request_response_pb2
from nzovu.models import (
    PYDANTIC_AVAILABLE,
    AcknowledgeMessageResponse,
    CreateQueueResponse,
    DeleteQueueResponse,
    GetNextMessageResponse,
    GetQueueStateResponse,
    Message,
    MessageMetadata,
    PeekQueueMessagesResponse,
    PostMessageResponse,
)
from nzovu.utils import ResponseWrapper


@pytest.mark.skipif(not PYDANTIC_AVAILABLE, reason="Pydantic not installed")
class TestPydanticModels:
    """Test suite for Pydantic model conversion from protobuf."""

    def test_create_queue_response_from_proto(self):
        """Test CreateQueueResponse conversion from protobuf."""
        # Create proto response
        proto_response = request_response_pb2.CreateQueueResponse(success=True)

        # Convert to Pydantic model
        model = CreateQueueResponse.from_proto(proto_response)

        # Verify
        assert model.success is True
        assert isinstance(model, CreateQueueResponse)

        # Test serialization
        assert model.model_dump() == {"success": True}

    def test_delete_queue_response_from_proto(self):
        """Test DeleteQueueResponse conversion from protobuf."""
        proto_response = request_response_pb2.DeleteQueueResponse(success=True)
        model = DeleteQueueResponse.from_proto(proto_response)

        assert model.success is True
        assert model.model_dump() == {"success": True}

    def test_post_message_response_from_proto(self):
        """Test PostMessageResponse conversion from protobuf."""
        proto_response = request_response_pb2.PostMessageResponse(success=True)
        model = PostMessageResponse.from_proto(proto_response)

        assert model.success is True

    def test_acknowledge_message_response_from_proto(self):
        """Test AcknowledgeMessageResponse conversion from protobuf."""
        proto_response = request_response_pb2.AcknowledgeMessageResponse(success=True)
        model = AcknowledgeMessageResponse.from_proto(proto_response)

        assert model.success is True

    def test_message_from_proto(self):
        """Test Message model conversion from protobuf."""
        # Create a protobuf Payload
        payload_struct = Struct()
        payload_struct.update({"key": "value", "number": 42})

        proto_payload = common_pb2.Payload(data=payload_struct)

        # Create a protobuf Message.Metadata
        proto_metadata = message_pb2.Message.Metadata(
            payload=proto_payload,
            state=message_pb2.Message.Metadata.State.PENDING,
            priority=5,
            attempts_left=3,
        )

        # Create a protobuf Message
        proto_message = message_pb2.Message(
            message_id="msg-123",
            metadata=proto_metadata,
        )

        # Convert to Pydantic model
        model = Message.from_proto(proto_message)

        # Verify main fields
        assert model.message_id == "msg-123"

        # Verify metadata
        assert model.metadata is not None
        assert model.metadata.priority == 5
        assert model.metadata.attempts_left == 3

        # Verify payload is nested in metadata
        assert model.metadata.payload is not None
        assert model.metadata.payload.data == {"key": "value", "number": 42}

        # Test serialization
        data = model.model_dump()
        assert data["message_id"] == "msg-123"

    def test_get_next_message_response_from_proto(self):
        """Test GetNextMessageResponse with message."""
        # Create a message with payload
        payload_struct = Struct()
        payload_struct.update({"task": "process"})

        proto_metadata = message_pb2.Message.Metadata(
            payload=common_pb2.Payload(data=payload_struct),
            state=message_pb2.Message.Metadata.State.RUNNING,
        )

        proto_message = message_pb2.Message(
            message_id="msg-456",
            metadata=proto_metadata,
        )

        # Create response with message
        proto_response = request_response_pb2.GetNextMessageResponse(message=proto_message)

        # Convert to model
        model = GetNextMessageResponse.from_proto(proto_response)

        # Verify
        assert model.message is not None
        assert model.message.message_id == "msg-456"
        assert model.message.metadata is not None
        assert model.message.metadata.payload is not None
        assert model.message.metadata.payload.data == {"task": "process"}

    def test_get_next_message_response_empty(self):
        """Test GetNextMessageResponse without message (empty queue)."""
        # Create empty response
        proto_response = request_response_pb2.GetNextMessageResponse()

        # Convert to model
        model = GetNextMessageResponse.from_proto(proto_response)

        # Verify message is None
        assert model.message is None

    def test_peek_queue_messages_response(self):
        """Test PeekQueueMessagesResponse with multiple messages."""
        # Create multiple messages
        messages = []
        for i in range(3):
            payload_struct = Struct()
            payload_struct.update({"index": i})

            metadata = message_pb2.Message.Metadata(
                payload=common_pb2.Payload(data=payload_struct),
                state=message_pb2.Message.Metadata.State.PENDING,
            )

            msg = message_pb2.Message(
                message_id=f"msg-{i}",
                metadata=metadata,
            )
            messages.append(msg)

        # Create response
        proto_response = request_response_pb2.PeekQueueMessagesResponse(messages=messages)

        # Convert to model
        model = PeekQueueMessagesResponse.from_proto(proto_response)

        # Verify
        assert len(model.messages) == 3
        assert model.messages[0].message_id == "msg-0"
        assert model.messages[1].message_id == "msg-1"
        assert model.messages[2].message_id == "msg-2"

    def test_get_queue_state_response(self):
        """Test GetQueueStateResponse with state counts."""
        # Create response with state counts
        proto_response = request_response_pb2.GetQueueStateResponse()
        proto_response.state_counts["PENDING"] = 10
        proto_response.state_counts["RUNNING"] = 5
        proto_response.state_counts["COMPLETED"] = 100

        # Convert to model
        model = GetQueueStateResponse.from_proto(proto_response)

        # Verify
        assert model.state_counts["PENDING"] == 10
        assert model.state_counts["RUNNING"] == 5
        assert model.state_counts["COMPLETED"] == 100


class TestResponseWrapper:
    """Test ResponseWrapper.to_model() functionality."""

    @pytest.mark.skipif(not PYDANTIC_AVAILABLE, reason="Pydantic not installed")
    def test_response_wrapper_to_model_create_queue(self):
        """Test ResponseWrapper.to_model() with CreateQueueResponse."""
        proto_response = request_response_pb2.CreateQueueResponse(success=True)
        wrapper = ResponseWrapper(proto_response)

        # Convert to model
        model = wrapper.to_model()

        # Verify correct type and data
        assert isinstance(model, CreateQueueResponse)
        assert model.success is True

    @pytest.mark.skipif(not PYDANTIC_AVAILABLE, reason="Pydantic not installed")
    def test_response_wrapper_to_model_get_next_message(self):
        """Test ResponseWrapper.to_model() with GetNextMessageResponse."""
        # Create message
        payload_struct = Struct()
        payload_struct.update({"data": "test"})

        metadata = message_pb2.Message.Metadata(
            payload=common_pb2.Payload(data=payload_struct),
            state=message_pb2.Message.Metadata.State.RUNNING,
        )

        proto_message = message_pb2.Message(
            message_id="test-msg",
            metadata=metadata,
        )

        proto_response = request_response_pb2.GetNextMessageResponse(message=proto_message)
        wrapper = ResponseWrapper(proto_response)

        # Convert to model
        model = wrapper.to_model()

        # Verify
        assert isinstance(model, GetNextMessageResponse)
        assert model.message is not None
        assert model.message.message_id == "test-msg"

    @pytest.mark.skipif(not PYDANTIC_AVAILABLE, reason="Pydantic not installed")
    def test_response_wrapper_to_model_explicit_class(self):
        """Test ResponseWrapper.to_model() with explicit model class."""
        proto_response = request_response_pb2.DeleteQueueResponse(success=True)
        wrapper = ResponseWrapper(proto_response)

        # Convert with explicit class
        model = wrapper.to_model(DeleteQueueResponse)

        # Verify
        assert isinstance(model, DeleteQueueResponse)
        assert model.success is True

    @pytest.mark.skipif(PYDANTIC_AVAILABLE, reason="Test for when Pydantic not installed")
    def test_response_wrapper_to_model_without_pydantic(self):
        """Test that to_model() raises error when Pydantic not installed."""
        proto_response = request_response_pb2.CreateQueueResponse(success=True)
        wrapper = ResponseWrapper(proto_response)

        with pytest.raises(ImportError) as exc_info:
            wrapper.to_model()

        assert "Pydantic is required" in str(exc_info.value)

    @pytest.mark.skipif(not PYDANTIC_AVAILABLE, reason="Pydantic not installed")
    def test_response_wrapper_backwards_compatibility(self):
        """Test that to_dict() and to_proto() still work with Pydantic available."""
        proto_response = request_response_pb2.CreateQueueResponse(success=True)
        wrapper = ResponseWrapper(proto_response)

        # to_dict() should still work
        dict_data = wrapper.to_dict()
        assert dict_data == {"success": True}

        # to_proto() should still work
        proto_data = wrapper.to_proto()
        assert proto_data == proto_response

        # Direct attribute access should still work
        assert wrapper.success is True


class TestModelValidation:
    """Test Pydantic model validation features."""

    @pytest.mark.skipif(not PYDANTIC_AVAILABLE, reason="Pydantic not installed")
    def test_model_json_serialization(self):
        """Test that models can serialize to JSON."""
        model = CreateQueueResponse(success=True)

        # Serialize to JSON string
        json_str = model.model_dump_json()
        assert '"success":true' in json_str or '"success": true' in json_str

    @pytest.mark.skipif(not PYDANTIC_AVAILABLE, reason="Pydantic not installed")
    def test_model_dict_serialization(self):
        """Test that models can serialize to dict."""
        model = GetQueueStateResponse(state_counts={"PENDING": 5, "RUNNING": 3}, earliest_deadline=None)

        data = model.model_dump()
        assert data["state_counts"]["PENDING"] == 5
        assert data["state_counts"]["RUNNING"] == 3

    @pytest.mark.skipif(not PYDANTIC_AVAILABLE, reason="Pydantic not installed")
    def test_message_metadata_fields(self):
        """Test MessageMetadata has correct fields."""
        metadata = MessageMetadata(state="PENDING", priority=10, attempts_left=3, max_attempts=5, lease_renewal_count=2)

        assert metadata.state == "PENDING"
        assert metadata.priority == 10
        assert metadata.attempts_left == 3
        assert metadata.max_attempts == 5
        assert metadata.lease_renewal_count == 2

        # Test default values
        metadata2 = MessageMetadata(state="RUNNING")
        assert metadata2.priority == 0
        assert metadata2.attempts_left == 0
        assert metadata2.max_attempts == 0

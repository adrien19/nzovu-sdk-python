"""
Unit tests for DLQ Pydantic models.
"""

import unittest

from nzovu.api.message.v1 import message_pb2
from nzovu.api.queueservice.v1 import request_response_pb2

# Test with Pydantic if available
try:
    from nzovu.models import (
        DeleteFromDLQResponse,
        GetDLQMessagesResponse,
        GetDLQStatsResponse,
        Message,
        PurgeDLQResponse,
        RequeueFromDLQResponse,
    )

    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False


@unittest.skipUnless(PYDANTIC_AVAILABLE, "Pydantic not available")
class TestDLQModels(unittest.TestCase):
    """Test suite for DLQ Pydantic models."""

    def test_get_dlq_messages_response_from_proto(self):
        """Test GetDLQMessagesResponse from_proto conversion."""
        proto_message1 = message_pb2.Message(message_id="msg-1")
        proto_message2 = message_pb2.Message(message_id="msg-2")
        proto_response = request_response_pb2.GetDLQMessagesResponse(messages=[proto_message1, proto_message2])

        model = GetDLQMessagesResponse.from_proto(proto_response)

        self.assertIsInstance(model, GetDLQMessagesResponse)
        self.assertEqual(len(model.messages), 2)
        self.assertIsInstance(model.messages[0], Message)
        self.assertEqual(model.messages[0].message_id, "msg-1")
        self.assertEqual(model.messages[1].message_id, "msg-2")

    def test_get_dlq_messages_response_empty_list(self):
        """Test GetDLQMessagesResponse with empty messages list."""
        proto_response = request_response_pb2.GetDLQMessagesResponse(messages=[])

        model = GetDLQMessagesResponse.from_proto(proto_response)

        self.assertEqual(len(model.messages), 0)

    def test_requeue_from_dlq_response_from_proto(self):
        """Test RequeueFromDLQResponse from_proto conversion."""
        proto_response = request_response_pb2.RequeueFromDLQResponse(success=True)

        model = RequeueFromDLQResponse.from_proto(proto_response)

        self.assertIsInstance(model, RequeueFromDLQResponse)
        self.assertTrue(model.success)

    def test_delete_from_dlq_response_from_proto(self):
        """Test DeleteFromDLQResponse from_proto conversion."""
        proto_response = request_response_pb2.DeleteFromDLQResponse(success=True)

        model = DeleteFromDLQResponse.from_proto(proto_response)

        self.assertIsInstance(model, DeleteFromDLQResponse)
        self.assertTrue(model.success)

    def test_purge_dlq_response_from_proto(self):
        """Test PurgeDLQResponse from_proto conversion."""
        proto_response = request_response_pb2.PurgeDLQResponse(success=True)

        model = PurgeDLQResponse.from_proto(proto_response)

        self.assertIsInstance(model, PurgeDLQResponse)
        self.assertTrue(model.success)

    def test_get_dlq_stats_response_from_proto(self):
        """Test GetDLQStatsResponse from_proto conversion."""
        proto_response = request_response_pb2.GetDLQStatsResponse(
            name="orders_queue_dlq", message_count=42, created_at=1234567890, updated_at=1234567900
        )

        model = GetDLQStatsResponse.from_proto(proto_response)

        self.assertIsInstance(model, GetDLQStatsResponse)
        self.assertEqual(model.name, "orders_queue_dlq")
        self.assertEqual(model.message_count, 42)
        self.assertEqual(model.created_at, 1234567890)
        self.assertEqual(model.updated_at, 1234567900)


if __name__ == "__main__":
    unittest.main()

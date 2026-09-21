"""
Unit tests for DLQ operations in NzovuClient.
"""

import unittest
from unittest.mock import MagicMock, patch

from nzovu.api.message.v1 import message_pb2
from nzovu.api.queueservice.v1 import request_response_pb2
from nzovu.client import NzovuClient


class TestDLQOperations(unittest.TestCase):
    """Test suite for DLQ operations."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_stub = MagicMock()
        with patch("nzovu.client.service_pb2_grpc.QueueServiceStub", return_value=self.mock_stub):
            with patch("nzovu.client.grpc.insecure_channel"):
                self.client = NzovuClient(host="localhost", port=50051, use_tls=False)

    def test_get_dlq_messages_default_limit(self):
        """Test getting DLQ messages with default page_size."""
        mock_response = request_response_pb2.GetDLQMessagesResponse(messages=[])
        self.mock_stub.GetDLQMessages.return_value = mock_response

        self.client.get_dlq_messages("orders_queue_dlq")

        self.mock_stub.GetDLQMessages.assert_called_once()
        call_args = self.mock_stub.GetDLQMessages.call_args[0][0]
        self.assertEqual(call_args.dlq_name, "orders_queue_dlq")
        self.assertEqual(call_args.page_size, 100)

    def test_get_dlq_messages_custom_limit(self):
        """Test getting DLQ messages with custom page_size."""
        mock_message = message_pb2.Message(message_id="msg-123")
        mock_response = request_response_pb2.GetDLQMessagesResponse(messages=[mock_message])
        self.mock_stub.GetDLQMessages.return_value = mock_response

        result = self.client.get_dlq_messages("orders_queue_dlq", page_size=50)

        call_args = self.mock_stub.GetDLQMessages.call_args[0][0]
        self.assertEqual(call_args.page_size, 50)
        self.assertIsNotNone(result)

    def test_requeue_from_dlq_to_original(self):
        """Test requeuing message to original queue."""
        mock_response = request_response_pb2.RequeueFromDLQResponse(success=True)
        self.mock_stub.RequeueFromDLQ.return_value = mock_response

        self.client.requeue_from_dlq("orders_queue_dlq", "msg-123")

        self.mock_stub.RequeueFromDLQ.assert_called_once()
        call_args = self.mock_stub.RequeueFromDLQ.call_args[0][0]
        self.assertEqual(call_args.dlq_name, "orders_queue_dlq")
        self.assertEqual(call_args.message_id, "msg-123")
        self.assertEqual(call_args.target_queue, "")

    def test_requeue_from_dlq_to_target_queue(self):
        """Test requeuing message to specific target queue."""
        mock_response = request_response_pb2.RequeueFromDLQResponse(success=True)
        self.mock_stub.RequeueFromDLQ.return_value = mock_response

        self.client.requeue_from_dlq("orders_queue_dlq", "msg-123", target_queue="manual_review_queue")

        call_args = self.mock_stub.RequeueFromDLQ.call_args[0][0]
        self.assertEqual(call_args.target_queue, "manual_review_queue")

    def test_delete_from_dlq(self):
        """Test deleting a message from DLQ."""
        mock_response = request_response_pb2.DeleteFromDLQResponse(success=True)
        self.mock_stub.DeleteFromDLQ.return_value = mock_response

        result = self.client.delete_from_dlq("orders_queue_dlq", "msg-123")

        self.mock_stub.DeleteFromDLQ.assert_called_once()
        call_args = self.mock_stub.DeleteFromDLQ.call_args[0][0]
        self.assertEqual(call_args.dlq_name, "orders_queue_dlq")
        self.assertEqual(call_args.message_id, "msg-123")
        self.assertIsNotNone(result)

    def test_purge_dlq(self):
        """Test purging all messages from DLQ."""
        mock_response = request_response_pb2.PurgeDLQResponse(success=True)
        self.mock_stub.PurgeDLQ.return_value = mock_response

        result = self.client.purge_dlq("orders_queue_dlq")

        self.mock_stub.PurgeDLQ.assert_called_once()
        call_args = self.mock_stub.PurgeDLQ.call_args[0][0]
        self.assertEqual(call_args.dlq_name, "orders_queue_dlq")
        self.assertIsNotNone(result)

    def test_get_dlq_stats(self):
        """Test getting DLQ statistics."""
        mock_response = request_response_pb2.GetDLQStatsResponse(
            name="orders_queue_dlq", message_count=42, created_at=1234567890, updated_at=1234567900
        )
        self.mock_stub.GetDLQStats.return_value = mock_response

        result = self.client.get_dlq_stats("orders_queue_dlq")

        self.mock_stub.GetDLQStats.assert_called_once()
        call_args = self.mock_stub.GetDLQStats.call_args[0][0]
        self.assertEqual(call_args.dlq_name, "orders_queue_dlq")
        self.assertIsNotNone(result)


if __name__ == "__main__":
    unittest.main()

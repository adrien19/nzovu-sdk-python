"""
Unit tests for schema operations in NzovuClient.
"""

import unittest
from unittest.mock import MagicMock, patch

from nzovu.api.queueservice.v1 import request_response_pb2
from nzovu.api.schema.v1 import schema_pb2
from nzovu.client import NzovuClient
from nzovu.utils import SchemaOptions


class TestSchemaOperations(unittest.TestCase):
    """Test suite for schema operations."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_stub = MagicMock()
        with patch("nzovu.client.service_pb2_grpc.QueueServiceStub", return_value=self.mock_stub):
            with patch("nzovu.client.grpc.insecure_channel"):
                self.client = NzovuClient(host="localhost", port=50051, use_tls=False)

    def test_register_schema(self):
        """Test registering a schema."""
        options = SchemaOptions(
            name="Order Schema",
            description="Validation schema for order messages",
            content='{"type": "object", "properties": {"orderId": {"type": "string"}}}',
            content_type="json-schema",
            metadata={"version": "1.0", "author": "team"},
        )

        mock_response = request_response_pb2.RegisterSchemaResponse(
            schema_id="order_schema", version=1, created_at=1234567890
        )
        self.mock_stub.RegisterSchema.return_value = mock_response

        result = self.client.register_schema("order_schema", options)

        self.mock_stub.RegisterSchema.assert_called_once()
        self.assertIsNotNone(result)

        call_args = self.mock_stub.RegisterSchema.call_args[0][0]
        self.assertEqual(call_args.schema_id, "order_schema")
        self.assertEqual(call_args.name, "Order Schema")
        self.assertEqual(call_args.description, "Validation schema for order messages")
        self.assertEqual(call_args.content_type, "json-schema")
        self.assertEqual(call_args.metadata["version"], "1.0")

    def test_register_schema_default_content_type(self):
        """Test registering a schema with default content type."""
        options = SchemaOptions(name="Test Schema", description="Test", content='{"type": "object"}')

        mock_response = request_response_pb2.RegisterSchemaResponse(schema_id="test", version=1)
        self.mock_stub.RegisterSchema.return_value = mock_response

        self.client.register_schema("test_schema", options)

        call_args = self.mock_stub.RegisterSchema.call_args[0][0]
        self.assertEqual(call_args.content_type, "json-schema")

    def test_get_schema_latest_version(self):
        """Test getting latest version of a schema."""
        mock_schema = schema_pb2.Schema(
            schema_id="order_schema",
            version=3,
            name="Order Schema",
            description="Order validation",
            content='{"type": "object"}',
            content_type="json-schema",
            is_active=True,
        )
        mock_response = request_response_pb2.GetSchemaResponse(schema=mock_schema)
        self.mock_stub.GetSchema.return_value = mock_response

        self.client.get_schema("order_schema")

        self.mock_stub.GetSchema.assert_called_once()
        call_args = self.mock_stub.GetSchema.call_args[0][0]
        self.assertEqual(call_args.schema_id, "order_schema")
        self.assertEqual(call_args.version, 0)  # 0 means latest

    def test_get_schema_specific_version(self):
        """Test getting specific version of a schema."""
        mock_schema = schema_pb2.Schema(schema_id="order_schema", version=2)
        mock_response = request_response_pb2.GetSchemaResponse(schema=mock_schema)
        self.mock_stub.GetSchema.return_value = mock_response

        self.client.get_schema("order_schema", version=2)

        call_args = self.mock_stub.GetSchema.call_args[0][0]
        self.assertEqual(call_args.version, 2)

    def test_list_schemas_no_filters(self):
        """Test listing all schemas without filters."""
        mock_response = request_response_pb2.ListSchemasResponse(total_count=0)
        self.mock_stub.ListSchemas.return_value = mock_response

        self.client.list_schemas()

        self.mock_stub.ListSchemas.assert_called_once()
        call_args = self.mock_stub.ListSchemas.call_args[0][0]
        self.assertEqual(call_args.prefix, "")
        self.assertEqual(call_args.page_size, 100)
        self.assertEqual(call_args.active_only, False)

    def test_list_schemas_with_filters(self):
        """Test listing schemas with filters."""
        mock_response = request_response_pb2.ListSchemasResponse(total_count=5)
        self.mock_stub.ListSchemas.return_value = mock_response

        self.client.list_schemas(prefix="order_", page_size=50, active_only=True)

        call_args = self.mock_stub.ListSchemas.call_args[0][0]
        self.assertEqual(call_args.prefix, "order_")
        self.assertEqual(call_args.page_size, 50)
        self.assertEqual(call_args.active_only, True)

    def test_delete_schema_specific_version(self):
        """Test deleting specific schema version."""
        mock_response = request_response_pb2.DeleteSchemaResponse(success=True, versions_deleted=1)
        self.mock_stub.DeleteSchema.return_value = mock_response

        self.client.delete_schema("order_schema", version=1)

        self.mock_stub.DeleteSchema.assert_called_once()
        call_args = self.mock_stub.DeleteSchema.call_args[0][0]
        self.assertEqual(call_args.schema_id, "order_schema")
        self.assertEqual(call_args.version, 1)

    def test_delete_schema_all_versions(self):
        """Test deleting all versions of a schema."""
        mock_response = request_response_pb2.DeleteSchemaResponse(success=True, versions_deleted=3)
        self.mock_stub.DeleteSchema.return_value = mock_response

        self.client.delete_schema("order_schema")

        call_args = self.mock_stub.DeleteSchema.call_args[0][0]
        self.assertEqual(call_args.version, 0)  # 0 means all versions

    def test_validate_payload_valid(self):
        """Test validating a valid payload."""
        import json

        payload = json.dumps({"orderId": "12345", "amount": 99.99})
        mock_response = request_response_pb2.ValidatePayloadResponse(
            valid=True, schema_id="order_schema", schema_version=1
        )
        self.mock_stub.ValidatePayload.return_value = mock_response

        self.client.validate_payload("order_schema", payload)

        self.mock_stub.ValidatePayload.assert_called_once()
        call_args = self.mock_stub.ValidatePayload.call_args[0][0]
        self.assertEqual(call_args.schema_id, "order_schema")
        self.assertEqual(call_args.payload, payload)
        self.assertEqual(call_args.version, 0)  # Latest version

    def test_validate_payload_with_version(self):
        """Test validating payload against specific schema version."""
        import json

        payload = json.dumps({"orderId": "12345"})
        mock_response = request_response_pb2.ValidatePayloadResponse(
            valid=True, schema_id="order_schema", schema_version=2
        )
        self.mock_stub.ValidatePayload.return_value = mock_response

        self.client.validate_payload("order_schema", payload, version=2)

        call_args = self.mock_stub.ValidatePayload.call_args[0][0]
        self.assertEqual(call_args.version, 2)

    def test_validate_payload_with_errors(self):
        """Test validating an invalid payload."""
        import json

        payload = json.dumps({"amount": 99.99})  # Missing orderId

        # Create validation error from schema_pb2
        error = schema_pb2.ValidationError(
            field="orderId", error_code="REQUIRED_FIELD_MISSING", message="Field orderId is required"
        )

        mock_response = request_response_pb2.ValidatePayloadResponse(
            valid=False, errors=[error], schema_id="order_schema", schema_version=1
        )
        self.mock_stub.ValidatePayload.return_value = mock_response

        result = self.client.validate_payload("order_schema", payload)

        self.assertIsNotNone(result)


if __name__ == "__main__":
    unittest.main()

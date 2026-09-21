"""
Unit tests for schema Pydantic models.
"""

import unittest

from nzovu.api.queueservice.v1 import request_response_pb2
from nzovu.api.schema.v1 import schema_pb2

# SchemaInfo is in request_response_pb2
# ValidationError is in schema_pb2
from nzovu.models import (
    PYDANTIC_AVAILABLE,
    DeleteSchemaResponse,
    GetSchemaResponse,
    ListSchemasResponse,
    RegisterSchemaResponse,
    Schema,
    SchemaInfo,
    ValidatePayloadResponse,
    ValidationError,
)
from nzovu.utils import ResponseWrapper


@unittest.skipUnless(PYDANTIC_AVAILABLE, "Pydantic not installed")
class TestSchemaModels(unittest.TestCase):
    """Test suite for schema Pydantic models."""

    def test_schema_from_proto(self):
        """Test Schema model from_proto conversion."""
        proto_schema = schema_pb2.Schema(
            schema_id="order_schema",
            version=1,
            name="Order Schema",
            description="Validation schema for order messages",
            content='{"type": "object", "properties": {"orderId": {"type": "string"}}}',
            content_type="json-schema",
            created_at=1234567890,
            updated_at=1234567900,
            is_active=True,
            metadata={"author": "team", "version": "1.0"},
        )

        model = Schema.from_proto(proto_schema)

        self.assertEqual(model.schema_id, "order_schema")
        self.assertEqual(model.version, 1)
        self.assertEqual(model.name, "Order Schema")
        self.assertEqual(model.description, "Validation schema for order messages")
        self.assertIn("type", model.content)
        self.assertEqual(model.content_type, "json-schema")
        self.assertEqual(model.created_at, 1234567890)
        self.assertEqual(model.updated_at, 1234567900)
        self.assertTrue(model.is_active)
        self.assertEqual(model.metadata["author"], "team")

    def test_schema_info_from_proto(self):
        """Test SchemaInfo model from_proto conversion."""
        proto_info = request_response_pb2.SchemaInfo(
            schema_id="payment_schema",
            name="Payment Schema",
            description="Payment validation",
            is_active=True,
            created_at=1234567890,
            updated_at=1234567900,
            version_count=3,
            latest_version=3,
        )

        model = SchemaInfo.from_proto(proto_info)

        self.assertEqual(model.schema_id, "payment_schema")
        self.assertEqual(model.name, "Payment Schema")
        self.assertEqual(model.description, "Payment validation")
        self.assertTrue(model.is_active)
        self.assertEqual(model.created_at, 1234567890)
        self.assertEqual(model.updated_at, 1234567900)
        self.assertEqual(model.version_count, 3)
        self.assertEqual(model.latest_version, 3)

    def test_validation_error_from_proto(self):
        """Test ValidationError model from_proto conversion."""
        proto_error = schema_pb2.ValidationError(
            field="orderId",
            error_code="REQUIRED_FIELD_MISSING",
            message="Field orderId is required",
            details={"expected": "string"},
        )

        model = ValidationError.from_proto(proto_error)

        self.assertEqual(model.field, "orderId")
        self.assertEqual(model.error_code, "REQUIRED_FIELD_MISSING")
        self.assertEqual(model.message, "Field orderId is required")
        self.assertEqual(model.details["expected"], "string")

    def test_register_schema_response_from_proto(self):
        """Test RegisterSchemaResponse model from_proto conversion."""
        proto_response = request_response_pb2.RegisterSchemaResponse(
            schema_id="order_schema", version=1, created_at=1234567890
        )

        model = RegisterSchemaResponse.from_proto(proto_response)

        self.assertEqual(model.schema_id, "order_schema")
        self.assertEqual(model.version, 1)
        self.assertEqual(model.created_at, 1234567890)

    def test_get_schema_response_from_proto_with_schema(self):
        """Test GetSchemaResponse model from_proto conversion with schema."""
        proto_schema = schema_pb2.Schema(
            schema_id="order_schema",
            version=2,
            name="Order Schema",
            description="Order validation",
            content='{"type": "object"}',
            content_type="json-schema",
            is_active=True,
        )
        proto_response = request_response_pb2.GetSchemaResponse(schema=proto_schema)

        model = GetSchemaResponse.from_proto(proto_response)

        self.assertIsNotNone(model.schema)
        self.assertEqual(model.schema.schema_id, "order_schema")
        self.assertEqual(model.schema.version, 2)

    def test_get_schema_response_from_proto_without_schema(self):
        """Test GetSchemaResponse model from_proto conversion without schema."""
        proto_response = request_response_pb2.GetSchemaResponse()

        model = GetSchemaResponse.from_proto(proto_response)

        self.assertIsNone(model.schema)

    def test_list_schemas_response_from_proto(self):
        """Test ListSchemasResponse model from_proto conversion."""
        proto_info1 = request_response_pb2.SchemaInfo(
            schema_id="schema1",
            name="Schema 1",
            version_count=1,
            latest_version=1,
        )
        proto_info2 = request_response_pb2.SchemaInfo(
            schema_id="schema2",
            name="Schema 2",
            version_count=2,
            latest_version=2,
        )
        proto_response = request_response_pb2.ListSchemasResponse(schemas=[proto_info1, proto_info2], total_count=2)

        model = ListSchemasResponse.from_proto(proto_response)

        self.assertEqual(len(model.schemas), 2)
        self.assertEqual(model.total_count, 2)
        self.assertEqual(model.schemas[0].schema_id, "schema1")
        self.assertEqual(model.schemas[1].schema_id, "schema2")

    def test_delete_schema_response_from_proto(self):
        """Test DeleteSchemaResponse model from_proto conversion."""
        proto_response = request_response_pb2.DeleteSchemaResponse(success=True, versions_deleted=3)

        model = DeleteSchemaResponse.from_proto(proto_response)

        self.assertTrue(model.success)
        self.assertEqual(model.versions_deleted, 3)

    def test_validate_payload_response_from_proto_valid(self):
        """Test ValidatePayloadResponse model from_proto conversion - valid payload."""
        proto_response = request_response_pb2.ValidatePayloadResponse(
            valid=True, schema_id="order_schema", schema_version=1
        )

        model = ValidatePayloadResponse.from_proto(proto_response)

        self.assertTrue(model.valid)
        self.assertEqual(len(model.errors), 0)
        self.assertEqual(model.schema_id, "order_schema")
        self.assertEqual(model.schema_version, 1)

    def test_validate_payload_response_from_proto_with_errors(self):
        """Test ValidatePayloadResponse model from_proto conversion - invalid payload."""
        proto_error1 = schema_pb2.ValidationError(
            field="orderId", error_code="REQUIRED_FIELD_MISSING", message="Field orderId is required"
        )
        proto_error2 = schema_pb2.ValidationError(
            field="amount", error_code="TYPE_MISMATCH", message="Expected number, got string"
        )
        proto_response = request_response_pb2.ValidatePayloadResponse(
            valid=False, errors=[proto_error1, proto_error2], schema_id="order_schema", schema_version=1
        )

        model = ValidatePayloadResponse.from_proto(proto_response)

        self.assertFalse(model.valid)
        self.assertEqual(len(model.errors), 2)
        self.assertEqual(model.errors[0].field, "orderId")
        self.assertEqual(model.errors[1].field, "amount")

    def test_response_wrapper_register_schema(self):
        """Test ResponseWrapper.to_model() for RegisterSchemaResponse."""
        proto_response = request_response_pb2.RegisterSchemaResponse(
            schema_id="test_schema", version=1, created_at=1234567890
        )
        wrapper = ResponseWrapper(proto_response)

        model = wrapper.to_model()

        self.assertIsInstance(model, RegisterSchemaResponse)
        self.assertEqual(model.schema_id, "test_schema")
        self.assertEqual(model.version, 1)

    def test_response_wrapper_list_schemas(self):
        """Test ResponseWrapper.to_model() for ListSchemasResponse."""
        proto_info = request_response_pb2.SchemaInfo(
            schema_id="schema1", name="Schema 1", version_count=1, latest_version=1
        )
        proto_response = request_response_pb2.ListSchemasResponse(schemas=[proto_info], total_count=1)
        wrapper = ResponseWrapper(proto_response)

        model = wrapper.to_model()

        self.assertIsInstance(model, ListSchemasResponse)
        self.assertEqual(len(model.schemas), 1)
        self.assertEqual(model.total_count, 1)

    def test_response_wrapper_validate_payload(self):
        """Test ResponseWrapper.to_model() for ValidatePayloadResponse."""
        proto_response = request_response_pb2.ValidatePayloadResponse(
            valid=True, schema_id="test_schema", schema_version=1
        )
        wrapper = ResponseWrapper(proto_response)

        model = wrapper.to_model()

        self.assertIsInstance(model, ValidatePayloadResponse)
        self.assertTrue(model.valid)


if __name__ == "__main__":
    unittest.main()

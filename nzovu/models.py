"""
Pydantic models for type-safe response handling.

This module provides optional Pydantic models that offer:
- Type safety and IDE autocomplete
- Data validation
- Easy serialization to JSON/dict
- Better developer experience compared to raw protobuf or untyped dicts

All models include a `from_proto()` class method to convert from protobuf responses.

Example:
    >>> response = client.get_next_message("my_queue", "5m")
    >>> msg = response.to_model()  # Returns MessageResponse (typed!)
    >>> print(msg.message_id)  # IDE autocomplete works
    >>> json_str = msg.model_dump_json()  # Easy serialization
"""

try:
    from typing import Any, Dict, List, Optional

    from google.protobuf import json_format
    from pydantic import BaseModel, ConfigDict, Field

    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False
    BaseModel = object  # type: ignore[assignment,misc]  # Fallback for type hints


if PYDANTIC_AVAILABLE:

    class MessageMetadata(BaseModel):
        """
        Metadata associated with a message.

        Proto source: proto/message/v1/message.proto::Message.Metadata
        """

        state: str = Field(..., description="Current state of the message")
        priority: int = Field(default=0, description="Message priority level")
        attempts_left: int = Field(default=0, description="Remaining processing attempts")
        max_attempts: int = Field(default=0, description="Maximum allowed attempts")
        lease_expiry: Optional[int] = Field(None, description="Lease expiration timestamp")
        lease_renewal_count: int = Field(default=0, description="Number of times lease was renewed")
        payload: Optional["MessagePayload"] = Field(None, description="Message payload")

        model_config = ConfigDict(from_attributes=True)

    class MessagePayload(BaseModel):
        """
        Message payload containing the actual data.

        Proto source: proto/common/v1/common.proto::Payload
        """

        data: Dict[str, Any] = Field(default_factory=dict, description="Payload data")
        metadata: Dict[str, Any] = Field(default_factory=dict, description="Payload metadata")

        model_config = ConfigDict(from_attributes=True)

    class Message(BaseModel):
        """
        Complete message structure.

        Proto source: proto/message/v1/message.proto::Message
        """

        message_id: str = Field(..., description="Unique message identifier")
        metadata: Optional[MessageMetadata] = Field(None, description="Message metadata including payload")

        model_config = ConfigDict(from_attributes=True)

        @classmethod
        def from_proto(cls, proto_msg):
            """
            Create a Message from a protobuf Message object.

            Args:
                proto_msg: Protobuf Message object

            Returns:
                Message: Pydantic Message model
            """
            data = json_format.MessageToDict(proto_msg, preserving_proto_field_name=True)

            # Extract metadata
            metadata_data = data.get("metadata", {})

            # Extract payload from metadata
            payload_data = metadata_data.get("payload", {})

            # Create MessagePayload if it exists
            payload_model = None
            if payload_data:
                payload_model = MessagePayload(**payload_data)

            # Create MessageMetadata with payload
            metadata_model = None
            if metadata_data:
                # Remove payload from metadata_data to avoid duplication
                metadata_dict = {k: v for k, v in metadata_data.items() if k != "payload"}
                if payload_model:
                    metadata_dict["payload"] = payload_model
                metadata_model = MessageMetadata(**metadata_dict)

            return cls(
                message_id=data.get("message_id", ""),
                metadata=metadata_model,
            )

    class CreateQueueResponse(BaseModel):
        """
        Response from create_queue operation.

        Proto source: proto/queueservice/v1/request_response.proto::CreateQueueResponse
        """

        success: bool = Field(..., description="Whether queue creation succeeded")

        model_config = ConfigDict(from_attributes=True)

        @classmethod
        def from_proto(cls, proto_response):
            """Create from CreateQueueResponse protobuf."""
            return cls(success=proto_response.success)

    class DeleteQueueResponse(BaseModel):
        """
        Response from delete_queue operation.

        Proto source: proto/queueservice/v1/request_response.proto::DeleteQueueResponse
        """

        success: bool = Field(..., description="Whether queue deletion succeeded")

        model_config = ConfigDict(from_attributes=True)

        @classmethod
        def from_proto(cls, proto_response):
            """Create from DeleteQueueResponse protobuf."""
            return cls(success=proto_response.success)

    class Queue(BaseModel):
        """
        Queue model representing a queue and its metadata.

        Proto source: proto/queue/v1/queue.proto::Queue
        """

        name: str = Field(..., description="Queue name")
        metadata: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Queue metadata configuration")

        model_config = ConfigDict(from_attributes=True)

        @classmethod
        def from_proto(cls, proto_queue):
            """Create from Queue protobuf."""
            data = json_format.MessageToDict(proto_queue, preserving_proto_field_name=True)
            return cls(
                name=data.get("name", ""),
                metadata=data.get("metadata", {}),
            )

    class ListQueuesResponse(BaseModel):
        """
        Response from list_queues operation containing a list of queues.

        Proto source: proto/queueservice/v1/request_response.proto::ListQueuesResponse
        """

        queues: List[Queue] = Field(default_factory=list, description="List of queues")

        next_page_token: str = Field("", description="Continuation token; empty on the final page")

        model_config = ConfigDict(from_attributes=True)

        @classmethod
        def from_proto(cls, proto_response):
            """Create from ListQueuesResponse protobuf."""
            queues = [Queue.from_proto(queue) for queue in proto_response.queues]
            return cls(next_page_token=proto_response.next_page_token, queues=queues)

    class PostMessageResponse(BaseModel):
        """
        Response from post_message operation.

        Proto source: proto/queueservice/v1/request_response.proto::PostMessageResponse
        """

        success: bool = Field(..., description="Whether message posting succeeded")

        model_config = ConfigDict(from_attributes=True)

        @classmethod
        def from_proto(cls, proto_response):
            """Create from PostMessageResponse protobuf."""
            return cls(success=proto_response.success)

    class GetNextMessageResponse(BaseModel):
        """
        Response from get_next_message operation containing the fetched message.

        Proto source: proto/queueservice/v1/request_response.proto::GetNextMessageResponse
        """

        message: Optional[Message] = Field(None, description="The fetched message, if available")

        model_config = ConfigDict(from_attributes=True)

        @classmethod
        def from_proto(cls, proto_response):
            """Create from GetNextMessageResponse protobuf."""
            if proto_response.HasField("message"):
                message = Message.from_proto(proto_response.message)
            else:
                message = None

            return cls(message=message)

    class AcknowledgeMessageResponse(BaseModel):
        """
        Response from acknowledge_message operation.

        Proto source: proto/queueservice/v1/request_response.proto::AcknowledgeMessageResponse
        """

        success: bool = Field(..., description="Whether acknowledgment succeeded")

        model_config = ConfigDict(from_attributes=True)

        @classmethod
        def from_proto(cls, proto_response):
            """Create from AcknowledgeMessageResponse protobuf."""
            return cls(success=proto_response.success)

    class RenewMessageLeaseResponse(BaseModel):
        """
        Response from renew_message_lease operation.

        Proto source: proto/queueservice/v1/request_response.proto::RenewMessageLeaseResponse
        """

        remaining_time: Optional[str] = Field(None, description="Remaining lease time")
        state: str = Field(..., description="Current message state")

        model_config = ConfigDict(from_attributes=True)

        @classmethod
        def from_proto(cls, proto_response):
            """Create from RenewMessageLeaseResponse protobuf."""
            data = json_format.MessageToDict(proto_response, preserving_proto_field_name=True)
            return cls(
                remaining_time=data.get("remaining_time"),
                state=data.get("state", "UNKNOWN"),
            )

    class PeekQueueMessagesResponse(BaseModel):
        """
        Response from peek_queue_messages operation.

        Proto source: proto/queueservice/v1/request_response.proto::PeekQueueMessagesResponse
        """

        messages: List[Message] = Field(default_factory=list, description="List of peeked messages")

        next_page_token: str = Field("", description="Continuation token; empty on the final page")

        model_config = ConfigDict(from_attributes=True)

        @classmethod
        def from_proto(cls, proto_response):
            """Create from PeekQueueMessagesResponse protobuf."""
            messages = [Message.from_proto(msg) for msg in proto_response.messages]
            return cls(next_page_token=proto_response.next_page_token, messages=messages)

    class GetQueueStateResponse(BaseModel):
        """
        Response from get_queue_state operation containing queue statistics.

        Proto source: proto/queueservice/v1/request_response.proto::GetQueueStateResponse
        """

        state_counts: Dict[str, int] = Field(default_factory=dict, description="Count of messages in each state")
        earliest_deadline: Optional[str] = Field(None, description="Earliest message deadline timestamp")

        model_config = ConfigDict(from_attributes=True)

        @classmethod
        def from_proto(cls, proto_response):
            """Create from GetQueueStateResponse protobuf."""
            data = json_format.MessageToDict(proto_response, preserving_proto_field_name=True)
            return cls(
                state_counts=data.get("state_counts", {}),
                earliest_deadline=data.get("earliest_deadline"),
            )

    class SendMessageHeartBeatResponse(BaseModel):
        """
        Response from send_message_heartbeat operation.

        Proto source: proto/queueservice/v1/request_response.proto::SendMessageHeartBeatResponse
        """

        remaining_time: Optional[str] = Field(None, description="Remaining lease time")
        state: str = Field(..., description="Current message state")

        model_config = ConfigDict(from_attributes=True)

        @classmethod
        def from_proto(cls, proto_response):
            """Create from SendMessageHeartBeatResponse protobuf."""
            data = json_format.MessageToDict(proto_response, preserving_proto_field_name=True)
            return cls(
                remaining_time=data.get("remaining_time"),
                state=data.get("state", "UNKNOWN"),
            )

    # Schedule Models

    class ScheduleMetadata(BaseModel):
        """
        Metadata for a schedule.

        Proto source: proto/schedule/v1/schedule.proto::Schedule.Metadata
        """

        payload: Optional[Dict[str, Any]] = Field(None, description="Schedule payload data")
        state: str = Field(..., description="Schedule state (SCHEDULED, CANCELED, ERRORED, PAUSED)")
        cron_schedule: Optional[str] = Field(None, description="Cron expression for schedule")
        calendar_schedule: Optional[Dict[str, Any]] = Field(None, description="Calendar schedule configuration")
        queue_name: str = Field(..., description="Target queue name")
        message_ids: List[str] = Field(default_factory=list, description="Generated message IDs")
        next_run: Optional[str] = Field(None, description="Next scheduled run time")
        last_run: Optional[str] = Field(None, description="Last run time")
        created_at: Optional[str] = Field(None, description="Creation timestamp")
        updated_at: Optional[str] = Field(None, description="Last update timestamp")
        state_message: Optional[str] = Field(None, description="State description/error message")
        priority: int = Field(0, description="Message priority")
        max_messages: Optional[int] = Field(None, description="Max messages per execution")
        lease_duration: Optional[str] = Field(None, description="Message lease duration")
        timezone: Optional[str] = Field(None, description="Schedule timezone")
        next_runs: List[str] = Field(default_factory=list, description="Upcoming run times")

        model_config = ConfigDict(from_attributes=True)

    class Schedule(BaseModel):
        """
        A schedule for automated message posting.

        Proto source: proto/schedule/v1/schedule.proto::Schedule
        """

        schedule_id: str = Field(..., description="Unique schedule identifier")
        metadata: ScheduleMetadata = Field(..., description="Schedule metadata and configuration")

        model_config = ConfigDict(from_attributes=True)

        @classmethod
        def from_proto(cls, proto_schedule):
            """Create from Schedule protobuf."""
            from nzovu.api.schedule.v1 import schedule_pb2

            # Get state enum name
            state_enum = proto_schedule.metadata.state
            state_name = schedule_pb2.Schedule.Metadata.State.Name(state_enum)

            # Extract payload data
            payload = None
            if proto_schedule.metadata.HasField("payload") and proto_schedule.metadata.payload.data:
                from google.protobuf import json_format

                payload = json_format.MessageToDict(proto_schedule.metadata.payload.data)

            # Helper to convert Timestamp to string
            def timestamp_to_str(ts):
                if ts and ts.seconds:
                    return ts.ToJsonString()
                return None

            metadata = ScheduleMetadata(
                payload=payload,
                state=state_name,
                cron_schedule=proto_schedule.metadata.cron_schedule if proto_schedule.metadata.cron_schedule else None,
                calendar_schedule=None,  # TODO: Parse calendar schedule if needed
                queue_name=proto_schedule.metadata.queue_name,
                message_ids=list(proto_schedule.metadata.message_ids),
                next_run=timestamp_to_str(proto_schedule.metadata.next_run),
                last_run=timestamp_to_str(proto_schedule.metadata.last_run),
                created_at=timestamp_to_str(proto_schedule.metadata.created_at),
                updated_at=timestamp_to_str(proto_schedule.metadata.updated_at),
                state_message=proto_schedule.metadata.state_message if proto_schedule.metadata.state_message else None,
                priority=proto_schedule.metadata.priority,
                max_messages=proto_schedule.metadata.max_messages if proto_schedule.metadata.has_max_messages else None,
                lease_duration=(
                    str(proto_schedule.metadata.lease_duration)
                    if proto_schedule.metadata.HasField("lease_duration")
                    else None
                ),
                timezone=proto_schedule.metadata.timezone if proto_schedule.metadata.timezone else None,
                next_runs=list(proto_schedule.metadata.next_runs),
            )

            return cls(
                schedule_id=proto_schedule.schedule_id,
                metadata=metadata,
            )

    class ScheduleHistoryEntry(BaseModel):
        """
        A single execution entry in schedule history.

        Proto source: proto/schedule/v1/schedule.proto::ScheduleHistory
        """

        execution_time: Optional[str] = Field(None, description="When the schedule executed")
        success: bool = Field(False, description="Whether execution succeeded")
        error_message: Optional[str] = Field(None, description="Error message if failed")
        message_ids: List[str] = Field(default_factory=list, description="Created message IDs")

        model_config = ConfigDict(from_attributes=True)

    class CreateScheduleResponse(BaseModel):
        """
        Response from create_schedule operation.

        Proto source: proto/queueservice/v1/request_response.proto::CreateScheduleResponse
        """

        success: bool = Field(True, description="Whether creation succeeded")

        model_config = ConfigDict(from_attributes=True)

        @classmethod
        def from_proto(cls, proto_response):
            """Create from CreateScheduleResponse protobuf."""
            data = json_format.MessageToDict(proto_response, preserving_proto_field_name=True)
            return cls(success=data.get("success", True))

    class DeleteScheduleResponse(BaseModel):
        """
        Response from delete_schedule operation.

        Proto source: proto/queueservice/v1/request_response.proto::DeleteScheduleResponse
        """

        success: bool = Field(True, description="Whether deletion succeeded")

        model_config = ConfigDict(from_attributes=True)

        @classmethod
        def from_proto(cls, proto_response):
            """Create from DeleteScheduleResponse protobuf."""
            return cls(success=True)

    class GetScheduleResponse(BaseModel):
        """
        Response from get_schedule operation.

        Proto source: proto/queueservice/v1/request_response.proto::GetScheduleResponse
        """

        schedule: Optional[Schedule] = Field(None, description="Retrieved schedule")

        model_config = ConfigDict(from_attributes=True)

        @classmethod
        def from_proto(cls, proto_response):
            """Create from GetScheduleResponse protobuf."""
            data = json_format.MessageToDict(proto_response, preserving_proto_field_name=True)
            schedule = None
            if "schedule" in data:
                schedule_proto = proto_response.schedule
                schedule = Schedule.from_proto(schedule_proto)

            return cls(schedule=schedule)

    class ListSchedulesResponse(BaseModel):
        """
        Response from list_schedules operation.

        Proto source: proto/queueservice/v1/request_response.proto::ListSchedulesResponse
        """

        schedules: List[Schedule] = Field(default_factory=list, description="List of schedules")

        next_page_token: str = Field("", description="Continuation token; empty on the final page")

        model_config = ConfigDict(from_attributes=True)

        @classmethod
        def from_proto(cls, proto_response):
            """Create from ListSchedulesResponse protobuf."""
            schedules = []
            for schedule_proto in proto_response.schedules:
                schedules.append(Schedule.from_proto(schedule_proto))

            return cls(next_page_token=proto_response.next_page_token, schedules=schedules)

    class GetScheduleHistoryResponse(BaseModel):
        """
        Response from get_schedule_history operation.

        Proto source: proto/queueservice/v1/request_response.proto::GetScheduleHistoryResponse
        """

        schedule_id: Optional[str] = Field(None, description="Schedule ID")
        messages: List[Message] = Field(default_factory=list, description="Messages created by schedule")
        next_run: Optional[str] = Field(None, description="Next scheduled run")
        last_run: Optional[str] = Field(None, description="Last run time")
        created_at: Optional[str] = Field(None, description="Creation timestamp")

        next_page_token: str = Field("", description="Continuation token; empty on the final page")

        model_config = ConfigDict(from_attributes=True)

        @classmethod
        def from_proto(cls, proto_response):
            """Create from GetScheduleHistoryResponse protobuf."""

            # Helper to convert Timestamp to string
            def timestamp_to_str(ts):
                if ts and ts.seconds:
                    return ts.ToJsonString()
                return None

            messages = []
            if hasattr(proto_response, "schedule_history") and proto_response.schedule_history:
                history = proto_response.schedule_history
                for msg_proto in history.messages:
                    messages.append(Message.from_proto(msg_proto))

                return cls(
                    next_page_token=proto_response.next_page_token,
                    schedule_id=history.schedule_id if history.schedule_id else None,
                    messages=messages,
                    next_run=timestamp_to_str(history.next_run),
                    last_run=timestamp_to_str(history.last_run),
                    created_at=timestamp_to_str(history.created_at),
                )

            return cls(next_page_token=proto_response.next_page_token, messages=[])

    class PauseScheduleResponse(BaseModel):
        """
        Response from pause_schedule operation.

        Proto source: proto/queueservice/v1/request_response.proto::PauseScheduleResponse
        """

        success: bool = Field(True, description="Whether pause succeeded")

        model_config = ConfigDict(from_attributes=True)

        @classmethod
        def from_proto(cls, proto_response):
            """Create from PauseScheduleResponse protobuf."""
            return cls(success=True)

    class ResumeScheduleResponse(BaseModel):
        """
        Response from resume_schedule operation.

        Proto source: proto/queueservice/v1/request_response.proto::ResumeScheduleResponse
        """

        success: bool = Field(True, description="Whether resume succeeded")

        model_config = ConfigDict(from_attributes=True)

        @classmethod
        def from_proto(cls, proto_response):
            """Create from ResumeScheduleResponse protobuf."""
            return cls(success=True)

    class ValidateCalendarScheduleResponse(BaseModel):
        """
        Response from validate_calendar_schedule operation.

        Proto source: proto/queueservice/v1/request_response.proto::ValidateCalendarScheduleResponse
        """

        valid: bool = Field(False, description="Whether the calendar schedule is valid")
        error_message: Optional[str] = Field(None, description="Validation error message if invalid")

        model_config = ConfigDict(from_attributes=True)

        @classmethod
        def from_proto(cls, proto_response):
            """Create from ValidateCalendarScheduleResponse protobuf."""
            data = json_format.MessageToDict(proto_response, preserving_proto_field_name=True)
            return cls(
                valid=data.get("valid", False),
                error_message=data.get("error_message"),
            )

    class PreviewCalendarScheduleResponse(BaseModel):
        """
        Response from preview_calendar_schedule operation.

        Proto source: proto/queueservice/v1/request_response.proto::PreviewCalendarScheduleResponse
        """

        execution_times: List[str] = Field(default_factory=list, description="Upcoming execution times")

        model_config = ConfigDict(from_attributes=True)

        @classmethod
        def from_proto(cls, proto_response):
            """Create from PreviewCalendarScheduleResponse protobuf."""
            data = json_format.MessageToDict(proto_response, preserving_proto_field_name=True)
            return cls(execution_times=data.get("execution_times", []))

    # Schema Models

    class Schema(BaseModel):
        """
        A schema definition for message validation.

        Proto source: proto/schema/v1/schema.proto::Schema
        """

        schema_id: str = Field(..., description="Unique schema identifier")
        version: int = Field(..., description="Schema version number")
        name: str = Field(..., description="Human-readable schema name")
        description: str = Field(..., description="Schema description")
        content: str = Field(..., description="JSON Schema content")
        content_type: str = Field(..., description="Schema type (e.g., json-schema)")
        created_at: Optional[int] = Field(None, description="Creation timestamp (Unix milliseconds)")
        updated_at: Optional[int] = Field(None, description="Last update timestamp (Unix milliseconds)")
        is_active: bool = Field(False, description="Whether this schema version is active")
        metadata: Dict[str, str] = Field(default_factory=dict, description="Additional metadata")

        model_config = ConfigDict(from_attributes=True)

        @classmethod
        def from_proto(cls, proto_schema):
            """Create from Schema protobuf."""
            data = json_format.MessageToDict(proto_schema, preserving_proto_field_name=True)
            return cls(
                schema_id=data.get("schema_id", ""),
                version=data.get("version", 0),
                name=data.get("name", ""),
                description=data.get("description", ""),
                content=data.get("content", ""),
                content_type=data.get("content_type", "json-schema"),
                created_at=data.get("created_at"),
                updated_at=data.get("updated_at"),
                is_active=data.get("is_active", False),
                metadata=data.get("metadata", {}),
            )

    class SchemaInfo(BaseModel):
        """
        Summary information about a schema (used in list responses).

        Proto source: proto/queueservice/v1/request_response.proto::SchemaInfo
        """

        schema_id: str = Field(..., description="Unique schema identifier")
        latest_version: int = Field(..., description="Latest version number")
        name: str = Field(..., description="Schema name")
        description: str = Field(..., description="Schema description")
        created_at: Optional[int] = Field(None, description="Creation timestamp")
        updated_at: Optional[int] = Field(None, description="Last update timestamp")
        version_count: int = Field(0, description="Total number of versions")
        is_active: bool = Field(False, description="Whether schema is active")

        model_config = ConfigDict(from_attributes=True)

        @classmethod
        def from_proto(cls, proto_schema_info):
            """Create from SchemaInfo protobuf."""
            data = json_format.MessageToDict(proto_schema_info, preserving_proto_field_name=True)
            return cls(
                schema_id=data.get("schema_id", ""),
                latest_version=data.get("latest_version", 0),
                name=data.get("name", ""),
                description=data.get("description", ""),
                created_at=data.get("created_at"),
                updated_at=data.get("updated_at"),
                version_count=data.get("version_count", 0),
                is_active=data.get("is_active", False),
            )

    class ValidationError(BaseModel):
        """
        Detailed validation error information.

        Proto source: proto/schema/v1/schema.proto::ValidationError
        """

        field: str = Field(..., description="Field path (e.g., 'payload.data.orderId')")
        error_code: str = Field(..., description="Error code (e.g., REQUIRED_FIELD_MISSING)")
        message: str = Field(..., description="Human-readable error message")
        details: Dict[str, str] = Field(default_factory=dict, description="Additional error details")

        model_config = ConfigDict(from_attributes=True)

        @classmethod
        def from_proto(cls, proto_error):
            """Create from ValidationError protobuf."""
            data = json_format.MessageToDict(proto_error, preserving_proto_field_name=True)
            return cls(
                field=data.get("field", ""),
                error_code=data.get("error_code", ""),
                message=data.get("message", ""),
                details=data.get("details", {}),
            )

    class RegisterSchemaResponse(BaseModel):
        """
        Response from register_schema operation.

        Proto source: proto/queueservice/v1/request_response.proto::RegisterSchemaResponse
        """

        schema_id: str = Field(..., description="Schema identifier")
        version: int = Field(..., description="Assigned schema version")
        created_at: Optional[int] = Field(None, description="Creation timestamp")

        model_config = ConfigDict(from_attributes=True)

        @classmethod
        def from_proto(cls, proto_response):
            """Create from RegisterSchemaResponse protobuf."""
            data = json_format.MessageToDict(proto_response, preserving_proto_field_name=True)
            return cls(
                schema_id=data.get("schema_id", ""),
                version=data.get("version", 0),
                created_at=data.get("created_at"),
            )

    class GetSchemaResponse(BaseModel):
        """
        Response from get_schema operation.

        Proto source: proto/queueservice/v1/request_response.proto::GetSchemaResponse
        """

        schema: Optional[Schema] = Field(None, description="Full schema object")  # type: ignore[assignment]

        model_config = ConfigDict(from_attributes=True)

        @classmethod
        def from_proto(cls, proto_response):
            """Create from GetSchemaResponse protobuf."""
            schema = None
            if proto_response.HasField("schema"):
                schema = Schema.from_proto(proto_response.schema)
            return cls(schema=schema)

    class ListSchemasResponse(BaseModel):
        """
        Response from list_schemas operation.

        Proto source: proto/queueservice/v1/request_response.proto::ListSchemasResponse
        """

        schemas: List[SchemaInfo] = Field(default_factory=list, description="List of schema summaries")
        total_count: int = Field(0, description="Total number of schemas")

        next_page_token: str = Field("", description="Continuation token; empty on the final page")

        model_config = ConfigDict(from_attributes=True)

        @classmethod
        def from_proto(cls, proto_response):
            """Create from ListSchemasResponse protobuf."""
            schemas = []
            for schema_info_proto in proto_response.schemas:
                schemas.append(SchemaInfo.from_proto(schema_info_proto))

            data = json_format.MessageToDict(proto_response, preserving_proto_field_name=True)
            return cls(
                next_page_token=proto_response.next_page_token, schemas=schemas, total_count=data.get("total_count", 0)
            )

    class DeleteSchemaResponse(BaseModel):
        """
        Response from delete_schema operation.

        Proto source: proto/queueservice/v1/request_response.proto::DeleteSchemaResponse
        """

        success: bool = Field(True, description="Whether deletion succeeded")
        versions_deleted: int = Field(0, description="Number of versions deleted")

        model_config = ConfigDict(from_attributes=True)

        @classmethod
        def from_proto(cls, proto_response):
            """Create from DeleteSchemaResponse protobuf."""
            data = json_format.MessageToDict(proto_response, preserving_proto_field_name=True)
            return cls(success=data.get("success", True), versions_deleted=data.get("versions_deleted", 0))

    class ValidatePayloadResponse(BaseModel):
        """
        Response from validate_payload operation.

        Proto source: proto/queueservice/v1/request_response.proto::ValidatePayloadResponse
        """

        valid: bool = Field(False, description="Whether validation passed")
        errors: List[ValidationError] = Field(default_factory=list, description="List of validation errors")
        schema_id: str = Field("", description="Schema used for validation")
        schema_version: int = Field(0, description="Schema version used")

        model_config = ConfigDict(from_attributes=True)

        @classmethod
        def from_proto(cls, proto_response):
            """Create from ValidatePayloadResponse protobuf."""
            errors = []
            for error_proto in proto_response.errors:
                errors.append(ValidationError.from_proto(error_proto))

            data = json_format.MessageToDict(proto_response, preserving_proto_field_name=True)
            return cls(
                valid=data.get("valid", False),
                errors=errors,
                schema_id=data.get("schema_id", ""),
                schema_version=data.get("schema_version", 0),
            )

    # Dead Letter Queue Models

    class GetDLQMessagesResponse(BaseModel):
        """
        Response from get_dlq_messages operation.

        Proto source: proto/queueservice/v1/request_response.proto::GetDLQMessagesResponse
        """

        messages: List[Message] = Field(default_factory=list, description="List of messages in DLQ")

        next_page_token: str = Field("", description="Continuation token; empty on the final page")

        model_config = ConfigDict(from_attributes=True)

        @classmethod
        def from_proto(cls, proto_response):
            """Create from GetDLQMessagesResponse protobuf."""
            messages = []
            for msg_proto in proto_response.messages:
                messages.append(Message.from_proto(msg_proto))
            return cls(next_page_token=proto_response.next_page_token, messages=messages)

    class RequeueFromDLQResponse(BaseModel):
        """
        Response from requeue_from_dlq operation.

        Proto source: proto/queueservice/v1/request_response.proto::RequeueFromDLQResponse
        """

        success: bool = Field(True, description="Whether requeue succeeded")

        model_config = ConfigDict(from_attributes=True)

        @classmethod
        def from_proto(cls, proto_response):
            """Create from RequeueFromDLQResponse protobuf."""
            data = json_format.MessageToDict(proto_response, preserving_proto_field_name=True)
            return cls(success=data.get("success", True))

    class DeleteFromDLQResponse(BaseModel):
        """
        Response from delete_from_dlq operation.

        Proto source: proto/queueservice/v1/request_response.proto::DeleteFromDLQResponse
        """

        success: bool = Field(True, description="Whether deletion succeeded")

        model_config = ConfigDict(from_attributes=True)

        @classmethod
        def from_proto(cls, proto_response):
            """Create from DeleteFromDLQResponse protobuf."""
            data = json_format.MessageToDict(proto_response, preserving_proto_field_name=True)
            return cls(success=data.get("success", True))

    class PurgeDLQResponse(BaseModel):
        """
        Response from purge_dlq operation.

        Proto source: proto/queueservice/v1/request_response.proto::PurgeDLQResponse
        """

        success: bool = Field(True, description="Whether purge succeeded")

        model_config = ConfigDict(from_attributes=True)

        @classmethod
        def from_proto(cls, proto_response):
            """Create from PurgeDLQResponse protobuf."""
            data = json_format.MessageToDict(proto_response, preserving_proto_field_name=True)
            return cls(success=data.get("success", True))

    class GetDLQStatsResponse(BaseModel):
        """
        Response from get_dlq_stats operation.

        Proto source: proto/queueservice/v1/request_response.proto::GetDLQStatsResponse
        """

        name: str = Field("", description="DLQ name")
        message_count: int = Field(0, description="Number of messages in DLQ")
        created_at: Optional[int] = Field(None, description="DLQ creation timestamp")
        updated_at: Optional[int] = Field(None, description="DLQ last update timestamp")

        model_config = ConfigDict(from_attributes=True)

        @classmethod
        def from_proto(cls, proto_response):
            """Create from GetDLQStatsResponse protobuf."""
            data = json_format.MessageToDict(proto_response, preserving_proto_field_name=True)
            return cls(
                name=data.get("name", ""),
                message_count=data.get("message_count", 0),
                created_at=data.get("created_at"),
                updated_at=data.get("updated_at"),
            )

else:
    # Pydantic not available - create placeholder classes
    class CreateQueueResponse:  # type: ignore[no-redef]
        """Pydantic not installed. Install with: pip install pydantic"""

        pass

    class DeleteQueueResponse:  # type: ignore[no-redef]
        """Pydantic not installed. Install with: pip install pydantic"""

        pass

    class Queue:  # type: ignore[no-redef]
        """Pydantic not installed. Install with: pip install pydantic"""

        pass

    class ListQueuesResponse:  # type: ignore[no-redef]
        """Pydantic not installed. Install with: pip install pydantic"""

        pass

    class PostMessageResponse:  # type: ignore[no-redef]
        """Pydantic not installed. Install with: pip install pydantic"""

        pass

    class GetNextMessageResponse:  # type: ignore[no-redef]
        """Pydantic not installed. Install with: pip install pydantic"""

        pass

    class AcknowledgeMessageResponse:  # type: ignore[no-redef]
        """Pydantic not installed. Install with: pip install pydantic"""

        pass

    class RenewMessageLeaseResponse:  # type: ignore[no-redef]
        """Pydantic not installed. Install with: pip install pydantic"""

        pass

    class PeekQueueMessagesResponse:  # type: ignore[no-redef]
        """Pydantic not installed. Install with: pip install pydantic"""

        pass

    class GetQueueStateResponse:  # type: ignore[no-redef]
        """Pydantic not installed. Install with: pip install pydantic"""

        pass

    class SendMessageHeartBeatResponse:  # type: ignore[no-redef]
        """Pydantic not installed. Install with: pip install pydantic"""

        pass

    class Message:  # type: ignore[no-redef]
        """Pydantic not installed. Install with: pip install pydantic"""

        pass

    class MessageMetadata:  # type: ignore[no-redef]
        """Pydantic not installed. Install with: pip install pydantic"""

        pass

    class MessagePayload:  # type: ignore[no-redef]
        """Pydantic not installed. Install with: pip install pydantic"""

        pass

    # Schedule model placeholders
    class Schedule:  # type: ignore[no-redef]
        """Pydantic not installed. Install with: pip install pydantic"""

        pass

    class ScheduleMetadata:  # type: ignore[no-redef]
        """Pydantic not installed. Install with: pip install pydantic"""

        pass

    class ScheduleHistoryEntry:  # type: ignore[no-redef]
        """Pydantic not installed. Install with: pip install pydantic"""

        pass

    class CreateScheduleResponse:  # type: ignore[no-redef]
        """Pydantic not installed. Install with: pip install pydantic"""

        pass

    class DeleteScheduleResponse:  # type: ignore[no-redef]
        """Pydantic not installed. Install with: pip install pydantic"""

        pass

    class GetScheduleResponse:  # type: ignore[no-redef]
        """Pydantic not installed. Install with: pip install pydantic"""

        pass

    class ListSchedulesResponse:  # type: ignore[no-redef]
        """Pydantic not installed. Install with: pip install pydantic"""

        pass

    class GetScheduleHistoryResponse:  # type: ignore[no-redef]
        """Pydantic not installed. Install with: pip install pydantic"""

        pass

    class PauseScheduleResponse:  # type: ignore[no-redef]
        """Pydantic not installed. Install with: pip install pydantic"""

        pass

    class ResumeScheduleResponse:  # type: ignore[no-redef]
        """Pydantic not installed. Install with: pip install pydantic"""

        pass

    class ValidateCalendarScheduleResponse:  # type: ignore[no-redef]
        """Pydantic not installed. Install with: pip install pydantic"""

        pass

    class PreviewCalendarScheduleResponse:  # type: ignore[no-redef]
        """Pydantic not installed. Install with: pip install pydantic"""

        pass

    # Schema model placeholders
    class Schema:  # type: ignore[no-redef]
        """Pydantic not installed. Install with: pip install pydantic"""

        pass

    class SchemaInfo:  # type: ignore[no-redef]
        """Pydantic not installed. Install with: pip install pydantic"""

        pass

    class ValidationError:  # type: ignore[no-redef]
        """Pydantic not installed. Install with: pip install pydantic"""

        pass

    class RegisterSchemaResponse:  # type: ignore[no-redef]
        """Pydantic not installed. Install with: pip install pydantic"""

        pass

    class GetSchemaResponse:  # type: ignore[no-redef]
        """Pydantic not installed. Install with: pip install pydantic"""

        pass

    class ListSchemasResponse:  # type: ignore[no-redef]
        """Pydantic not installed. Install with: pip install pydantic"""

        pass

    class DeleteSchemaResponse:  # type: ignore[no-redef]
        """Pydantic not installed. Install with: pip install pydantic"""

        pass

    class ValidatePayloadResponse:  # type: ignore[no-redef]
        """Pydantic not installed. Install with: pip install pydantic"""

        pass

    # DLQ model placeholders
    class GetDLQMessagesResponse:  # type: ignore[no-redef]
        """Pydantic not installed. Install with: pip install pydantic"""

        pass

    class RequeueFromDLQResponse:  # type: ignore[no-redef]
        """Pydantic not installed. Install with: pip install pydantic"""

        pass

    class DeleteFromDLQResponse:  # type: ignore[no-redef]
        """Pydantic not installed. Install with: pip install pydantic"""

        pass

    class PurgeDLQResponse:  # type: ignore[no-redef]
        """Pydantic not installed. Install with: pip install pydantic"""

        pass

    class GetDLQStatsResponse:  # type: ignore[no-redef]
        """Pydantic not installed. Install with: pip install pydantic"""

        pass


__all__ = [
    "PYDANTIC_AVAILABLE",
    # Queue and Message responses
    "CreateQueueResponse",
    "DeleteQueueResponse",
    "Queue",
    "ListQueuesResponse",
    "PostMessageResponse",
    "GetNextMessageResponse",
    "AcknowledgeMessageResponse",
    "RenewMessageLeaseResponse",
    "PeekQueueMessagesResponse",
    "GetQueueStateResponse",
    "SendMessageHeartBeatResponse",
    # Message models
    "Message",
    "MessageMetadata",
    "MessagePayload",
    # Schedule responses
    "CreateScheduleResponse",
    "DeleteScheduleResponse",
    "GetScheduleResponse",
    "ListSchedulesResponse",
    "GetScheduleHistoryResponse",
    "PauseScheduleResponse",
    "ResumeScheduleResponse",
    "ValidateCalendarScheduleResponse",
    "PreviewCalendarScheduleResponse",
    # Schedule models
    "Schedule",
    "ScheduleMetadata",
    "ScheduleHistoryEntry",
    # Schema responses
    "RegisterSchemaResponse",
    "GetSchemaResponse",
    "ListSchemasResponse",
    "DeleteSchemaResponse",
    "ValidatePayloadResponse",
    # Schema models
    "Schema",
    "SchemaInfo",
    "ValidationError",
    # DLQ responses
    "GetDLQMessagesResponse",
    "RequeueFromDLQResponse",
    "DeleteFromDLQResponse",
    "PurgeDLQResponse",
    "GetDLQStatsResponse",
]

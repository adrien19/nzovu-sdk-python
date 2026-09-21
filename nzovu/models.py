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

    from pydantic import BaseModel, ConfigDict, Field

    from .conversion import protobuf_to_python

    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False
    BaseModel = object  # type: ignore[assignment,misc]  # Fallback for type hints


if PYDANTIC_AVAILABLE:

    class ProtoModel(BaseModel):
        model_config = ConfigDict(from_attributes=True, ser_json_bytes="base64")

        @classmethod
        def from_proto(cls, message):
            return cls.model_validate(protobuf_to_python(message))

    class MessageHeader(ProtoModel):
        key: str
        value: bytes = b""

    class LeasePolicy(ProtoModel):
        base_lease: Optional[str] = None
        max_extension: Optional[str] = None
        heartbeat_timeout: Optional[str] = None
        extend_step: Optional[str] = None
        max_renewals: Optional[int] = None

    class AttemptRuntime(ProtoModel):
        attempt_id: str = ""
        worker_id: str = ""
        lease_started_at: Optional[str] = None
        lease_expiry: int = 0
        lease_extension_used: Optional[str] = None
        lease_renewal_count: int = 0
        last_heartbeat_at: Optional[str] = None
        heartbeat_expiry: int = 0

    class MessagePostResult(ProtoModel):
        message_id: str = ""
        success: bool = False
        error: str = ""
        error_code: str | int = "SUCCESS"

    class PostMessagesBulkResponse(ProtoModel):
        success: bool = False
        successful_count: int = 0
        failed_count: int = 0
        results: List[MessagePostResult] = Field(default_factory=list)

    class CancelMessageResponse(ProtoModel):
        success: bool = False

    class ValidationIssue(ProtoModel):
        severity: str = ""
        rule_index: int = 0
        field: str = ""
        message: str = ""
        suggestion: str = ""

    class MessageMetadata(ProtoModel):
        """
        Metadata associated with a message.

        Proto source: proto/message/v1/message.proto::Message.Metadata
        """

        state: str | int = Field(..., description="Current state of the message")
        priority: int = Field(default=0, description="Message priority level")
        attempts_left: int = Field(default=0, description="Remaining processing attempts")
        max_attempts: int = Field(default=0, description="Maximum allowed attempts")
        lease_expiry: Optional[int] = Field(None, description="Lease expiration timestamp")
        lease_renewal_count: int = Field(default=0, description="Number of times lease was renewed")
        payload: Optional["MessagePayload"] = Field(None, description="Message payload")

        lease_duration: Optional[str] = None
        lease_policy: Optional[LeasePolicy] = None
        current_attempt: Optional[AttemptRuntime] = None
        scheduled_time: Optional[str] = None
        priority_level: int = 0
        headers: List[MessageHeader] = Field(default_factory=list)

        model_config = ConfigDict(from_attributes=True)

    class MessagePayload(ProtoModel):
        """
        Message payload containing the actual data.

        Proto source: proto/common/v1/common.proto::Payload
        """

        data: Optional[Dict[str, Any]] = Field(None, description="Payload data")
        metadata: Dict[str, Any] = Field(default_factory=dict, description="Payload metadata")

        content_type: str = ""
        schema_id: str = ""
        schema_version: int = 0

        model_config = ConfigDict(from_attributes=True)

    class Message(ProtoModel):
        """
        Complete message structure.

        Proto source: proto/message/v1/message.proto::Message
        """

        message_id: str = Field(..., description="Unique message identifier")
        metadata: Optional[MessageMetadata] = Field(None, description="Message metadata including payload")

        model_config = ConfigDict(from_attributes=True)

    class CreateQueueResponse(ProtoModel):
        """
        Response from create_queue operation.

        Proto source: proto/queueservice/v1/request_response.proto::CreateQueueResponse
        """

        success: bool = Field(..., description="Whether queue creation succeeded")

        model_config = ConfigDict(from_attributes=True)

    class DeleteQueueResponse(ProtoModel):
        """
        Response from delete_queue operation.

        Proto source: proto/queueservice/v1/request_response.proto::DeleteQueueResponse
        """

        success: bool = Field(..., description="Whether queue deletion succeeded")

        model_config = ConfigDict(from_attributes=True)

    class Queue(ProtoModel):
        """
        Queue model representing a queue and its metadata.

        Proto source: proto/queue/v1/queue.proto::Queue
        """

        name: str = Field(..., description="Queue name")
        metadata: Optional[Dict[str, Any]] = Field(None, description="Queue metadata configuration")

        model_config = ConfigDict(from_attributes=True)

    class ListQueuesResponse(ProtoModel):
        """
        Response from list_queues operation containing a list of queues.

        Proto source: proto/queueservice/v1/request_response.proto::ListQueuesResponse
        """

        queues: List[Queue] = Field(default_factory=list, description="List of queues")

        next_page_token: str = Field("", description="Continuation token; empty on the final page")

        model_config = ConfigDict(from_attributes=True)

    class PostMessageResponse(ProtoModel):
        """
        Response from post_message operation.

        Proto source: proto/queueservice/v1/request_response.proto::PostMessageResponse
        """

        success: bool = Field(..., description="Whether message posting succeeded")

        model_config = ConfigDict(from_attributes=True)

    class GetNextMessageResponse(ProtoModel):
        """
        Response from get_next_message operation containing the fetched message.

        Proto source: proto/queueservice/v1/request_response.proto::GetNextMessageResponse
        """

        message: Optional[Message] = Field(None, description="The fetched message, if available")

        worker_id: Optional[str] = None
        attempt_id: Optional[str] = None

        model_config = ConfigDict(from_attributes=True)

    class AcknowledgeMessageResponse(ProtoModel):
        """
        Response from acknowledge_message operation.

        Proto source: proto/queueservice/v1/request_response.proto::AcknowledgeMessageResponse
        """

        success: bool = Field(..., description="Whether acknowledgment succeeded")

        model_config = ConfigDict(from_attributes=True)

    class RenewMessageLeaseResponse(ProtoModel):
        """
        Response from renew_message_lease operation.

        Proto source: proto/queueservice/v1/request_response.proto::RenewMessageLeaseResponse
        """

        remaining_time: Optional[str] = Field(None, description="Remaining lease time")
        state: str | int = Field(..., description="Current message state")

        model_config = ConfigDict(from_attributes=True)

    class PeekQueueMessagesResponse(ProtoModel):
        """
        Response from peek_queue_messages operation.

        Proto source: proto/queueservice/v1/request_response.proto::PeekQueueMessagesResponse
        """

        messages: List[Message] = Field(default_factory=list, description="List of peeked messages")

        next_page_token: str = Field("", description="Continuation token; empty on the final page")

        model_config = ConfigDict(from_attributes=True)

    class GetQueueStateResponse(ProtoModel):
        """
        Response from get_queue_state operation containing queue statistics.

        Proto source: proto/queueservice/v1/request_response.proto::GetQueueStateResponse
        """

        state_counts: Dict[str, int] = Field(default_factory=dict, description="Count of messages in each state")
        earliest_deadline: Optional[str] = Field(None, description="Earliest message deadline timestamp")

        model_config = ConfigDict(from_attributes=True)

    class SendMessageHeartBeatResponse(ProtoModel):
        """
        Response from send_message_heartbeat operation.

        Proto source: proto/queueservice/v1/request_response.proto::SendMessageHeartBeatResponse
        """

        remaining_time: Optional[str] = Field(None, description="Remaining lease time")
        state: str | int = Field(..., description="Current message state")

        model_config = ConfigDict(from_attributes=True)

    # Schedule Models

    class ScheduleMetadata(ProtoModel):
        """
        Metadata for a schedule.

        Proto source: proto/schedule/v1/schedule.proto::Schedule.Metadata
        """

        payload: Optional[MessagePayload] = Field(None, description="Schedule payload data and metadata")
        state: str | int = Field(..., description="Schedule state (SCHEDULED, CANCELED, ERRORED, PAUSED)")
        cron_schedule: Optional[str] = Field(None, description="Cron expression for schedule")
        calendar_schedule: Optional[Dict[str, Any]] = Field(None, description="Calendar schedule configuration")
        queue_name: str = Field(..., description="Target queue name")
        message_ids: List[str] = Field(
            default_factory=list, description="Deprecated generated message IDs; use history executions"
        )
        next_run: Optional[str] = Field(None, description="Next scheduled run time")
        last_run: Optional[str] = Field(None, description="Last run time")
        created_at: Optional[str] = Field(None, description="Creation timestamp")
        updated_at: Optional[str] = Field(None, description="Last update timestamp")
        state_message: Optional[str] = Field(None, description="State description/error message")
        priority: int = Field(0, description="Message priority")
        max_messages: Optional[int] = Field(None, description="Max messages per execution")
        lease_duration: Optional[str] = Field(None, description="Message lease duration")
        timezone: Optional[str] = Field(None, description="Deprecated timezone; use calendar_schedule.timezone")
        next_runs: List[str] = Field(default_factory=list, description="Upcoming run times")

        has_max_messages: bool = False
        headers: List[MessageHeader] = Field(default_factory=list)

        model_config = ConfigDict(from_attributes=True)

    class Schedule(ProtoModel):
        """
        A schedule for automated message posting.

        Proto source: proto/schedule/v1/schedule.proto::Schedule
        """

        schedule_id: str = Field(..., description="Unique schedule identifier")
        metadata: Optional[ScheduleMetadata] = Field(None, description="Schedule metadata and configuration")

        model_config = ConfigDict(from_attributes=True)

    class ScheduleHistoryEntry(ProtoModel):
        message_id: str = ""
        executed_at: Optional[str] = None
        success: bool = False
        error_message: str = ""
        message: Optional[Message] = None

    class ScheduleHistory(ProtoModel):
        schedule_id: str = ""
        messages: List[Message] = Field(default_factory=list)
        next_run: Optional[str] = None
        last_run: Optional[str] = None
        created_at: Optional[str] = None
        updated_at: Optional[str] = None
        executions: List[ScheduleHistoryEntry] = Field(default_factory=list)

    class CreateScheduleResponse(ProtoModel):
        """
        Response from create_schedule operation.

        Proto source: proto/queueservice/v1/request_response.proto::CreateScheduleResponse
        """

        success: bool = Field(False, description="Whether creation succeeded")

        model_config = ConfigDict(from_attributes=True)

    class DeleteScheduleResponse(ProtoModel):
        """
        Response from delete_schedule operation.

        Proto source: proto/queueservice/v1/request_response.proto::DeleteScheduleResponse
        """

        success: bool = Field(False, description="Whether deletion succeeded")

        model_config = ConfigDict(from_attributes=True)

    class GetScheduleResponse(ProtoModel):
        """
        Response from get_schedule operation.

        Proto source: proto/queueservice/v1/request_response.proto::GetScheduleResponse
        """

        schedule: Optional[Schedule] = Field(None, description="Retrieved schedule")

        model_config = ConfigDict(from_attributes=True)

    class ListSchedulesResponse(ProtoModel):
        """
        Response from list_schedules operation.

        Proto source: proto/queueservice/v1/request_response.proto::ListSchedulesResponse
        """

        schedules: List[Schedule] = Field(default_factory=list, description="List of schedules")

        next_page_token: str = Field("", description="Continuation token; empty on the final page")

        model_config = ConfigDict(from_attributes=True)

    class GetScheduleHistoryResponse(ProtoModel):
        schedule_history: Optional[ScheduleHistory] = None
        next_page_token: str = ""

    class PauseScheduleResponse(ProtoModel):
        """
        Response from pause_schedule operation.

        Proto source: proto/queueservice/v1/request_response.proto::PauseScheduleResponse
        """

        success: bool = Field(False, description="Whether pause succeeded")

        model_config = ConfigDict(from_attributes=True)

    class ResumeScheduleResponse(ProtoModel):
        """
        Response from resume_schedule operation.

        Proto source: proto/queueservice/v1/request_response.proto::ResumeScheduleResponse
        """

        success: bool = Field(False, description="Whether resume succeeded")

        model_config = ConfigDict(from_attributes=True)

    class ValidateCalendarScheduleResponse(ProtoModel):
        """
        Response from validate_calendar_schedule operation.

        Proto source: proto/queueservice/v1/request_response.proto::ValidateCalendarScheduleResponse
        """

        valid: bool = Field(False, description="Whether the calendar schedule is valid")
        error_message: Optional[str] = Field(None, description="Validation error message if invalid")

        validation_issues: List[ValidationIssue] = Field(default_factory=list)

        model_config = ConfigDict(from_attributes=True)

    class PreviewCalendarScheduleResponse(ProtoModel):
        """
        Response from preview_calendar_schedule operation.

        Proto source: proto/queueservice/v1/request_response.proto::PreviewCalendarScheduleResponse
        """

        execution_times: List[str] = Field(default_factory=list, description="Upcoming execution times")

        timezone: str = ""
        preview_start: Optional[str] = None
        total_count: int = 0

        model_config = ConfigDict(from_attributes=True)

    # Schema Models

    class Schema(ProtoModel):
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

    class SchemaInfo(ProtoModel):
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

    class ValidationError(ProtoModel):
        """
        Detailed validation error information.

        Proto source: proto/schema/v1/schema.proto::ValidationError
        """

        field: str = Field(..., description="Field path (e.g., 'payload.data.orderId')")
        error_code: str = Field(..., description="Error code (e.g., REQUIRED_FIELD_MISSING)")
        message: str = Field(..., description="Human-readable error message")
        details: Dict[str, str] = Field(default_factory=dict, description="Additional error details")

        model_config = ConfigDict(from_attributes=True)

    class RegisterSchemaResponse(ProtoModel):
        """
        Response from register_schema operation.

        Proto source: proto/queueservice/v1/request_response.proto::RegisterSchemaResponse
        """

        schema_id: str = Field(..., description="Schema identifier")
        version: int = Field(..., description="Assigned schema version")
        created_at: Optional[int] = Field(None, description="Creation timestamp")

        model_config = ConfigDict(from_attributes=True)

    class GetSchemaResponse(ProtoModel):
        """
        Response from get_schema operation.

        Proto source: proto/queueservice/v1/request_response.proto::GetSchemaResponse
        """

        schema: Optional[Schema] = Field(None, description="Full schema object")  # type: ignore[assignment]

        model_config = ConfigDict(from_attributes=True)

    class ListSchemasResponse(ProtoModel):
        """
        Response from list_schemas operation.

        Proto source: proto/queueservice/v1/request_response.proto::ListSchemasResponse
        """

        schemas: List[SchemaInfo] = Field(default_factory=list, description="List of schema summaries")
        total_count: int = Field(0, description="Total number of schemas")

        next_page_token: str = Field("", description="Continuation token; empty on the final page")

        model_config = ConfigDict(from_attributes=True)

    class DeleteSchemaResponse(ProtoModel):
        """
        Response from delete_schema operation.

        Proto source: proto/queueservice/v1/request_response.proto::DeleteSchemaResponse
        """

        success: bool = Field(False, description="Whether deletion succeeded")
        versions_deleted: int = Field(0, description="Number of versions deleted")

        model_config = ConfigDict(from_attributes=True)

    class ValidatePayloadResponse(ProtoModel):
        """
        Response from validate_payload operation.

        Proto source: proto/queueservice/v1/request_response.proto::ValidatePayloadResponse
        """

        valid: bool = Field(False, description="Whether validation passed")
        errors: List[ValidationError] = Field(default_factory=list, description="List of validation errors")
        schema_id: str = Field("", description="Schema used for validation")
        schema_version: int = Field(0, description="Schema version used")

        model_config = ConfigDict(from_attributes=True)

    # Dead Letter Queue Models

    class GetDLQMessagesResponse(ProtoModel):
        """
        Response from get_dlq_messages operation.

        Proto source: proto/queueservice/v1/request_response.proto::GetDLQMessagesResponse
        """

        messages: List[Message] = Field(default_factory=list, description="List of messages in DLQ")

        next_page_token: str = Field("", description="Continuation token; empty on the final page")

        model_config = ConfigDict(from_attributes=True)

    class RequeueFromDLQResponse(ProtoModel):
        """
        Response from requeue_from_dlq operation.

        Proto source: proto/queueservice/v1/request_response.proto::RequeueFromDLQResponse
        """

        success: bool = Field(False, description="Whether requeue succeeded")

        model_config = ConfigDict(from_attributes=True)

    class DeleteFromDLQResponse(ProtoModel):
        """
        Response from delete_from_dlq operation.

        Proto source: proto/queueservice/v1/request_response.proto::DeleteFromDLQResponse
        """

        success: bool = Field(False, description="Whether deletion succeeded")

        model_config = ConfigDict(from_attributes=True)

    class PurgeDLQResponse(ProtoModel):
        """
        Response from purge_dlq operation.

        Proto source: proto/queueservice/v1/request_response.proto::PurgeDLQResponse
        """

        success: bool = Field(False, description="Whether purge succeeded")

        model_config = ConfigDict(from_attributes=True)

    class GetDLQStatsResponse(ProtoModel):
        """
        Response from get_dlq_stats operation.

        Proto source: proto/queueservice/v1/request_response.proto::GetDLQStatsResponse
        """

        name: str = Field("", description="DLQ name")
        message_count: int = Field(0, description="Number of messages in DLQ")
        created_at: Optional[int] = Field(None, description="DLQ creation timestamp")
        updated_at: Optional[int] = Field(None, description="DLQ last update timestamp")

        model_config = ConfigDict(from_attributes=True)

else:

    class MessageHeader:  # type: ignore[no-redef]
        def __init__(self, *args, **kwargs):
            raise ImportError("Install nzovu[pydantic] to use typed models")

    class LeasePolicy:  # type: ignore[no-redef]
        def __init__(self, *args, **kwargs):
            raise ImportError("Install nzovu[pydantic] to use typed models")

    class AttemptRuntime:  # type: ignore[no-redef]
        def __init__(self, *args, **kwargs):
            raise ImportError("Install nzovu[pydantic] to use typed models")

    class MessagePostResult:  # type: ignore[no-redef]
        def __init__(self, *args, **kwargs):
            raise ImportError("Install nzovu[pydantic] to use typed models")

    class PostMessagesBulkResponse:  # type: ignore[no-redef]
        def __init__(self, *args, **kwargs):
            raise ImportError("Install nzovu[pydantic] to use typed models")

    class CancelMessageResponse:  # type: ignore[no-redef]
        def __init__(self, *args, **kwargs):
            raise ImportError("Install nzovu[pydantic] to use typed models")

    class ValidationIssue:  # type: ignore[no-redef]
        def __init__(self, *args, **kwargs):
            raise ImportError("Install nzovu[pydantic] to use typed models")

    class ScheduleHistory:  # type: ignore[no-redef]
        def __init__(self, *args, **kwargs):
            raise ImportError("Install nzovu[pydantic] to use typed models")

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
    "MessageHeader",
    "LeasePolicy",
    "AttemptRuntime",
    "MessagePostResult",
    "PostMessagesBulkResponse",
    "CancelMessageResponse",
    "ValidationIssue",
    "ScheduleHistory",
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

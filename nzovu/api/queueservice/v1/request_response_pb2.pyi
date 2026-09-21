from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf import duration_pb2 as _duration_pb2
from nzovu.api.google.api import field_behavior_pb2 as _field_behavior_pb2
from nzovu.api.queue.v1 import queue_pb2 as _queue_pb2
from nzovu.api.message.v1 import message_pb2 as _message_pb2
from nzovu.api.schedule.v1 import schedule_pb2 as _schedule_pb2
from nzovu.api.schema.v1 import schema_pb2 as _schema_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class CreateQueueRequest(_message.Message):
    __slots__ = ("name", "metadata")
    NAME_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    name: str
    metadata: _queue_pb2.QueueMetadata
    def __init__(self, name: _Optional[str] = ..., metadata: _Optional[_Union[_queue_pb2.QueueMetadata, _Mapping]] = ...) -> None: ...

class CreateQueueResponse(_message.Message):
    __slots__ = ("success",)
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    success: bool
    def __init__(self, success: bool = ...) -> None: ...

class DeleteQueueRequest(_message.Message):
    __slots__ = ("name",)
    NAME_FIELD_NUMBER: _ClassVar[int]
    name: str
    def __init__(self, name: _Optional[str] = ...) -> None: ...

class DeleteQueueResponse(_message.Message):
    __slots__ = ("success",)
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    success: bool
    def __init__(self, success: bool = ...) -> None: ...

class PostMessageRequest(_message.Message):
    __slots__ = ("queue_name", "message")
    QUEUE_NAME_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    queue_name: str
    message: _message_pb2.Message
    def __init__(self, queue_name: _Optional[str] = ..., message: _Optional[_Union[_message_pb2.Message, _Mapping]] = ...) -> None: ...

class PostMessageResponse(_message.Message):
    __slots__ = ("success",)
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    success: bool
    def __init__(self, success: bool = ...) -> None: ...

class PostMessagesBulkRequest(_message.Message):
    __slots__ = ("queue_name", "messages", "transaction_mode")
    class TransactionMode(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        ALL_OR_NOTHING: _ClassVar[PostMessagesBulkRequest.TransactionMode]
        BEST_EFFORT: _ClassVar[PostMessagesBulkRequest.TransactionMode]
    ALL_OR_NOTHING: PostMessagesBulkRequest.TransactionMode
    BEST_EFFORT: PostMessagesBulkRequest.TransactionMode
    QUEUE_NAME_FIELD_NUMBER: _ClassVar[int]
    MESSAGES_FIELD_NUMBER: _ClassVar[int]
    TRANSACTION_MODE_FIELD_NUMBER: _ClassVar[int]
    queue_name: str
    messages: _containers.RepeatedCompositeFieldContainer[_message_pb2.Message]
    transaction_mode: PostMessagesBulkRequest.TransactionMode
    def __init__(self, queue_name: _Optional[str] = ..., messages: _Optional[_Iterable[_Union[_message_pb2.Message, _Mapping]]] = ..., transaction_mode: _Optional[_Union[PostMessagesBulkRequest.TransactionMode, str]] = ...) -> None: ...

class PostMessagesBulkResponse(_message.Message):
    __slots__ = ("success", "successful_count", "failed_count", "results")
    class MessagePostResult(_message.Message):
        __slots__ = ("message_id", "success", "error", "error_code")
        class ErrorCode(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
            __slots__ = ()
            SUCCESS: _ClassVar[PostMessagesBulkResponse.MessagePostResult.ErrorCode]
            VALIDATION_FAILED: _ClassVar[PostMessagesBulkResponse.MessagePostResult.ErrorCode]
            DUPLICATE_MESSAGE_ID: _ClassVar[PostMessagesBulkResponse.MessagePostResult.ErrorCode]
            SCHEMA_MISMATCH: _ClassVar[PostMessagesBulkResponse.MessagePostResult.ErrorCode]
            INTERNAL_ERROR: _ClassVar[PostMessagesBulkResponse.MessagePostResult.ErrorCode]
            QUEUE_NOT_FOUND: _ClassVar[PostMessagesBulkResponse.MessagePostResult.ErrorCode]
        SUCCESS: PostMessagesBulkResponse.MessagePostResult.ErrorCode
        VALIDATION_FAILED: PostMessagesBulkResponse.MessagePostResult.ErrorCode
        DUPLICATE_MESSAGE_ID: PostMessagesBulkResponse.MessagePostResult.ErrorCode
        SCHEMA_MISMATCH: PostMessagesBulkResponse.MessagePostResult.ErrorCode
        INTERNAL_ERROR: PostMessagesBulkResponse.MessagePostResult.ErrorCode
        QUEUE_NOT_FOUND: PostMessagesBulkResponse.MessagePostResult.ErrorCode
        MESSAGE_ID_FIELD_NUMBER: _ClassVar[int]
        SUCCESS_FIELD_NUMBER: _ClassVar[int]
        ERROR_FIELD_NUMBER: _ClassVar[int]
        ERROR_CODE_FIELD_NUMBER: _ClassVar[int]
        message_id: str
        success: bool
        error: str
        error_code: PostMessagesBulkResponse.MessagePostResult.ErrorCode
        def __init__(self, message_id: _Optional[str] = ..., success: bool = ..., error: _Optional[str] = ..., error_code: _Optional[_Union[PostMessagesBulkResponse.MessagePostResult.ErrorCode, str]] = ...) -> None: ...
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    SUCCESSFUL_COUNT_FIELD_NUMBER: _ClassVar[int]
    FAILED_COUNT_FIELD_NUMBER: _ClassVar[int]
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    success: bool
    successful_count: int
    failed_count: int
    results: _containers.RepeatedCompositeFieldContainer[PostMessagesBulkResponse.MessagePostResult]
    def __init__(self, success: bool = ..., successful_count: _Optional[int] = ..., failed_count: _Optional[int] = ..., results: _Optional[_Iterable[_Union[PostMessagesBulkResponse.MessagePostResult, _Mapping]]] = ...) -> None: ...

class GetNextMessageRequest(_message.Message):
    __slots__ = ("queue_name", "lease_duration", "exclusivity_key", "worker_id", "attempt_id")
    QUEUE_NAME_FIELD_NUMBER: _ClassVar[int]
    LEASE_DURATION_FIELD_NUMBER: _ClassVar[int]
    EXCLUSIVITY_KEY_FIELD_NUMBER: _ClassVar[int]
    WORKER_ID_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_ID_FIELD_NUMBER: _ClassVar[int]
    queue_name: str
    lease_duration: _duration_pb2.Duration
    exclusivity_key: str
    worker_id: str
    attempt_id: str
    def __init__(self, queue_name: _Optional[str] = ..., lease_duration: _Optional[_Union[_duration_pb2.Duration, _Mapping]] = ..., exclusivity_key: _Optional[str] = ..., worker_id: _Optional[str] = ..., attempt_id: _Optional[str] = ...) -> None: ...

class GetNextMessageResponse(_message.Message):
    __slots__ = ("message", "worker_id", "attempt_id")
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    WORKER_ID_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_ID_FIELD_NUMBER: _ClassVar[int]
    message: _message_pb2.Message
    worker_id: str
    attempt_id: str
    def __init__(self, message: _Optional[_Union[_message_pb2.Message, _Mapping]] = ..., worker_id: _Optional[str] = ..., attempt_id: _Optional[str] = ...) -> None: ...

class AcknowledgeMessageRequest(_message.Message):
    __slots__ = ("queue_name", "message_id", "state", "worker_id", "attempt_id")
    QUEUE_NAME_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_ID_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    WORKER_ID_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_ID_FIELD_NUMBER: _ClassVar[int]
    queue_name: str
    message_id: str
    state: _message_pb2.Message.Metadata.State
    worker_id: str
    attempt_id: str
    def __init__(self, queue_name: _Optional[str] = ..., message_id: _Optional[str] = ..., state: _Optional[_Union[_message_pb2.Message.Metadata.State, str]] = ..., worker_id: _Optional[str] = ..., attempt_id: _Optional[str] = ...) -> None: ...

class AcknowledgeMessageResponse(_message.Message):
    __slots__ = ("success",)
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    success: bool
    def __init__(self, success: bool = ...) -> None: ...

class CancelMessageRequest(_message.Message):
    __slots__ = ("queue_name", "message_id", "reason")
    QUEUE_NAME_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_ID_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    queue_name: str
    message_id: str
    reason: str
    def __init__(self, queue_name: _Optional[str] = ..., message_id: _Optional[str] = ..., reason: _Optional[str] = ...) -> None: ...

class CancelMessageResponse(_message.Message):
    __slots__ = ("success",)
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    success: bool
    def __init__(self, success: bool = ...) -> None: ...

class RenewMessageLeaseRequest(_message.Message):
    __slots__ = ("queue_name", "message_id", "lease_duration", "worker_id", "attempt_id")
    QUEUE_NAME_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_ID_FIELD_NUMBER: _ClassVar[int]
    LEASE_DURATION_FIELD_NUMBER: _ClassVar[int]
    WORKER_ID_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_ID_FIELD_NUMBER: _ClassVar[int]
    queue_name: str
    message_id: str
    lease_duration: _duration_pb2.Duration
    worker_id: str
    attempt_id: str
    def __init__(self, queue_name: _Optional[str] = ..., message_id: _Optional[str] = ..., lease_duration: _Optional[_Union[_duration_pb2.Duration, _Mapping]] = ..., worker_id: _Optional[str] = ..., attempt_id: _Optional[str] = ...) -> None: ...

class RenewMessageLeaseResponse(_message.Message):
    __slots__ = ("remaining_time", "state")
    REMAINING_TIME_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    remaining_time: _duration_pb2.Duration
    state: _message_pb2.Message.Metadata.State
    def __init__(self, remaining_time: _Optional[_Union[_duration_pb2.Duration, _Mapping]] = ..., state: _Optional[_Union[_message_pb2.Message.Metadata.State, str]] = ...) -> None: ...

class PeekQueueMessagesRequest(_message.Message):
    __slots__ = ("queue_name", "page_size", "priority_range", "page_token")
    class PriorityRange(_message.Message):
        __slots__ = ("min", "max")
        MIN_FIELD_NUMBER: _ClassVar[int]
        MAX_FIELD_NUMBER: _ClassVar[int]
        min: int
        max: int
        def __init__(self, min: _Optional[int] = ..., max: _Optional[int] = ...) -> None: ...
    QUEUE_NAME_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    PRIORITY_RANGE_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    queue_name: str
    page_size: int
    priority_range: PeekQueueMessagesRequest.PriorityRange
    page_token: str
    def __init__(self, queue_name: _Optional[str] = ..., page_size: _Optional[int] = ..., priority_range: _Optional[_Union[PeekQueueMessagesRequest.PriorityRange, _Mapping]] = ..., page_token: _Optional[str] = ...) -> None: ...

class PeekQueueMessagesResponse(_message.Message):
    __slots__ = ("messages", "next_page_token")
    MESSAGES_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    messages: _containers.RepeatedCompositeFieldContainer[_message_pb2.Message]
    next_page_token: str
    def __init__(self, messages: _Optional[_Iterable[_Union[_message_pb2.Message, _Mapping]]] = ..., next_page_token: _Optional[str] = ...) -> None: ...

class GetQueueStateRequest(_message.Message):
    __slots__ = ("queue_name",)
    QUEUE_NAME_FIELD_NUMBER: _ClassVar[int]
    queue_name: str
    def __init__(self, queue_name: _Optional[str] = ...) -> None: ...

class GetQueueStateResponse(_message.Message):
    __slots__ = ("state_counts", "earliest_deadline")
    class StateCountsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: int
        def __init__(self, key: _Optional[str] = ..., value: _Optional[int] = ...) -> None: ...
    STATE_COUNTS_FIELD_NUMBER: _ClassVar[int]
    EARLIEST_DEADLINE_FIELD_NUMBER: _ClassVar[int]
    state_counts: _containers.ScalarMap[str, int]
    earliest_deadline: _timestamp_pb2.Timestamp
    def __init__(self, state_counts: _Optional[_Mapping[str, int]] = ..., earliest_deadline: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class SendMessageHeartBeatRequest(_message.Message):
    __slots__ = ("queue_name", "message_id", "worker_id", "attempt_id")
    QUEUE_NAME_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_ID_FIELD_NUMBER: _ClassVar[int]
    WORKER_ID_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_ID_FIELD_NUMBER: _ClassVar[int]
    queue_name: str
    message_id: str
    worker_id: str
    attempt_id: str
    def __init__(self, queue_name: _Optional[str] = ..., message_id: _Optional[str] = ..., worker_id: _Optional[str] = ..., attempt_id: _Optional[str] = ...) -> None: ...

class SendMessageHeartBeatResponse(_message.Message):
    __slots__ = ("remaining_time", "state")
    REMAINING_TIME_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    remaining_time: _duration_pb2.Duration
    state: _message_pb2.Message.Metadata.State
    def __init__(self, remaining_time: _Optional[_Union[_duration_pb2.Duration, _Mapping]] = ..., state: _Optional[_Union[_message_pb2.Message.Metadata.State, str]] = ...) -> None: ...

class ListQueuesRequest(_message.Message):
    __slots__ = ("prefix", "page_size", "page_token")
    PREFIX_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    prefix: str
    page_size: int
    page_token: str
    def __init__(self, prefix: _Optional[str] = ..., page_size: _Optional[int] = ..., page_token: _Optional[str] = ...) -> None: ...

class ListQueuesResponse(_message.Message):
    __slots__ = ("queues", "next_page_token")
    QUEUES_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    queues: _containers.RepeatedCompositeFieldContainer[_queue_pb2.Queue]
    next_page_token: str
    def __init__(self, queues: _Optional[_Iterable[_Union[_queue_pb2.Queue, _Mapping]]] = ..., next_page_token: _Optional[str] = ...) -> None: ...

class CreateScheduleRequest(_message.Message):
    __slots__ = ("schedule",)
    SCHEDULE_FIELD_NUMBER: _ClassVar[int]
    schedule: _schedule_pb2.Schedule
    def __init__(self, schedule: _Optional[_Union[_schedule_pb2.Schedule, _Mapping]] = ...) -> None: ...

class CreateScheduleResponse(_message.Message):
    __slots__ = ("success",)
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    success: bool
    def __init__(self, success: bool = ...) -> None: ...

class DeleteScheduleRequest(_message.Message):
    __slots__ = ("schedule_id",)
    SCHEDULE_ID_FIELD_NUMBER: _ClassVar[int]
    schedule_id: str
    def __init__(self, schedule_id: _Optional[str] = ...) -> None: ...

class DeleteScheduleResponse(_message.Message):
    __slots__ = ("success",)
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    success: bool
    def __init__(self, success: bool = ...) -> None: ...

class PauseScheduleRequest(_message.Message):
    __slots__ = ("schedule_id",)
    SCHEDULE_ID_FIELD_NUMBER: _ClassVar[int]
    schedule_id: str
    def __init__(self, schedule_id: _Optional[str] = ...) -> None: ...

class PauseScheduleResponse(_message.Message):
    __slots__ = ("success",)
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    success: bool
    def __init__(self, success: bool = ...) -> None: ...

class ResumeScheduleRequest(_message.Message):
    __slots__ = ("schedule_id",)
    SCHEDULE_ID_FIELD_NUMBER: _ClassVar[int]
    schedule_id: str
    def __init__(self, schedule_id: _Optional[str] = ...) -> None: ...

class ResumeScheduleResponse(_message.Message):
    __slots__ = ("success",)
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    success: bool
    def __init__(self, success: bool = ...) -> None: ...

class GetScheduleRequest(_message.Message):
    __slots__ = ("schedule_id",)
    SCHEDULE_ID_FIELD_NUMBER: _ClassVar[int]
    schedule_id: str
    def __init__(self, schedule_id: _Optional[str] = ...) -> None: ...

class GetScheduleResponse(_message.Message):
    __slots__ = ("schedule",)
    SCHEDULE_FIELD_NUMBER: _ClassVar[int]
    schedule: _schedule_pb2.Schedule
    def __init__(self, schedule: _Optional[_Union[_schedule_pb2.Schedule, _Mapping]] = ...) -> None: ...

class ListSchedulesRequest(_message.Message):
    __slots__ = ("prefix", "page_size", "page_token")
    PREFIX_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    prefix: str
    page_size: int
    page_token: str
    def __init__(self, prefix: _Optional[str] = ..., page_size: _Optional[int] = ..., page_token: _Optional[str] = ...) -> None: ...

class ListSchedulesResponse(_message.Message):
    __slots__ = ("schedules", "next_page_token")
    SCHEDULES_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    schedules: _containers.RepeatedCompositeFieldContainer[_schedule_pb2.Schedule]
    next_page_token: str
    def __init__(self, schedules: _Optional[_Iterable[_Union[_schedule_pb2.Schedule, _Mapping]]] = ..., next_page_token: _Optional[str] = ...) -> None: ...

class GetScheduleHistoryRequest(_message.Message):
    __slots__ = ("schedule_id", "page_size", "page_token")
    SCHEDULE_ID_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    schedule_id: str
    page_size: int
    page_token: str
    def __init__(self, schedule_id: _Optional[str] = ..., page_size: _Optional[int] = ..., page_token: _Optional[str] = ...) -> None: ...

class GetScheduleHistoryResponse(_message.Message):
    __slots__ = ("schedule_history", "next_page_token")
    SCHEDULE_HISTORY_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    schedule_history: _schedule_pb2.ScheduleHistory
    next_page_token: str
    def __init__(self, schedule_history: _Optional[_Union[_schedule_pb2.ScheduleHistory, _Mapping]] = ..., next_page_token: _Optional[str] = ...) -> None: ...

class GetDLQMessagesRequest(_message.Message):
    __slots__ = ("dlq_name", "page_size", "page_token")
    DLQ_NAME_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    dlq_name: str
    page_size: int
    page_token: str
    def __init__(self, dlq_name: _Optional[str] = ..., page_size: _Optional[int] = ..., page_token: _Optional[str] = ...) -> None: ...

class GetDLQMessagesResponse(_message.Message):
    __slots__ = ("messages", "next_page_token")
    MESSAGES_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    messages: _containers.RepeatedCompositeFieldContainer[_message_pb2.Message]
    next_page_token: str
    def __init__(self, messages: _Optional[_Iterable[_Union[_message_pb2.Message, _Mapping]]] = ..., next_page_token: _Optional[str] = ...) -> None: ...

class RequeueFromDLQRequest(_message.Message):
    __slots__ = ("dlq_name", "message_id", "target_queue")
    DLQ_NAME_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_ID_FIELD_NUMBER: _ClassVar[int]
    TARGET_QUEUE_FIELD_NUMBER: _ClassVar[int]
    dlq_name: str
    message_id: str
    target_queue: str
    def __init__(self, dlq_name: _Optional[str] = ..., message_id: _Optional[str] = ..., target_queue: _Optional[str] = ...) -> None: ...

class RequeueFromDLQResponse(_message.Message):
    __slots__ = ("success",)
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    success: bool
    def __init__(self, success: bool = ...) -> None: ...

class DeleteFromDLQRequest(_message.Message):
    __slots__ = ("dlq_name", "message_id")
    DLQ_NAME_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_ID_FIELD_NUMBER: _ClassVar[int]
    dlq_name: str
    message_id: str
    def __init__(self, dlq_name: _Optional[str] = ..., message_id: _Optional[str] = ...) -> None: ...

class DeleteFromDLQResponse(_message.Message):
    __slots__ = ("success",)
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    success: bool
    def __init__(self, success: bool = ...) -> None: ...

class PurgeDLQRequest(_message.Message):
    __slots__ = ("dlq_name",)
    DLQ_NAME_FIELD_NUMBER: _ClassVar[int]
    dlq_name: str
    def __init__(self, dlq_name: _Optional[str] = ...) -> None: ...

class PurgeDLQResponse(_message.Message):
    __slots__ = ("success",)
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    success: bool
    def __init__(self, success: bool = ...) -> None: ...

class GetDLQStatsRequest(_message.Message):
    __slots__ = ("dlq_name",)
    DLQ_NAME_FIELD_NUMBER: _ClassVar[int]
    dlq_name: str
    def __init__(self, dlq_name: _Optional[str] = ...) -> None: ...

class GetDLQStatsResponse(_message.Message):
    __slots__ = ("name", "message_count", "created_at", "updated_at")
    NAME_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_COUNT_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    name: str
    message_count: int
    created_at: int
    updated_at: int
    def __init__(self, name: _Optional[str] = ..., message_count: _Optional[int] = ..., created_at: _Optional[int] = ..., updated_at: _Optional[int] = ...) -> None: ...

class ValidateCalendarScheduleRequest(_message.Message):
    __slots__ = ("calendar_schedule",)
    CALENDAR_SCHEDULE_FIELD_NUMBER: _ClassVar[int]
    calendar_schedule: _schedule_pb2.CalendarSchedule
    def __init__(self, calendar_schedule: _Optional[_Union[_schedule_pb2.CalendarSchedule, _Mapping]] = ...) -> None: ...

class ValidateCalendarScheduleResponse(_message.Message):
    __slots__ = ("valid", "error_message", "validation_issues")
    VALID_FIELD_NUMBER: _ClassVar[int]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    VALIDATION_ISSUES_FIELD_NUMBER: _ClassVar[int]
    valid: bool
    error_message: str
    validation_issues: _containers.RepeatedCompositeFieldContainer[ValidationIssue]
    def __init__(self, valid: bool = ..., error_message: _Optional[str] = ..., validation_issues: _Optional[_Iterable[_Union[ValidationIssue, _Mapping]]] = ...) -> None: ...

class PreviewCalendarScheduleRequest(_message.Message):
    __slots__ = ("calendar_schedule", "count")
    CALENDAR_SCHEDULE_FIELD_NUMBER: _ClassVar[int]
    COUNT_FIELD_NUMBER: _ClassVar[int]
    calendar_schedule: _schedule_pb2.CalendarSchedule
    count: int
    def __init__(self, calendar_schedule: _Optional[_Union[_schedule_pb2.CalendarSchedule, _Mapping]] = ..., count: _Optional[int] = ...) -> None: ...

class PreviewCalendarScheduleResponse(_message.Message):
    __slots__ = ("execution_times", "timezone", "preview_start", "total_count")
    EXECUTION_TIMES_FIELD_NUMBER: _ClassVar[int]
    TIMEZONE_FIELD_NUMBER: _ClassVar[int]
    PREVIEW_START_FIELD_NUMBER: _ClassVar[int]
    TOTAL_COUNT_FIELD_NUMBER: _ClassVar[int]
    execution_times: _containers.RepeatedCompositeFieldContainer[_timestamp_pb2.Timestamp]
    timezone: str
    preview_start: _timestamp_pb2.Timestamp
    total_count: int
    def __init__(self, execution_times: _Optional[_Iterable[_Union[_timestamp_pb2.Timestamp, _Mapping]]] = ..., timezone: _Optional[str] = ..., preview_start: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., total_count: _Optional[int] = ...) -> None: ...

class ValidationIssue(_message.Message):
    __slots__ = ("severity", "rule_index", "field", "message", "suggestion")
    SEVERITY_FIELD_NUMBER: _ClassVar[int]
    RULE_INDEX_FIELD_NUMBER: _ClassVar[int]
    FIELD_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    SUGGESTION_FIELD_NUMBER: _ClassVar[int]
    severity: str
    rule_index: int
    field: str
    message: str
    suggestion: str
    def __init__(self, severity: _Optional[str] = ..., rule_index: _Optional[int] = ..., field: _Optional[str] = ..., message: _Optional[str] = ..., suggestion: _Optional[str] = ...) -> None: ...

class RegisterSchemaRequest(_message.Message):
    __slots__ = ("schema_id", "name", "description", "content", "content_type", "metadata")
    class MetadataEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    SCHEMA_ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    CONTENT_FIELD_NUMBER: _ClassVar[int]
    CONTENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    schema_id: str
    name: str
    description: str
    content: str
    content_type: str
    metadata: _containers.ScalarMap[str, str]
    def __init__(self, schema_id: _Optional[str] = ..., name: _Optional[str] = ..., description: _Optional[str] = ..., content: _Optional[str] = ..., content_type: _Optional[str] = ..., metadata: _Optional[_Mapping[str, str]] = ...) -> None: ...

class RegisterSchemaResponse(_message.Message):
    __slots__ = ("schema_id", "version", "created_at")
    SCHEMA_ID_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    schema_id: str
    version: int
    created_at: int
    def __init__(self, schema_id: _Optional[str] = ..., version: _Optional[int] = ..., created_at: _Optional[int] = ...) -> None: ...

class GetSchemaRequest(_message.Message):
    __slots__ = ("schema_id", "version")
    SCHEMA_ID_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    schema_id: str
    version: int
    def __init__(self, schema_id: _Optional[str] = ..., version: _Optional[int] = ...) -> None: ...

class GetSchemaResponse(_message.Message):
    __slots__ = ("schema",)
    SCHEMA_FIELD_NUMBER: _ClassVar[int]
    schema: _schema_pb2.Schema
    def __init__(self, schema: _Optional[_Union[_schema_pb2.Schema, _Mapping]] = ...) -> None: ...

class ListSchemasRequest(_message.Message):
    __slots__ = ("prefix", "page_size", "active_only", "page_token")
    PREFIX_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_ONLY_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    prefix: str
    page_size: int
    active_only: bool
    page_token: str
    def __init__(self, prefix: _Optional[str] = ..., page_size: _Optional[int] = ..., active_only: bool = ..., page_token: _Optional[str] = ...) -> None: ...

class ListSchemasResponse(_message.Message):
    __slots__ = ("schemas", "total_count", "next_page_token")
    SCHEMAS_FIELD_NUMBER: _ClassVar[int]
    TOTAL_COUNT_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    schemas: _containers.RepeatedCompositeFieldContainer[SchemaInfo]
    total_count: int
    next_page_token: str
    def __init__(self, schemas: _Optional[_Iterable[_Union[SchemaInfo, _Mapping]]] = ..., total_count: _Optional[int] = ..., next_page_token: _Optional[str] = ...) -> None: ...

class SchemaInfo(_message.Message):
    __slots__ = ("schema_id", "latest_version", "name", "description", "created_at", "updated_at", "version_count", "is_active")
    SCHEMA_ID_FIELD_NUMBER: _ClassVar[int]
    LATEST_VERSION_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    VERSION_COUNT_FIELD_NUMBER: _ClassVar[int]
    IS_ACTIVE_FIELD_NUMBER: _ClassVar[int]
    schema_id: str
    latest_version: int
    name: str
    description: str
    created_at: int
    updated_at: int
    version_count: int
    is_active: bool
    def __init__(self, schema_id: _Optional[str] = ..., latest_version: _Optional[int] = ..., name: _Optional[str] = ..., description: _Optional[str] = ..., created_at: _Optional[int] = ..., updated_at: _Optional[int] = ..., version_count: _Optional[int] = ..., is_active: bool = ...) -> None: ...

class DeleteSchemaRequest(_message.Message):
    __slots__ = ("schema_id", "version")
    SCHEMA_ID_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    schema_id: str
    version: int
    def __init__(self, schema_id: _Optional[str] = ..., version: _Optional[int] = ...) -> None: ...

class DeleteSchemaResponse(_message.Message):
    __slots__ = ("success", "versions_deleted")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    VERSIONS_DELETED_FIELD_NUMBER: _ClassVar[int]
    success: bool
    versions_deleted: int
    def __init__(self, success: bool = ..., versions_deleted: _Optional[int] = ...) -> None: ...

class ValidatePayloadRequest(_message.Message):
    __slots__ = ("schema_id", "version", "payload")
    SCHEMA_ID_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    schema_id: str
    version: int
    payload: str
    def __init__(self, schema_id: _Optional[str] = ..., version: _Optional[int] = ..., payload: _Optional[str] = ...) -> None: ...

class ValidatePayloadResponse(_message.Message):
    __slots__ = ("valid", "errors", "schema_id", "schema_version")
    VALID_FIELD_NUMBER: _ClassVar[int]
    ERRORS_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_ID_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_VERSION_FIELD_NUMBER: _ClassVar[int]
    valid: bool
    errors: _containers.RepeatedCompositeFieldContainer[_schema_pb2.ValidationError]
    schema_id: str
    schema_version: int
    def __init__(self, valid: bool = ..., errors: _Optional[_Iterable[_Union[_schema_pb2.ValidationError, _Mapping]]] = ..., schema_id: _Optional[str] = ..., schema_version: _Optional[int] = ...) -> None: ...

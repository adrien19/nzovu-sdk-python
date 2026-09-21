import json
import os
import re
import socket
import uuid
from dataclasses import dataclass, field
from decimal import Decimal, localcontext
from enum import Enum
from typing import Any, Callable, Dict, Optional, Type, TypeVar

from google.protobuf import json_format
from google.protobuf.duration_pb2 import Duration
from google.protobuf.struct_pb2 import Struct
from google.protobuf.timestamp_pb2 import Timestamp

from .api.common.v1.common_pb2 import Payload  # type: ignore[attr-defined]
from .api.message.v1.message_pb2 import Message  # type: ignore[attr-defined]
from .api.queue.v1.queue_pb2 import MessageRetentionPolicy as _MessageRetentionPolicyProto  # type: ignore[attr-defined]
from .api.queue.v1.queue_pb2 import QueueType  # type: ignore[attr-defined]
from .api.queueservice.v1.request_response_pb2 import (  # type: ignore[attr-defined]
    CreateScheduleRequest,
    PostMessageRequest,
    PostMessagesBulkRequest,
)
from .api.schedule.v1.schedule_pb2 import CalendarSchedule, Schedule  # type: ignore[attr-defined]
from .ownership import Claim

# Import Pydantic models (will handle if not available)
try:
    from . import models

    PYDANTIC_AVAILABLE = models.PYDANTIC_AVAILABLE
except ImportError:
    PYDANTIC_AVAILABLE = False
    models = None  # type: ignore[assignment]

T = TypeVar("T")


def generate_worker_id(prefix: str = "") -> str:
    """
    Generate a unique worker identifier.

    Creates a stable, unique identifier for a worker/consumer that can be used
    across multiple message processing operations. The ID includes the hostname
    and process ID for traceability, along with a UUID for uniqueness.

    Args:
        prefix: Optional prefix for the worker ID (e.g., "api-server", "batch-processor")

    Returns:
        A unique worker identifier string in the format:
        "{prefix}-{hostname}-{pid}-{uuid}" or "{hostname}-{pid}-{uuid}" if no prefix

    Example:
        >>> worker_id = generate_worker_id("order-processor")
        >>> print(worker_id)
        order-processor-hostname-12345-a1b2c3d4

        >>> worker_id = generate_worker_id()
        >>> print(worker_id)
        hostname-12345-a1b2c3d4
    """
    hostname = socket.gethostname().replace(".", "-")[:32]  # Limit hostname length
    pid = os.getpid()
    unique_id = str(uuid.uuid4())[:8]  # Short UUID for readability

    if prefix:
        return f"{prefix}-{hostname}-{pid}-{unique_id}"
    return f"{hostname}-{pid}-{unique_id}"


def validate_integer(value, name, minimum, maximum):
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError(f"{name} must be an integer between {minimum} and {maximum}")


def validate_page(page_size, page_token):
    validate_integer(page_size, "page_size", 0, 1000)
    if not isinstance(page_token, str):
        raise ValueError("page_token must be a string")


def require_name(value, name):
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} is required")


@dataclass(frozen=True)
class Header:
    key: str
    value: bytes


def build_headers(headers):
    result = []
    total = 0
    for header in headers:
        if not isinstance(header, Header):
            raise ValueError("headers must contain Header instances")
        if not isinstance(header.key, str) or not re.fullmatch(r"[a-z0-9-]+", header.key):
            raise ValueError("Header keys allow lowercase letters, numbers and hyphens")
        if header.key.startswith(("x-nzovu-", "x-internal-", "x-system-")):
            raise ValueError("Header key uses a reserved prefix")
        if not isinstance(header.value, bytes) or len(header.value) > 4096:
            raise ValueError("Header values must be bytes of at most 4096 bytes")
        total += len(header.key) + len(header.value)
        result.append(Message.Metadata.Header(key=header.key, value=header.value))
    if total > 32768:
        raise ValueError("Total header keys and values exceed 32768 bytes")
    return result


def validate_json_value(value):
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValueError("JSON object keys must be strings")
            validate_json_value(item)
    elif isinstance(value, list):
        for item in value:
            validate_json_value(item)
    elif value is not None and not isinstance(value, (str, bool, int, float)):
        raise ValueError("Payload values must be JSON-compatible")


def build_payload(data, metadata=None, content_type="", schema_id="", schema_version=0):
    if not isinstance(data, dict) or (metadata is not None and not isinstance(metadata, dict)):
        raise ValueError("Payload data and metadata must be dictionaries")
    normalized = content_type.split(";", 1)[0].strip().lower()
    if normalized not in {"", "application/json", "application/x-json"}:
        raise ValueError("content_type must describe a JSON payload")
    validate_integer(schema_version, "schema_version", 0, 2**31 - 1)
    values = {
        "data": data,
        "metadata": metadata or {},
        "content_type": content_type,
        "schema_id": schema_id,
        "schema_version": schema_version,
    }
    validate_json_value(values)
    json.dumps(values, allow_nan=False)
    return json_format.ParseDict(values, Payload())


@dataclass
class TlsConfig:
    """
    TlsConfig represents the configuration required for setting up a secure TLS connection.

    Attributes:
        ca_path (str): Path to the Certificate Authority (CA) certificate.
        client_crt_path (str): Path to the client's certificate.
        client_key_path (str): Path to the client's private key.
    """

    ca_path: Optional[str] = None
    client_crt_path: Optional[str] = None
    client_key_path: Optional[str] = None


class MessageState(Enum):
    """
    Enumeration representing the possible states of a message in Nzovu.

    Attributes:
    ----------
    INVISIBLE : MessageState
        Message is not visible for processing.
    PENDING : MessageState
        Message is pending and waiting for processing.
    RUNNING : MessageState
        Message is currently being processed.
    COMPLETED : MessageState
        Message processing has completed.
    CANCELED : MessageState
        Message processing was canceled.
    ERRORED : MessageState
        An error occurred during message processing.
    """

    INVISIBLE = Message.Metadata.State.INVISIBLE
    PENDING = Message.Metadata.State.PENDING
    RUNNING = Message.Metadata.State.RUNNING
    COMPLETED = Message.Metadata.State.COMPLETED
    CANCELED = Message.Metadata.State.CANCELED
    ERRORED = Message.Metadata.State.ERRORED


class ScheduleState(Enum):
    """
    Enumeration representing the possible states of a schedule in Nzovu.

    Attributes:
    ----------
    SCHEDULED : ScheduleState
        Schedule is active and will run at configured times.
    CANCELED : ScheduleState
        Schedule has been canceled.
    ERRORED : ScheduleState
        Schedule encountered an error.
    PAUSED : ScheduleState
        Schedule is paused and will not run.
    """

    SCHEDULED = Schedule.Metadata.State.SCHEDULED
    CANCELED = Schedule.Metadata.State.CANCELED
    ERRORED = Schedule.Metadata.State.ERRORED
    PAUSED = Schedule.Metadata.State.PAUSED


class TransactionMode(Enum):
    """
    Enumeration representing transaction modes for bulk message posting.

    Attributes:
    ----------
    ALL_OR_NOTHING : TransactionMode
        All messages succeed or all fail (atomic operation).
        Batch processed in single database transaction.
    BEST_EFFORT : TransactionMode
        Process as many as possible, continue on failures.
        Messages processed independently, partial success allowed.
    """

    ALL_OR_NOTHING = "ALL_OR_NOTHING"
    BEST_EFFORT = "BEST_EFFORT"


class RetentionMode(Enum):
    """
    Enumeration representing message retention modes for a queue.

    Attributes:
    ----------
    DELETE_IMMEDIATELY : RetentionMode
        Remove message from the database immediately upon acknowledgment (default).
    RETAIN_DURATION : RetentionMode
        Soft-delete messages and remove them after retention_seconds.
    RETAIN_FOREVER : RetentionMode
        Soft-delete messages and never auto-remove them.
    """

    DELETE_IMMEDIATELY = _MessageRetentionPolicyProto.Mode.DELETE_IMMEDIATELY
    RETAIN_DURATION = _MessageRetentionPolicyProto.Mode.RETAIN_DURATION
    RETAIN_FOREVER = _MessageRetentionPolicyProto.Mode.RETAIN_FOREVER


@dataclass
class MessageRetentionPolicy:
    """
    Controls how long messages are retained after completion or error.

    Attributes:
    ----------
    mode : RetentionMode
        Retention strategy. DELETE_IMMEDIATELY, RETAIN_DURATION, or RETAIN_FOREVER.
    retention_seconds : int, optional
        Seconds to retain messages. Only used with RETAIN_DURATION mode.
    """

    mode: RetentionMode = RetentionMode.DELETE_IMMEDIATELY
    retention_seconds: int = 0


def string_to_duration(s: str) -> Duration:
    """
    Convert a string representation of duration to a protobuf Duration object.

    Args:
        s: Duration string in format "[number]unit" (e.g., "5s", "2m", "3h", "1d")

    Returns:
        Duration: Protobuf Duration object
    """
    if not s:
        return Duration(seconds=0)

    unit_map = {"s": 1, "m": 60, "h": 3600, "d": 86400}
    match = re.match(r"^(\d+(?:\.\d+)?)([smhd])$", s)
    if not match:
        raise ValueError(f"Invalid duration format: {s}")

    value, unit = match.groups()
    with localcontext() as context:
        context.prec = max(28, len(value) + 12)
        nanoseconds = Decimal(value) * unit_map[unit] * 1_000_000_000
    if nanoseconds != nanoseconds.to_integral_value():
        raise ValueError("Duration precision cannot exceed nanoseconds")
    seconds, nanos = divmod(int(nanoseconds), 1_000_000_000)
    if seconds > 315_576_000_000:
        raise ValueError("Duration exceeds protobuf range")
    return Duration(seconds=seconds, nanos=nanos)


def dict_to_protobuf_struct(data: Dict[str, Any]) -> Struct:
    """Convert a Python dict to a protobuf Struct."""
    struct = Struct()
    struct.update(data)
    return struct


@dataclass
class LeasePolicyOptions:
    """
    Configuration for message lease policies.

    Defines how long a single processing attempt is allowed to run and how
    heartbeats can extend that time.

    Attributes:
    ----------
    base_lease : str, optional
        Initial lease duration for an attempt. The attempt starts with this much
        time before timing out, unless extended. Format: "[number]unit" (e.g., "30s", "5m").
    max_extension : str, optional
        Maximum additional time beyond base_lease that an attempt may obtain via
        heartbeats. Format: "[number]unit" (e.g., "10m", "1h").
    heartbeat_timeout : str, optional
        Maximum allowed gap between heartbeats. If a heartbeat is not received within
        this duration, the lease may expire. Format: "[number]unit" (e.g., "30s", "2m").
    extend_step : str, optional
        Amount of time to extend the lease by when a heartbeat is received, until
        max_extension is exhausted. Format: "[number]unit" (e.g., "2s", "30s").

    Example:
    -------
    >>> policy = LeasePolicyOptions(
    ...     base_lease="30s",
    ...     max_extension="10m",
    ...     heartbeat_timeout="30s",
    ...     extend_step="2s"
    ... )
    """

    base_lease: Optional[str] = None
    max_extension: Optional[str] = None
    heartbeat_timeout: Optional[str] = None
    extend_step: Optional[str] = None
    max_renewals: Optional[int] = None

    def __post_init__(self):
        if self.max_renewals is not None:
            validate_integer(self.max_renewals, "max_renewals", 0, 2**31 - 1)
        duration_pattern = re.compile(r"^\d+(\.\d+)?[smhd]$")
        for field_name in ["base_lease", "max_extension", "heartbeat_timeout", "extend_step"]:
            value = getattr(self, field_name)
            if value and not duration_pattern.match(value):
                raise ValueError(f"{field_name} must be in format '[number]unit', e.g., '5s', '2m', '3.5m', '3d'.")


def build_lease_policy(opts: Optional[LeasePolicyOptions]):
    """
    Build a protobuf LeasePolicy from LeasePolicyOptions.

    Args:
        opts: LeasePolicyOptions instance or None

    Returns:
        LeasePolicy protobuf object or None if opts is None or all fields are None
    """
    if opts is None:
        return None

    # Check if any field is set
    if (
        not any([opts.base_lease, opts.max_extension, opts.heartbeat_timeout, opts.extend_step])
        and opts.max_renewals is None
    ):
        return None

    from .api.common.v1.common_pb2 import LeasePolicy  # type: ignore[attr-defined]

    lp = LeasePolicy()  # type: ignore[attr-defined]

    if opts.base_lease:
        lp.base_lease.CopyFrom(string_to_duration(opts.base_lease))

    if opts.max_extension:
        lp.max_extension.CopyFrom(string_to_duration(opts.max_extension))

    if opts.heartbeat_timeout:
        lp.heartbeat_timeout.CopyFrom(string_to_duration(opts.heartbeat_timeout))

    if opts.extend_step:
        lp.extend_step.CopyFrom(string_to_duration(opts.extend_step))

    if opts.max_renewals is not None:
        validate_integer(opts.max_renewals, "max_renewals", 0, 2**31 - 1)
        lp.max_renewals = opts.max_renewals
    return lp


@dataclass
class PostMessageOptions:
    """
    Optional settings for posting a message to a Nzovu.

    Attributes:
    ----------
    priority : int, optional (default=0)
        Priority level of the message.
    state : MessageState, optional (default=INVISIBLE)
        Initial state of the message.
    lease_duration : str, optional (default="0s")
        Duration for which the message should be processed for by a worker. Must be in format "[number]unit",
        for example: "5s", "2m", "3.5m", or "3d".
        Duration for which the message should remain invisible. Must be in format "[number]unit",
        for example: "5s", "2m", "3.5m", or "3d".
    max_attempts : int, optional (default=3)
        Maximum number of processing attempts for the message.
    data_metadata : Dict, optional
        Metadata associated with the message's payload.
    lease_policy : LeasePolicyOptions, optional
        Lease policy configuration for fine-grained control over message processing timeouts
        and heartbeat behavior.
    """

    priority: int = 0
    state: MessageState = MessageState.INVISIBLE
    lease_duration: Optional[str] = None
    max_attempts: int = 0
    data_metadata: Dict = field(default_factory=dict)
    lease_policy: Optional[LeasePolicyOptions] = None

    headers: list[Header] = field(default_factory=list)
    content_type: str = ""
    schema_id: str = ""
    schema_version: int = 0
    scheduled_time: Optional[str] = None

    def __post_init__(self):
        validate_integer(self.priority, "priority", 0, 4)
        duration_pattern = re.compile(r"^\d+(\.\d+)?[smhd]$")
        if self.lease_duration and not duration_pattern.match(self.lease_duration):
            raise ValueError("lease_duration must be in format '[number]unit', e.g., '5s', '2m', '3.5m', '3d'.")


@dataclass
class PostMessageParams:
    """
    Parameters required for posting a message to a Nzovu.

    Attributes:
    ----------
    message_id : str
        Unique identifier for the message.
    data : dict
        Payload data for the message.
    queue_name : str, optional (default="default_queue")
        Name of the queue to post the message to.
    options : PostMessageOptions, optional
        Optional settings for posting a message. If not provided, defaults will be used.
    """

    message_id: str
    data: dict
    queue_name: str
    options: Optional[PostMessageOptions] = field(default_factory=PostMessageOptions)


@dataclass
class AcknowledgeMessageParams:
    """
    Parameters required for acknowledging the processing status of a message in Nzovu.

    Attributes:
    ----------
    message_id : str
        Unique identifier for the message being acknowledged.
    queue_name : str, optional (default="default_queue")
        Name of the queue containing the message.
    state : MessageState
        Updated state for the message.
    worker_id : str, optional
        Optional stable identifier to consistently represent the same consumer.
    attempt_id : str, optional
        Attempt identifier to validate acknowledgment against current attempt.
    """

    message_id: str
    state: MessageState
    queue_name: str = "default_queue"
    worker_id: str = ""
    attempt_id: str = ""


@dataclass
class MessagePriorityRange:
    """
    Priority range for fetching messages from a Nzovu.

    Attributes:
    ----------
    min : int, optional (default=0)
        Minimum priority level (inclusive). Valid range: 0–4.
    max : int, optional (default=4)
        Maximum priority level (inclusive). Valid range: 0–4.
    """

    min: int = 0
    max: int = 4

    def __post_init__(self):
        validate_integer(self.min, "priority_range.min", 0, 4)
        validate_integer(self.max, "priority_range.max", 0, 4)
        if self.min > self.max:
            raise ValueError("priority_range.min cannot exceed max")


@dataclass
class PeekQueueMessagesParams:
    """
    Parameters required for peeking at messages in a Nzovu without dequeuing them.

    Attributes:
    ----------
    queue_name : str, optional (default="default_queue")
        Name of the queue to peek into.
    page_size : int, optional (default=5)
        Maximum number of messages to retrieve.
    page_token : str, optional
        Continuation token from the previous response.
    priority_range : MessagePriorityRange, optional
        Priority range for filtering the messages.
    """

    queue_name: str
    page_size: int = 5
    priority_range: Optional[MessagePriorityRange] = field(default_factory=MessagePriorityRange)
    page_token: str = ""


@dataclass
class QueueOptions:
    """
    Data class representing the options for creating a new queue in the Nzovu service.

    Attributes:
    ----------
    type : QueueType, default[SIMPLE]
        The type of queue to be created. It can be SIMPLE or EXCLUSIVE.

    max_attempts : Optional[int]
        The number of times a message can be dequeued before it is considered failed.

    lease_duration : Optional[str]
        The duration a message remains leased after being dequeued. Must be in format "[number]unit",
        for example: "5s", "2m", "3.5m", or "3d".

    exclusivity_key : Optional[str]
        The key used to ensure message exclusivity in the queue.

    dead_letter_queue_name : Optional[str]
        Name of the dead letter queue for messages that exhaust retries.

    auto_create_dlq : Optional[bool]
        If true, automatically create the DLQ if it doesn't exist.

    schema_id : Optional[str]
        Default schema for validating messages posted to this queue.

    schema_required : Optional[bool]
        If true, all messages must have a valid schema and pass validation.

    max_payload_size : Optional[int]
        Maximum size of message payload in bytes.

    allowed_content_types : Optional[list]
        List of permitted MIME types for message payloads.

    priority_config : Optional[dict]
        Advanced priority scheduling configuration.

    lease_policy : LeasePolicyOptions, optional
        Lease policy configuration for fine-grained control over message processing timeouts
        and heartbeat behavior at the queue level.
    """

    max_attempts: Optional[int] = None
    lease_duration: Optional[str] = None
    type: QueueType = QueueType.SIMPLE
    exclusivity_key: Optional[str] = ""
    dead_letter_queue_name: Optional[str] = ""
    auto_create_dlq: Optional[bool] = True
    schema_id: Optional[str] = ""
    schema_required: Optional[bool] = False
    max_payload_size: Optional[int] = 0
    allowed_content_types: Optional[list] = None
    priority_config: Optional[dict] = None
    lease_policy: Optional[LeasePolicyOptions] = None
    retention_policy: Optional[MessageRetentionPolicy] = None

    def __post_init__(self):
        duration_pattern = re.compile(r"^\d+(\.\d+)?[smhd]$")
        if self.lease_duration and not duration_pattern.match(self.lease_duration):
            raise ValueError("lease_duration must be in format '[number]unit', e.g., '5s', '2m', '3.5m', '3d'.")


@dataclass
class ScheduleOptions:
    """
    Data class representing the options for creating a schedule in the Nzovu service.

    Attributes:
    ----------
    payload : dict
        The payload data for messages created by this schedule.

    queue_name : str
        The name of the queue to post messages to.

    state : ScheduleState, default[SCHEDULED]
        The initial state of the schedule.

    cron_schedule : Optional[str]
        Cron expression for schedule timing (e.g., "0 0 * * *" for daily at midnight).
        Mutually exclusive with calendar_schedule.

    calendar_schedule : Optional[dict]
        Calendar-based schedule configuration as a dict.
        Mutually exclusive with cron_schedule.

    priority : Optional[int]
        Priority level for messages created by this schedule.

    max_messages : Optional[int]
        Maximum number of messages to create per schedule execution.

    lease_duration : Optional[str]
        Duration for message leases. Must be in format "[number]unit",
        for example: "5s", "2m", "3.5m", or "3d".

    """

    payload: dict
    queue_name: str
    state: ScheduleState = ScheduleState.SCHEDULED
    cron_schedule: Optional[str] = None
    calendar_schedule: Optional[dict] = None
    priority: Optional[int] = 0
    max_messages: Optional[int] = None
    lease_duration: Optional[str] = None

    headers: list[Header] = field(default_factory=list)
    data_metadata: Dict = field(default_factory=dict)
    content_type: str = ""
    schema_id: str = ""
    schema_version: int = 0

    def __post_init__(self):
        if self.priority is not None:
            validate_integer(self.priority, "priority", 0, 4)
        if self.cron_schedule and self.calendar_schedule:
            raise ValueError("Cannot specify both cron_schedule and calendar_schedule")
        if not self.cron_schedule and not self.calendar_schedule:
            raise ValueError("Must specify either cron_schedule or calendar_schedule")

        if self.lease_duration:
            duration_pattern = re.compile(r"^\d+(\.\d+)?[smhd]$")
            if not duration_pattern.match(self.lease_duration):
                raise ValueError("lease_duration must be in format '[number]unit', e.g., '5s', '2m', '3.5m', '3d'.")


@dataclass
class SchemaOptions:
    """
    Data class representing the options for registering a schema in the Nzovu service.

    Attributes:
    ----------
    name : str
        Human-readable schema name.

    description : str
        Schema description.

    content : str
        JSON Schema content (as a string).

    content_type : str, default="json-schema"
        Schema type (e.g., "json-schema").

    metadata : Optional[dict]
        Additional metadata as key-value pairs.
    """

    name: str
    description: str
    content: str
    content_type: str = "json-schema"
    metadata: Optional[dict] = field(default_factory=dict)


def _create_post_message_request(params: PostMessageParams) -> PostMessageRequest:
    require_name(params.queue_name, "queue_name")
    if not isinstance(params.message_id, str) or not re.fullmatch(r"[a-zA-Z0-9_-]{1,256}", params.message_id):
        raise ValueError("message_id must contain 1-256 ASCII letters, numbers, underscores or hyphens")
    options = params.options or PostMessageOptions()
    validate_integer(options.priority, "priority", 0, 4)
    payload = build_payload(
        params.data, options.data_metadata, options.content_type, options.schema_id, options.schema_version
    )
    metadata = Message.Metadata(
        payload=payload,
        state=options.state.name if isinstance(options.state, MessageState) else options.state,
        lease_duration=string_to_duration(options.lease_duration) if options.lease_duration is not None else None,
        max_attempts=options.max_attempts,
        priority=options.priority,
        headers=build_headers(options.headers),
    )
    policy = build_lease_policy(options.lease_policy)
    if policy is not None:
        metadata.lease_policy.CopyFrom(policy)
    if options.scheduled_time is not None:
        scheduled_time = Timestamp()
        scheduled_time.FromJsonString(options.scheduled_time)
        metadata.scheduled_time.CopyFrom(scheduled_time)
    return PostMessageRequest(
        queue_name=params.queue_name, message=Message(message_id=params.message_id, metadata=metadata)
    )


def build_bulk_request(queue_name, messages, transaction_mode):
    require_name(queue_name, "queue_name")
    if not 1 <= len(messages) <= 1000:
        raise ValueError("Bulk requests require 1-1000 messages")
    mode = transaction_mode.value if isinstance(transaction_mode, TransactionMode) else transaction_mode
    if mode not in {"ALL_OR_NOTHING", "BEST_EFFORT"}:
        raise ValueError(f"Invalid transaction_mode: {transaction_mode}")
    request = PostMessagesBulkRequest(queue_name=queue_name, transaction_mode=mode)
    for index, params in enumerate(messages):
        if params.queue_name != queue_name:
            raise ValueError(f"messages[{index}].queue_name must match queue_name='{queue_name}'")
        request.messages.append(_create_post_message_request(params).message)
    return request


def build_schedule_request(schedule_id, options):
    require_name(schedule_id, "schedule_id")
    require_name(options.queue_name, "queue_name")
    options.__post_init__()
    metadata = Schedule.Metadata(
        payload=build_payload(
            options.payload, options.data_metadata, options.content_type, options.schema_id, options.schema_version
        ),
        state=options.state.name,
        queue_name=options.queue_name,
        headers=build_headers(options.headers),
        priority=options.priority,
    )
    if options.cron_schedule:
        metadata.cron_schedule = options.cron_schedule
    else:
        metadata.calendar_schedule.CopyFrom(json_format.ParseDict(options.calendar_schedule, CalendarSchedule()))
    if options.max_messages is not None:
        metadata.has_max_messages = True
        metadata.max_messages = options.max_messages
    if options.lease_duration is not None:
        metadata.lease_duration.CopyFrom(string_to_duration(options.lease_duration))
    return CreateScheduleRequest(schedule=Schedule(schedule_id=schedule_id, metadata=metadata))


class ResponseWrapper:
    """
    A wrapper for gRPC protobuf responses that provides utility methods for converting
    the response to other formats, such as a dictionary, Pydantic model, and for accessing
    the raw protobuf response.

    The ResponseWrapper acts as a bridge between the raw gRPC protobuf response and more
    user-friendly formats. It supports three output modes:

    1. `.to_dict()` - Returns an untyped dictionary (legacy, always available)
    2. `.to_proto()` - Returns the raw protobuf object (for advanced use)
    3. `.to_model()` - Returns a typed Pydantic model (requires pydantic package)

    Args:
        response_protobuf: The gRPC protobuf response object.
            This is the raw response received from the gRPC service.
        converter_func: Optional callable that converts the protobuf response to a dictionary.
            If not provided, uses protobuf's built-in JSON conversion.

    Example:
        >>> response = client.get_next_message("my_queue", "5m")
        >>> # Legacy dict approach
        >>> data = response.to_dict()
        >>> msg_id = data.get("message", {}).get("messageId")
        >>>
        >>> # New typed model approach (requires pydantic)
        >>> msg = response.to_model()  # Returns GetNextMessageResponse
        >>> msg_id = msg.message.message_id  # Full IDE autocomplete!
    """

    # Map protobuf response types to Pydantic model classes
    _MODEL_MAP: Dict[str, Any] = {}

    def __init__(self, response_protobuf, converter_func: Optional[Callable] = None):
        """
        Initializes the ResponseWrapper with the provided protobuf response and converter function.
        """
        self.claim: Optional[Claim] = None
        self._response_protobuf = response_protobuf
        self._converter_func = converter_func

        # Lazy-load model map when first instance is created
        if not ResponseWrapper._MODEL_MAP and PYDANTIC_AVAILABLE:
            ResponseWrapper._MODEL_MAP = {
                # Queue and Message responses
                "CreateQueueResponse": models.CreateQueueResponse,
                "DeleteQueueResponse": models.DeleteQueueResponse,
                "ListQueuesResponse": models.ListQueuesResponse,
                "PostMessageResponse": models.PostMessageResponse,
                "PostMessagesBulkResponse": models.PostMessagesBulkResponse,
                "CancelMessageResponse": models.CancelMessageResponse,
                "GetNextMessageResponse": models.GetNextMessageResponse,
                "AcknowledgeMessageResponse": models.AcknowledgeMessageResponse,
                "RenewMessageLeaseResponse": models.RenewMessageLeaseResponse,
                "PeekQueueMessagesResponse": models.PeekQueueMessagesResponse,
                "GetQueueStateResponse": models.GetQueueStateResponse,
                "SendMessageHeartBeatResponse": models.SendMessageHeartBeatResponse,
                # Schedule responses
                "CreateScheduleResponse": models.CreateScheduleResponse,
                "DeleteScheduleResponse": models.DeleteScheduleResponse,
                "GetScheduleResponse": models.GetScheduleResponse,
                "ListSchedulesResponse": models.ListSchedulesResponse,
                "GetScheduleHistoryResponse": models.GetScheduleHistoryResponse,
                "PauseScheduleResponse": models.PauseScheduleResponse,
                "ResumeScheduleResponse": models.ResumeScheduleResponse,
                "ValidateCalendarScheduleResponse": models.ValidateCalendarScheduleResponse,
                "PreviewCalendarScheduleResponse": models.PreviewCalendarScheduleResponse,
                # Schema responses
                "RegisterSchemaResponse": models.RegisterSchemaResponse,
                "GetSchemaResponse": models.GetSchemaResponse,
                "ListSchemasResponse": models.ListSchemasResponse,
                "DeleteSchemaResponse": models.DeleteSchemaResponse,
                "ValidatePayloadResponse": models.ValidatePayloadResponse,
                # DLQ responses
                "GetDLQMessagesResponse": models.GetDLQMessagesResponse,
                "RequeueFromDLQResponse": models.RequeueFromDLQResponse,
                "DeleteFromDLQResponse": models.DeleteFromDLQResponse,
                "PurgeDLQResponse": models.PurgeDLQResponse,
                "GetDLQStatsResponse": models.GetDLQStatsResponse,
            }

    def to_dict(self) -> Dict:
        """
        Converts the wrapped protobuf response to a dictionary using the provided converter function.

        This is the legacy output format - returns an untyped dictionary. For better type safety
        and IDE support, consider using `.to_model()` instead (requires pydantic).

        Returns:
            dict: The converted dictionary representation of the protobuf response.

        Example:
            >>> response = client.create_queue("my_queue")
            >>> data = response.to_dict()
            >>> success = data.get("success")  # No type hints
        """
        if self._converter_func:
            return self._converter_func(response_protobuf=self._response_protobuf)
        else:
            return json_format.MessageToDict(self._response_protobuf)

    def to_proto(self) -> Any:
        """
        Retrieves the raw gRPC protobuf response.

        Use this when you need to access protobuf-specific functionality or pass
        the response to other protobuf-aware code.

        Returns:
            The raw gRPC protobuf response object.

        Example:
            >>> response = client.get_next_message("my_queue", "5m")
            >>> proto_msg = response.to_proto()
            >>> # Access protobuf methods
            >>> proto_msg.HasField("message")
        """
        return self._response_protobuf

    def to_model(self, model_class: Optional[Type[T]] = None) -> T:
        """
        Converts the response to a typed Pydantic model for better IDE support and validation.

        This method provides a type-safe interface to access response data with full
        IDE autocomplete support. The model class is auto-detected based on the response
        type, or you can specify it explicitly.

        Args:
            model_class: Optional Pydantic model class to use for conversion.
                        If not provided, auto-detects based on response type.

        Returns:
            A Pydantic model instance with type hints and validation.

        Raises:
            ImportError: If pydantic is not installed.
            ValueError: If no Pydantic model is registered for this response type.

        Example:
            >>> # Auto-detect model type
            >>> response = client.get_next_message("my_queue", "5m")
            >>> msg = response.to_model()  # Returns GetNextMessageResponse
            >>> print(msg.message.message_id)  # Full type hints!
            >>>
            >>> # Explicit model type
            >>> from nzovu.models import GetNextMessageResponse
            >>> msg = response.to_model(GetNextMessageResponse)
            >>>
            >>> # Export to JSON with validation
            >>> json_str = msg.model_dump_json()
            >>> dict_data = msg.model_dump()

        Note:
            Requires pydantic to be installed:
            ```bash
            pip install nzovu[pydantic]
            # or
            pip install pydantic
            ```
        """
        if not PYDANTIC_AVAILABLE:
            raise ImportError(
                "Pydantic is required for .to_model(). "
                "Install it with: pip install nzovu[pydantic] or pip install pydantic\n"
                "Alternatively, use .to_dict() for untyped dictionary output."
            )

        if model_class is None:
            # Auto-detect model class based on protobuf type
            proto_type_name = type(self._response_protobuf).__name__
            model_class = self._MODEL_MAP.get(proto_type_name)

            if model_class is None:
                available_types = ", ".join(sorted(self._MODEL_MAP.keys()))
                raise ValueError(
                    f"No Pydantic model registered for response type '{proto_type_name}'.\n"
                    f"Available types: {available_types}\n"
                    f"You can either:\n"
                    f"  1. Use .to_dict() for dictionary output\n"
                    f"  2. Specify model_class explicitly: .to_model(YourModel)\n"
                    f"  3. Request this model type to be added to nzovu"
                )

        # Convert using the model's from_proto method
        return model_class.from_proto(self._response_protobuf)  # type: ignore[union-attr]

    def __getattr__(self, name):
        """
        Delegates attribute access to the underlying protobuf object. This allows for direct access
        to the fields of the wrapped protobuf response.

        Args:
            name (str): The name of the attribute to access.

        Returns:
            The value of the specified attribute in the wrapped protobuf response.

        Example:
            >>> response = client.create_queue("my_queue")
            >>> # Direct attribute access (delegates to protobuf)
            >>> success = response.success
        """
        return getattr(self._response_protobuf, name)


PAGED_METHODS = frozenset(
    {"list_queues", "list_schedules", "list_schemas", "get_schedule_history", "get_dlq_messages", "peek_queue_messages"}
)


def page_call_arguments(method, args, kwargs, token):
    from dataclasses import replace

    kwargs = dict(kwargs)
    if method == "peek_queue_messages":
        params = args[0] if args else kwargs.pop("params")
        return (), dict(kwargs, params=replace(params, page_token=token))
    return args, dict(kwargs, page_token=token)


def validate_page_iterator(method, max_pages):
    if method not in PAGED_METHODS:
        raise ValueError("iter_pages requires a paginated SDK method name")
    if max_pages is not None and (type(max_pages) is not int or max_pages < 1):
        raise ValueError("max_pages must be a positive integer or None")

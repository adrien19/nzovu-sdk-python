"""
Chronoqueue Python SDK

A Python client library for interacting with the Chronoqueue distributed task queue service.

Basic Usage (Synchronous):
    >>> from chronoqueue import ChronoqueueClient
    >>> from chronoqueue.utils import PostMessageParams
    >>>
    >>> client = ChronoqueueClient(host='localhost', port=50051, use_tls=False)
    >>> client.create_queue(name="my_queue")
    >>> params = PostMessageParams(message_id="123", data={"key": "value"}, queue_name="my_queue")
    >>> response = client.post_message(params)
    >>> msg = response.to_dict()  # Legacy dict output
    >>> # Or with Pydantic (requires: pip install pydantic)
    >>> msg = response.to_model()  # Typed model output

Async Usage:
    >>> import asyncio
    >>> from chronoqueue import AsyncChronoqueueClient
    >>> from chronoqueue.utils import PostMessageParams
    >>>
    >>> async def main():
    ...     async with AsyncChronoqueueClient(host='localhost', port=50051, use_tls=False) as client:
    ...         msg = await client.get_next_message("queue", "5m", enable_heartbeat=True)
    ...         # Process message...
    ...         await client.acknowledge_message(params)
    >>> asyncio.run(main())

For more information, see the documentation at:
https://github.com/adrien19/chronoqueue-pythonsdk
"""

from .async_client import AsyncChronoqueueClient
from .client import ChronoqueueClient
from .exceptions import InitializationError, RpcOperationError
from .utils import (
    AcknowledgeMessageParams,
    LeasePolicyOptions,
    MessageRetentionPolicy,
    MessageState,
    PeekQueueMessagesParams,
    PostMessageOptions,
    PostMessageParams,
    QueueOptions,
    QueueType,
    ResponseWrapper,
    RetentionMode,
    ScheduleOptions,
    ScheduleState,
    SchemaOptions,
    TlsConfig,
    TransactionMode,
    generate_worker_id,
)

# Optionally export Pydantic models if available
try:
    from . import models
    from .models import (
        AcknowledgeMessageResponse,
        CreateQueueResponse,
        CreateScheduleResponse,
        DeleteFromDLQResponse,
        DeleteQueueResponse,
        DeleteScheduleResponse,
        DeleteSchemaResponse,
        GetDLQMessagesResponse,
        GetDLQStatsResponse,
        GetNextMessageResponse,
        GetQueueStateResponse,
        GetScheduleHistoryResponse,
        GetScheduleResponse,
        GetSchemaResponse,
        ListQueuesResponse,
        ListSchedulesResponse,
        ListSchemasResponse,
        Message,
        MessageMetadata,
        MessagePayload,
        PauseScheduleResponse,
        PeekQueueMessagesResponse,
        PostMessageResponse,
        PreviewCalendarScheduleResponse,
        PurgeDLQResponse,
        Queue,
        RegisterSchemaResponse,
        RenewMessageLeaseResponse,
        RequeueFromDLQResponse,
        ResumeScheduleResponse,
        Schedule,
        ScheduleHistoryEntry,
        ScheduleMetadata,
        Schema,
        SchemaInfo,
        SendMessageHeartBeatResponse,
        ValidateCalendarScheduleResponse,
        ValidatePayloadResponse,
        ValidationError,
    )

    __all__ = [
        # Clients
        "ChronoqueueClient",
        "AsyncChronoqueueClient",
        # Utils and params
        "TlsConfig",
        "PostMessageParams",
        "PostMessageOptions",
        "AcknowledgeMessageParams",
        "PeekQueueMessagesParams",
        "QueueOptions",
        "QueueType",
        "MessageState",
        "ScheduleOptions",
        "ScheduleState",
        "SchemaOptions",
        "LeasePolicyOptions",
        "ResponseWrapper",
        "generate_worker_id",
        # Exceptions
        "InitializationError",
        "RpcOperationError",
        # Pydantic models (optional) - Queue and Message
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
        "Message",
        "MessageMetadata",
        "MessagePayload",
        # Pydantic models (optional) - Schedule
        "CreateScheduleResponse",
        "DeleteScheduleResponse",
        "GetScheduleResponse",
        "ListSchedulesResponse",
        "GetScheduleHistoryResponse",
        "PauseScheduleResponse",
        "ResumeScheduleResponse",
        "ValidateCalendarScheduleResponse",
        "PreviewCalendarScheduleResponse",
        "Schedule",
        "ScheduleMetadata",
        "ScheduleHistoryEntry",
        # Pydantic models (optional) - Schema
        "RegisterSchemaResponse",
        "GetSchemaResponse",
        "ListSchemasResponse",
        "DeleteSchemaResponse",
        "ValidatePayloadResponse",
        "Schema",
        "SchemaInfo",
        "ValidationError",
        # Pydantic models (optional) - DLQ
        "GetDLQMessagesResponse",
        "RequeueFromDLQResponse",
        "DeleteFromDLQResponse",
        "PurgeDLQResponse",
        "GetDLQStatsResponse",
        "models",
    ]
except ImportError:
    # Pydantic not available - export only non-model types
    __all__ = [
        # Clients
        "ChronoqueueClient",
        "AsyncChronoqueueClient",
        # Utils and params
        "TlsConfig",
        "PostMessageParams",
        "PostMessageOptions",
        "AcknowledgeMessageParams",
        "PeekQueueMessagesParams",
        "QueueOptions",
        "QueueType",
        "MessageState",
        "ScheduleOptions",
        "ScheduleState",
        "SchemaOptions",
        "LeasePolicyOptions",
        "ResponseWrapper",
        "TransactionMode",
        "generate_worker_id",
        # Exceptions
        "InitializationError",
        "RpcOperationError",
    ]

# Dynamic version from package metadata
try:
    from importlib.metadata import version
    __version__ = version("chronoqueue")
except Exception:
    # Fallback for development environment or if package not installed
    __version__ = "0.1.0"

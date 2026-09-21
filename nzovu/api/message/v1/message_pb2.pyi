from google.protobuf import duration_pb2 as _duration_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from nzovu.api.google.api import field_behavior_pb2 as _field_behavior_pb2
from nzovu.api.common.v1 import common_pb2 as _common_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Message(_message.Message):
    __slots__ = ("message_id", "metadata")
    class Metadata(_message.Message):
        __slots__ = ("payload", "state", "attempts_left", "lease_duration", "lease_expiry", "lease_renewal_count", "priority", "max_attempts", "lease_policy", "current_attempt", "scheduled_time", "priority_level", "headers")
        class State(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
            __slots__ = ()
            INVISIBLE: _ClassVar[Message.Metadata.State]
            PENDING: _ClassVar[Message.Metadata.State]
            RUNNING: _ClassVar[Message.Metadata.State]
            COMPLETED: _ClassVar[Message.Metadata.State]
            CANCELED: _ClassVar[Message.Metadata.State]
            ERRORED: _ClassVar[Message.Metadata.State]
        INVISIBLE: Message.Metadata.State
        PENDING: Message.Metadata.State
        RUNNING: Message.Metadata.State
        COMPLETED: Message.Metadata.State
        CANCELED: Message.Metadata.State
        ERRORED: Message.Metadata.State
        class AttemptRuntime(_message.Message):
            __slots__ = ("attempt_id", "worker_id", "lease_started_at", "lease_expiry", "lease_extension_used", "lease_renewal_count", "last_heartbeat_at", "heartbeat_expiry")
            ATTEMPT_ID_FIELD_NUMBER: _ClassVar[int]
            WORKER_ID_FIELD_NUMBER: _ClassVar[int]
            LEASE_STARTED_AT_FIELD_NUMBER: _ClassVar[int]
            LEASE_EXPIRY_FIELD_NUMBER: _ClassVar[int]
            LEASE_EXTENSION_USED_FIELD_NUMBER: _ClassVar[int]
            LEASE_RENEWAL_COUNT_FIELD_NUMBER: _ClassVar[int]
            LAST_HEARTBEAT_AT_FIELD_NUMBER: _ClassVar[int]
            HEARTBEAT_EXPIRY_FIELD_NUMBER: _ClassVar[int]
            attempt_id: str
            worker_id: str
            lease_started_at: _timestamp_pb2.Timestamp
            lease_expiry: int
            lease_extension_used: _duration_pb2.Duration
            lease_renewal_count: int
            last_heartbeat_at: _timestamp_pb2.Timestamp
            heartbeat_expiry: int
            def __init__(self, attempt_id: _Optional[str] = ..., worker_id: _Optional[str] = ..., lease_started_at: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., lease_expiry: _Optional[int] = ..., lease_extension_used: _Optional[_Union[_duration_pb2.Duration, _Mapping]] = ..., lease_renewal_count: _Optional[int] = ..., last_heartbeat_at: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., heartbeat_expiry: _Optional[int] = ...) -> None: ...
        class Header(_message.Message):
            __slots__ = ("key", "value")
            KEY_FIELD_NUMBER: _ClassVar[int]
            VALUE_FIELD_NUMBER: _ClassVar[int]
            key: str
            value: bytes
            def __init__(self, key: _Optional[str] = ..., value: _Optional[bytes] = ...) -> None: ...
        PAYLOAD_FIELD_NUMBER: _ClassVar[int]
        STATE_FIELD_NUMBER: _ClassVar[int]
        ATTEMPTS_LEFT_FIELD_NUMBER: _ClassVar[int]
        LEASE_DURATION_FIELD_NUMBER: _ClassVar[int]
        LEASE_EXPIRY_FIELD_NUMBER: _ClassVar[int]
        LEASE_RENEWAL_COUNT_FIELD_NUMBER: _ClassVar[int]
        PRIORITY_FIELD_NUMBER: _ClassVar[int]
        MAX_ATTEMPTS_FIELD_NUMBER: _ClassVar[int]
        LEASE_POLICY_FIELD_NUMBER: _ClassVar[int]
        CURRENT_ATTEMPT_FIELD_NUMBER: _ClassVar[int]
        SCHEDULED_TIME_FIELD_NUMBER: _ClassVar[int]
        PRIORITY_LEVEL_FIELD_NUMBER: _ClassVar[int]
        HEADERS_FIELD_NUMBER: _ClassVar[int]
        payload: _common_pb2.Payload
        state: Message.Metadata.State
        attempts_left: int
        lease_duration: _duration_pb2.Duration
        lease_expiry: int
        lease_renewal_count: int
        priority: int
        max_attempts: int
        lease_policy: _common_pb2.LeasePolicy
        current_attempt: Message.Metadata.AttemptRuntime
        scheduled_time: _timestamp_pb2.Timestamp
        priority_level: int
        headers: _containers.RepeatedCompositeFieldContainer[Message.Metadata.Header]
        def __init__(self, payload: _Optional[_Union[_common_pb2.Payload, _Mapping]] = ..., state: _Optional[_Union[Message.Metadata.State, str]] = ..., attempts_left: _Optional[int] = ..., lease_duration: _Optional[_Union[_duration_pb2.Duration, _Mapping]] = ..., lease_expiry: _Optional[int] = ..., lease_renewal_count: _Optional[int] = ..., priority: _Optional[int] = ..., max_attempts: _Optional[int] = ..., lease_policy: _Optional[_Union[_common_pb2.LeasePolicy, _Mapping]] = ..., current_attempt: _Optional[_Union[Message.Metadata.AttemptRuntime, _Mapping]] = ..., scheduled_time: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., priority_level: _Optional[int] = ..., headers: _Optional[_Iterable[_Union[Message.Metadata.Header, _Mapping]]] = ...) -> None: ...
    MESSAGE_ID_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    message_id: str
    metadata: Message.Metadata
    def __init__(self, message_id: _Optional[str] = ..., metadata: _Optional[_Union[Message.Metadata, _Mapping]] = ...) -> None: ...

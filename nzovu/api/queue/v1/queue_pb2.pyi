from nzovu.api.common.v1 import common_pb2 as _common_pb2
from google.protobuf import duration_pb2 as _duration_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class QueueType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    SIMPLE: _ClassVar[QueueType]
    EXCLUSIVE: _ClassVar[QueueType]

class FairnessPolicy(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    STRICT: _ClassVar[FairnessPolicy]
    WEIGHTED: _ClassVar[FairnessPolicy]
    AGING: _ClassVar[FairnessPolicy]
    HYBRID: _ClassVar[FairnessPolicy]
SIMPLE: QueueType
EXCLUSIVE: QueueType
STRICT: FairnessPolicy
WEIGHTED: FairnessPolicy
AGING: FairnessPolicy
HYBRID: FairnessPolicy

class QueueMetadata(_message.Message):
    __slots__ = ("type", "default_max_attempts", "lease_duration", "exclusivity_key", "dead_letter_queue_name", "auto_create_dlq", "schema_id", "schema_required", "max_payload_size", "allowed_content_types", "priority_config", "lease_policy", "message_retention_policy")
    TYPE_FIELD_NUMBER: _ClassVar[int]
    DEFAULT_MAX_ATTEMPTS_FIELD_NUMBER: _ClassVar[int]
    LEASE_DURATION_FIELD_NUMBER: _ClassVar[int]
    EXCLUSIVITY_KEY_FIELD_NUMBER: _ClassVar[int]
    DEAD_LETTER_QUEUE_NAME_FIELD_NUMBER: _ClassVar[int]
    AUTO_CREATE_DLQ_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_ID_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_REQUIRED_FIELD_NUMBER: _ClassVar[int]
    MAX_PAYLOAD_SIZE_FIELD_NUMBER: _ClassVar[int]
    ALLOWED_CONTENT_TYPES_FIELD_NUMBER: _ClassVar[int]
    PRIORITY_CONFIG_FIELD_NUMBER: _ClassVar[int]
    LEASE_POLICY_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_RETENTION_POLICY_FIELD_NUMBER: _ClassVar[int]
    type: QueueType
    default_max_attempts: int
    lease_duration: _duration_pb2.Duration
    exclusivity_key: str
    dead_letter_queue_name: str
    auto_create_dlq: bool
    schema_id: str
    schema_required: bool
    max_payload_size: int
    allowed_content_types: _containers.RepeatedScalarFieldContainer[str]
    priority_config: PriorityConfig
    lease_policy: _common_pb2.LeasePolicy
    message_retention_policy: MessageRetentionPolicy
    def __init__(self, type: _Optional[_Union[QueueType, str]] = ..., default_max_attempts: _Optional[int] = ..., lease_duration: _Optional[_Union[_duration_pb2.Duration, _Mapping]] = ..., exclusivity_key: _Optional[str] = ..., dead_letter_queue_name: _Optional[str] = ..., auto_create_dlq: bool = ..., schema_id: _Optional[str] = ..., schema_required: bool = ..., max_payload_size: _Optional[int] = ..., allowed_content_types: _Optional[_Iterable[str]] = ..., priority_config: _Optional[_Union[PriorityConfig, _Mapping]] = ..., lease_policy: _Optional[_Union[_common_pb2.LeasePolicy, _Mapping]] = ..., message_retention_policy: _Optional[_Union[MessageRetentionPolicy, _Mapping]] = ...) -> None: ...

class MessageRetentionPolicy(_message.Message):
    __slots__ = ("mode", "retention_seconds")
    class Mode(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        DELETE_IMMEDIATELY: _ClassVar[MessageRetentionPolicy.Mode]
        RETAIN_DURATION: _ClassVar[MessageRetentionPolicy.Mode]
        RETAIN_FOREVER: _ClassVar[MessageRetentionPolicy.Mode]
    DELETE_IMMEDIATELY: MessageRetentionPolicy.Mode
    RETAIN_DURATION: MessageRetentionPolicy.Mode
    RETAIN_FOREVER: MessageRetentionPolicy.Mode
    MODE_FIELD_NUMBER: _ClassVar[int]
    RETENTION_SECONDS_FIELD_NUMBER: _ClassVar[int]
    mode: MessageRetentionPolicy.Mode
    retention_seconds: int
    def __init__(self, mode: _Optional[_Union[MessageRetentionPolicy.Mode, str]] = ..., retention_seconds: _Optional[int] = ...) -> None: ...

class PriorityConfig(_message.Message):
    __slots__ = ("policy", "priority_weights", "age_boost_threshold", "age_boost_multiplier")
    class PriorityWeightsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: int
        value: int
        def __init__(self, key: _Optional[int] = ..., value: _Optional[int] = ...) -> None: ...
    POLICY_FIELD_NUMBER: _ClassVar[int]
    PRIORITY_WEIGHTS_FIELD_NUMBER: _ClassVar[int]
    AGE_BOOST_THRESHOLD_FIELD_NUMBER: _ClassVar[int]
    AGE_BOOST_MULTIPLIER_FIELD_NUMBER: _ClassVar[int]
    policy: FairnessPolicy
    priority_weights: _containers.ScalarMap[int, int]
    age_boost_threshold: _duration_pb2.Duration
    age_boost_multiplier: int
    def __init__(self, policy: _Optional[_Union[FairnessPolicy, str]] = ..., priority_weights: _Optional[_Mapping[int, int]] = ..., age_boost_threshold: _Optional[_Union[_duration_pb2.Duration, _Mapping]] = ..., age_boost_multiplier: _Optional[int] = ...) -> None: ...

class Queue(_message.Message):
    __slots__ = ("name", "metadata")
    NAME_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    name: str
    metadata: QueueMetadata
    def __init__(self, name: _Optional[str] = ..., metadata: _Optional[_Union[QueueMetadata, _Mapping]] = ...) -> None: ...

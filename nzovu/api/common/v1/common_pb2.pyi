from google.protobuf import struct_pb2 as _struct_pb2
from google.protobuf import duration_pb2 as _duration_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Payload(_message.Message):
    __slots__ = ("metadata", "data", "content_type", "schema_id", "schema_version")
    class MetadataEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: _struct_pb2.Value
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[_struct_pb2.Value, _Mapping]] = ...) -> None: ...
    METADATA_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    CONTENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_ID_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_VERSION_FIELD_NUMBER: _ClassVar[int]
    metadata: _containers.MessageMap[str, _struct_pb2.Value]
    data: _struct_pb2.Struct
    content_type: str
    schema_id: str
    schema_version: int
    def __init__(self, metadata: _Optional[_Mapping[str, _struct_pb2.Value]] = ..., data: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ..., content_type: _Optional[str] = ..., schema_id: _Optional[str] = ..., schema_version: _Optional[int] = ...) -> None: ...

class LeasePolicy(_message.Message):
    __slots__ = ("base_lease", "max_extension", "heartbeat_timeout", "extend_step", "max_renewals")
    BASE_LEASE_FIELD_NUMBER: _ClassVar[int]
    MAX_EXTENSION_FIELD_NUMBER: _ClassVar[int]
    HEARTBEAT_TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    EXTEND_STEP_FIELD_NUMBER: _ClassVar[int]
    MAX_RENEWALS_FIELD_NUMBER: _ClassVar[int]
    base_lease: _duration_pb2.Duration
    max_extension: _duration_pb2.Duration
    heartbeat_timeout: _duration_pb2.Duration
    extend_step: _duration_pb2.Duration
    max_renewals: int
    def __init__(self, base_lease: _Optional[_Union[_duration_pb2.Duration, _Mapping]] = ..., max_extension: _Optional[_Union[_duration_pb2.Duration, _Mapping]] = ..., heartbeat_timeout: _Optional[_Union[_duration_pb2.Duration, _Mapping]] = ..., extend_step: _Optional[_Union[_duration_pb2.Duration, _Mapping]] = ..., max_renewals: _Optional[int] = ...) -> None: ...

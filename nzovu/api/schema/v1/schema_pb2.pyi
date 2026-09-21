from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ErrorCode(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    UNKNOWN_ERROR: _ClassVar[ErrorCode]
    REQUIRED_FIELD_MISSING: _ClassVar[ErrorCode]
    INVALID_TYPE: _ClassVar[ErrorCode]
    INVALID_FORMAT: _ClassVar[ErrorCode]
    PATTERN_MISMATCH: _ClassVar[ErrorCode]
    VALUE_OUT_OF_RANGE: _ClassVar[ErrorCode]
    ARRAY_LENGTH_INVALID: _ClassVar[ErrorCode]
    OBJECT_PROPERTIES_INVALID: _ClassVar[ErrorCode]
    ADDITIONAL_PROPERTIES_NOT_ALLOWED: _ClassVar[ErrorCode]
    ENUM_VALUE_INVALID: _ClassVar[ErrorCode]
    SCHEMA_NOT_FOUND: _ClassVar[ErrorCode]
    CONTENT_TYPE_INVALID: _ClassVar[ErrorCode]
    PAYLOAD_SIZE_EXCEEDED: _ClassVar[ErrorCode]
    SCHEMA_VALIDATION_FAILED: _ClassVar[ErrorCode]
UNKNOWN_ERROR: ErrorCode
REQUIRED_FIELD_MISSING: ErrorCode
INVALID_TYPE: ErrorCode
INVALID_FORMAT: ErrorCode
PATTERN_MISMATCH: ErrorCode
VALUE_OUT_OF_RANGE: ErrorCode
ARRAY_LENGTH_INVALID: ErrorCode
OBJECT_PROPERTIES_INVALID: ErrorCode
ADDITIONAL_PROPERTIES_NOT_ALLOWED: ErrorCode
ENUM_VALUE_INVALID: ErrorCode
SCHEMA_NOT_FOUND: ErrorCode
CONTENT_TYPE_INVALID: ErrorCode
PAYLOAD_SIZE_EXCEEDED: ErrorCode
SCHEMA_VALIDATION_FAILED: ErrorCode

class Schema(_message.Message):
    __slots__ = ("schema_id", "version", "name", "description", "content", "content_type", "created_at", "updated_at", "is_active", "metadata")
    class MetadataEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    SCHEMA_ID_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    CONTENT_FIELD_NUMBER: _ClassVar[int]
    CONTENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    IS_ACTIVE_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    schema_id: str
    version: int
    name: str
    description: str
    content: str
    content_type: str
    created_at: int
    updated_at: int
    is_active: bool
    metadata: _containers.ScalarMap[str, str]
    def __init__(self, schema_id: _Optional[str] = ..., version: _Optional[int] = ..., name: _Optional[str] = ..., description: _Optional[str] = ..., content: _Optional[str] = ..., content_type: _Optional[str] = ..., created_at: _Optional[int] = ..., updated_at: _Optional[int] = ..., is_active: bool = ..., metadata: _Optional[_Mapping[str, str]] = ...) -> None: ...

class ValidationResult(_message.Message):
    __slots__ = ("valid", "errors", "validated_at", "schema_id", "schema_version")
    VALID_FIELD_NUMBER: _ClassVar[int]
    ERRORS_FIELD_NUMBER: _ClassVar[int]
    VALIDATED_AT_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_ID_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_VERSION_FIELD_NUMBER: _ClassVar[int]
    valid: bool
    errors: _containers.RepeatedCompositeFieldContainer[ValidationError]
    validated_at: int
    schema_id: str
    schema_version: int
    def __init__(self, valid: bool = ..., errors: _Optional[_Iterable[_Union[ValidationError, _Mapping]]] = ..., validated_at: _Optional[int] = ..., schema_id: _Optional[str] = ..., schema_version: _Optional[int] = ...) -> None: ...

class ValidationError(_message.Message):
    __slots__ = ("field", "error_code", "message", "details")
    class DetailsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    FIELD_FIELD_NUMBER: _ClassVar[int]
    ERROR_CODE_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    DETAILS_FIELD_NUMBER: _ClassVar[int]
    field: str
    error_code: str
    message: str
    details: _containers.ScalarMap[str, str]
    def __init__(self, field: _Optional[str] = ..., error_code: _Optional[str] = ..., message: _Optional[str] = ..., details: _Optional[_Mapping[str, str]] = ...) -> None: ...

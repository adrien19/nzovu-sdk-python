"""Lossless Python values for typed protobuf responses."""

from google.protobuf import json_format
from google.protobuf.descriptor import FieldDescriptor


def protobuf_to_python(message):
    name = message.DESCRIPTOR.full_name
    if name in {"google.protobuf.Timestamp", "google.protobuf.Duration"}:
        return message.ToJsonString()
    if name in {"google.protobuf.Struct", "google.protobuf.Value", "google.protobuf.ListValue"}:
        return json_format.MessageToDict(message)
    result = {}
    for field in message.DESCRIPTOR.fields:
        if field.has_presence and not message.HasField(field.name):
            continue
        value = getattr(message, field.name)
        if field.label == FieldDescriptor.LABEL_REPEATED:
            if field.message_type and field.message_type.GetOptions().map_entry:
                entry = field.message_type.fields_by_name["value"]
                value = {key: field_value(entry, item) for key, item in value.items()}
            else:
                value = [field_value(field, item) for item in value]
        else:
            value = field_value(field, value)
        result[field.name] = value
    return result


def field_value(field, value):
    if field.type == FieldDescriptor.TYPE_MESSAGE:
        return protobuf_to_python(value)
    if field.type == FieldDescriptor.TYPE_ENUM:
        enum = field.enum_type.values_by_number.get(value)
        return enum.name if enum is not None else value
    return value

import hashlib
import importlib
import json
import pickle
import runpy
import subprocess
import sys
from importlib.resources import files
from pathlib import Path

import pytest
from google.protobuf import descriptor_pb2

from nzovu.api.queueservice.v1 import request_response_pb2, service_pb2, service_pb2_grpc

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("change", ["modified", "missing", "additional"])
def test_generator_rejects_source_drift(tmp_path, change):
    proto = tmp_path / "proto"
    proto.mkdir()
    source = proto / "test.proto"
    source.write_text('syntax = "proto3";\n')
    manifest = {"files": {"proto/test.proto": hashlib.sha256(source.read_bytes()).hexdigest()}}
    (proto / "SOURCE.json").write_text(json.dumps(manifest))
    verify = runpy.run_path(str(ROOT / "scripts/generate_proto.py"))["verify_sources"]
    verify.__globals__["ROOT"] = tmp_path
    assert verify() == ["proto/test.proto"]
    if change == "modified":
        source.write_text('syntax = "proto2";\n')
    elif change == "missing":
        source.unlink()
    else:
        (proto / "extra.proto").write_text('syntax = "proto3";\n')
    with pytest.raises(ValueError, match="Protocol sources differ"):
        verify()


def clear_json_names(message):
    for field in [*message.field, *message.extension]:
        field.ClearField("json_name")
    for nested in message.nested_type:
        clear_json_names(nested)


def test_generated_descriptors_match_pinned_sources(tmp_path):
    manifest = json.loads((ROOT / "proto/SOURCE.json").read_text())
    for name, checksum in manifest["files"].items():
        assert hashlib.sha256((ROOT / name).read_bytes()).hexdigest() == checksum
    descriptor_path = tmp_path / "source.pb"
    subprocess.run(
        [
            sys.executable,
            "-m",
            "grpc_tools.protoc",
            f"-I{ROOT}",
            f"-I{files('grpc_tools') / '_proto'}",
            f"--descriptor_set_out={descriptor_path}",
            *sorted(manifest["files"]),
        ],
        cwd=ROOT,
        check=True,
    )
    descriptors = descriptor_pb2.FileDescriptorSet.FromString(descriptor_path.read_bytes())
    assert len(descriptors.file) == len(manifest["files"])
    for expected in descriptors.file:
        module_name = (
            "nzovu.api." + expected.name.removeprefix("proto/").removesuffix(".proto").replace("/", ".") + "_pb2"
        )
        generated = importlib.import_module(module_name)
        actual = descriptor_pb2.FileDescriptorProto()
        generated.DESCRIPTOR.CopyToProto(actual)
        for descriptor in [expected, actual]:
            for extension in descriptor.extension:
                extension.ClearField("json_name")
            for message in descriptor.message_type:
                clear_json_names(message)
        assert actual == expected, expected.name


def test_stub_uses_only_nzovu_service_paths():
    class RecordingChannel:
        def __init__(self):
            self.paths = []

        def unary_unary(self, path, **kwargs):
            self.paths.append(path)
            return object()

    channel = RecordingChannel()
    service_pb2_grpc.QueueServiceStub(channel)
    service = service_pb2.DESCRIPTOR.services_by_name["QueueService"]
    assert service.full_name == "nzovu.api.queueservice.v1.QueueService"
    assert len(service.methods) == 31
    assert channel.paths == [f"/{service.full_name}/{method.name}" for method in service.methods]


def test_generated_types_have_importable_module_identity():
    request = request_response_pb2.ListQueuesRequest(page_size=25, page_token="next")
    assert request.__class__.__module__ == "nzovu.api.queueservice.v1.request_response_pb2"
    assert pickle.loads(pickle.dumps(request)) == request

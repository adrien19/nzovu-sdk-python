from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import grpc
import pytest

from nzovu.api.queueservice.v1 import service_pb2
from nzovu.exceptions import InitializationError
from nzovu.transport import AsyncRpcInterceptor, RpcOptions, SyncRpcInterceptor, create_channel, tls_credentials
from nzovu.utils import TlsConfig


@pytest.mark.parametrize("value", ["", "key\n", "key\x00", "clé", 123])
def test_invalid_keys_are_rejected_without_disclosure(value):
    with pytest.raises(InitializationError, match="api_key") as error:
        RpcOptions(api_key=value)
    if isinstance(value, str) and value:
        assert value not in str(error.value)


@pytest.mark.parametrize("timeout", [0, -1, float("inf"), float("nan"), True, "1"])
def test_invalid_deadlines(timeout):
    with pytest.raises(InitializationError, match="rpc_timeout"):
        RpcOptions(timeout=timeout)


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize(
    "method", [method.name for method in service_pb2.DESCRIPTOR.services_by_name["QueueService"].methods]
)
async def test_auth_and_deadline_apply_to_every_rpc(asynchronous, method):
    options = RpcOptions(api_key="test-secret", timeout=12)
    assert "test-secret" not in repr(options)
    details = SimpleNamespace(
        method=f"/nzovu.api.queueservice.v1.QueueService/{method}",
        timeout=None,
        metadata=[("trace", "123"), ("api-key", "stale")],
        credentials=None,
        wait_for_ready=True,
        compression=None,
    )
    continuation = AsyncMock(return_value="result") if asynchronous else Mock(return_value="result")
    interceptor = AsyncRpcInterceptor(options) if asynchronous else SyncRpcInterceptor(options)
    result = interceptor.intercept_unary_unary(continuation, details, "request")
    if asynchronous:
        result = await result
    assert result == "result"
    sent, request = continuation.call_args.args
    assert sent.metadata == (("trace", "123"), ("api-key", "test-secret"))
    assert sent.timeout == 12 and sent.method == details.method and sent.wait_for_ready is True
    assert request == "request"
    details.timeout = 3
    result = interceptor.intercept_unary_unary(continuation, details, "request")
    if asynchronous:
        await result
    assert continuation.call_args.args[0].timeout == 3


def test_tls_system_roots_and_server_only_ca(monkeypatch, tmp_path):
    credentials = Mock(return_value="credentials")
    monkeypatch.setattr(grpc, "ssl_channel_credentials", credentials)
    assert tls_credentials(None) == "credentials"
    credentials.assert_called_with(None, None, None)
    ca = tmp_path / "ca.pem"
    ca.write_bytes(b"ca")
    tls_credentials(TlsConfig(ca_path=str(ca)))
    credentials.assert_called_with(b"ca", None, None)


def test_mtls_reads_pair_and_rejects_partial_configuration(monkeypatch, tmp_path):
    credentials = Mock()
    monkeypatch.setattr(grpc, "ssl_channel_credentials", credentials)
    paths = []
    for name in ["ca", "cert", "key"]:
        path = tmp_path / name
        path.write_bytes(name.encode())
        paths.append(str(path))
    tls_credentials(TlsConfig(*paths))
    credentials.assert_called_once_with(b"ca", b"key", b"cert")
    with pytest.raises(InitializationError, match="both"):
        tls_credentials(TlsConfig(client_crt_path=paths[1]))
    with pytest.raises(InitializationError, match="Cannot read"):
        tls_credentials(TlsConfig(ca_path=str(tmp_path / "missing")))


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("use_tls", [False, True])
def test_channel_construction_always_installs_interceptor(monkeypatch, asynchronous, use_tls):
    factory = Mock(return_value="channel")
    module = grpc.aio if asynchronous else grpc
    monkeypatch.setattr(module, "secure_channel" if use_tls else "insecure_channel", factory)
    monkeypatch.setattr(grpc, "ssl_channel_credentials", Mock(return_value="credentials"))
    wrap = Mock(return_value="wrapped")
    monkeypatch.setattr(grpc, "intercept_channel", wrap)
    result = create_channel("localhost:9000", use_tls, None, RpcOptions(api_key="test"), asynchronous=asynchronous)
    assert factory.call_args.args[0] == "localhost:9000"
    if asynchronous:
        assert isinstance(factory.call_args.kwargs["interceptors"][0], AsyncRpcInterceptor)
        assert result == "channel"
    else:
        assert isinstance(wrap.call_args.args[1], SyncRpcInterceptor)
        assert result == "wrapped"


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("key", ["test-key", "wrong-key"])
async def test_api_key_and_deadline_reach_real_grpc_server(asynchronous, key):
    from concurrent.futures import ThreadPoolExecutor

    from nzovu import AsyncNzovuClient, NzovuClient
    from nzovu.api.queueservice.v1 import request_response_pb2 as rpc
    from nzovu.exceptions import RpcOperationError

    received = []

    def list_queues(request, context):
        metadata = dict(context.invocation_metadata())
        received.append((metadata, context.time_remaining()))
        if metadata.get("api-key") != "test-key":
            context.abort(grpc.StatusCode.UNAUTHENTICATED, "invalid API key")
        return rpc.ListQueuesResponse()

    with ThreadPoolExecutor(max_workers=1) as executor:
        server = grpc.server(executor)
        handler = grpc.unary_unary_rpc_method_handler(
            list_queues,
            request_deserializer=rpc.ListQueuesRequest.FromString,
            response_serializer=rpc.ListQueuesResponse.SerializeToString,
        )
        server.add_generic_rpc_handlers(
            (grpc.method_handlers_generic_handler("nzovu.api.queueservice.v1.QueueService", {"ListQueues": handler}),)
        )
        port = server.add_insecure_port("127.0.0.1:0")
        assert port > 0
        server.start()
        client_type = AsyncNzovuClient if asynchronous else NzovuClient
        client = client_type("127.0.0.1", port, use_tls=False, api_key=key, rpc_timeout=3)
        try:
            if asynchronous:
                await client.connect()
            if key == "wrong-key":
                with pytest.raises(RpcOperationError, match="invalid API key"):
                    result = client.list_queues()
                    if asynchronous:
                        await result
            else:
                result = client.list_queues()
                if asynchronous:
                    result = await result
                assert result.to_proto() == rpc.ListQueuesResponse()
            assert received[0][0]["api-key"] == key
            assert 0 < received[0][1] <= 3.1
        finally:
            if asynchronous:
                await client.close()
            else:
                client.close()
            assert server.stop(0).wait(2)

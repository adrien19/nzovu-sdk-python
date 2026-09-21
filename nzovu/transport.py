"""Shared authenticated gRPC transport for synchronous and asynchronous clients."""

import math
from collections import namedtuple
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import grpc

from .exceptions import InitializationError


@dataclass(frozen=True)
class RpcOptions:
    api_key: Optional[str] = field(default=None, repr=False)
    timeout: Optional[float] = None

    def __post_init__(self):
        if self.api_key is not None:
            if (
                not isinstance(self.api_key, str)
                or not self.api_key
                or any(not 32 <= ord(c) <= 126 for c in self.api_key)
            ):
                raise InitializationError("api_key must be non-empty printable ASCII metadata")
        if self.timeout is not None:
            if (
                isinstance(self.timeout, bool)
                or not isinstance(self.timeout, (int, float))
                or not math.isfinite(self.timeout)
                or self.timeout <= 0
            ):
                raise InitializationError("rpc_timeout must be a positive finite number")

    def metadata(self, existing):
        metadata = list(existing or ())
        if self.api_key is not None:
            metadata = [(key, value) for key, value in metadata if key.lower() != "api-key"]
            metadata.append(("api-key", self.api_key))
        return tuple(metadata)


class _CallDetails(
    namedtuple("CallDetails", "method timeout metadata credentials wait_for_ready compression"), grpc.ClientCallDetails
):
    pass


class SyncRpcInterceptor(grpc.UnaryUnaryClientInterceptor):
    def __init__(self, options: RpcOptions):
        self.options = options

    def intercept_unary_unary(self, continuation, client_call_details, request):
        details = client_call_details
        updated = _CallDetails(
            details.method,
            self.options.timeout if details.timeout is None else details.timeout,
            self.options.metadata(details.metadata),
            details.credentials,
            getattr(details, "wait_for_ready", None),
            getattr(details, "compression", None),
        )
        return continuation(updated, request)


class AsyncRpcInterceptor(grpc.aio.UnaryUnaryClientInterceptor):
    def __init__(self, options: RpcOptions):
        self.options = options

    async def intercept_unary_unary(self, continuation, client_call_details, request):
        details = client_call_details
        updated = grpc.aio.ClientCallDetails(
            details.method,
            self.options.timeout if details.timeout is None else details.timeout,
            self.options.metadata(details.metadata),
            details.credentials,
            details.wait_for_ready,
        )
        return await continuation(updated, request)


def tls_credentials(config):
    ca_path = config.ca_path if config is not None else None
    certificate_path = config.client_crt_path if config is not None else None
    key_path = config.client_key_path if config is not None else None
    if bool(certificate_path) != bool(key_path):
        raise InitializationError("mTLS requires both client_crt_path and client_key_path")

    def read(path):
        if path is None:
            return None
        try:
            return Path(path).read_bytes()
        except OSError as error:
            raise InitializationError(f"Cannot read TLS credential file: {path}") from error

    return grpc.ssl_channel_credentials(read(ca_path), read(key_path), read(certificate_path))


def create_channel(target, use_tls, tls_config, options: RpcOptions, *, asynchronous=False):
    credentials = tls_credentials(tls_config) if use_tls else None
    if asynchronous:
        interceptors = [AsyncRpcInterceptor(options)]
        if use_tls:
            return grpc.aio.secure_channel(target, credentials, interceptors=interceptors)
        return grpc.aio.insecure_channel(target, interceptors=interceptors)
    channel = grpc.secure_channel(target, credentials) if use_tls else grpc.insecure_channel(target)
    return grpc.intercept_channel(channel, SyncRpcInterceptor(options))

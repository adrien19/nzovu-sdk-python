"""Opt-in real Nzovu TLS and ownership gates; see scripts/run_live_ownership.py."""

import asyncio
import os
import time
from pathlib import Path
from uuid import uuid4

import grpc
import pytest

from nzovu import (
    AsyncNzovuClient,
    Claim,
    LeasePolicyOptions,
    MessageState,
    NzovuClient,
    PostMessageParams,
    QueueOptions,
    RpcOperationError,
    TlsConfig,
)
from tests.test_ownership import invoke, until

pytestmark = pytest.mark.skipif(not os.environ.get("NZOVU_LIVE_CONFIG"), reason="requires live Nzovu fixture")


async def connect(live, asynchronous, mode="tls", credentials="valid", key="valid", ca="ca", plaintext=False):
    directory = Path(live["directory"])
    config = TlsConfig(ca_path=str(directory / f"{ca}.crt"))
    if mode == "mtls" and credentials != "missing":
        name = "client" if credentials == "valid" else "wrong-client"
        config.client_crt_path = str(directory / f"{name}.crt")
        config.client_key_path = str(directory / f"{name}.key")
    c = (AsyncNzovuClient if asynchronous else NzovuClient)(
        live["host"],
        port=live[mode],
        use_tls=not plaintext,
        tls_config=config,
        api_key=live["api_key"] if key == "valid" else (None if key == "missing" else "wrong-key"),
        rpc_timeout=2,
        heartbeat_interval=0.05,
    )
    if asynchronous:
        await c.connect()
    return c


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize(
    "mode,credentials,key,ca,plaintext,status",
    [
        ("tls", "valid", "valid", "ca", False, None),
        ("mtls", "valid", "valid", "ca", False, None),
        ("tls", "valid", "wrong", "ca", False, grpc.StatusCode.UNAUTHENTICATED),
        ("tls", "valid", "missing", "ca", False, grpc.StatusCode.UNAUTHENTICATED),
        ("tls", "valid", "valid", "wrong-ca", False, grpc.StatusCode.UNAVAILABLE),
        ("mtls", "missing", "valid", "ca", False, grpc.StatusCode.UNAVAILABLE),
        ("mtls", "wrong", "valid", "ca", False, grpc.StatusCode.UNAVAILABLE),
        ("tls", "valid", "valid", "ca", True, grpc.StatusCode.UNAVAILABLE),
    ],
)
async def test_live_auth(live, asynchronous, mode, credentials, key, ca, plaintext, status):
    c = await connect(live, asynchronous, mode, credentials, key, ca, plaintext)
    try:
        if status is None:
            assert (await invoke(c.list_queues)).to_proto() is not None
        else:
            with pytest.raises(RpcOperationError) as error:
                await invoke(c.list_queues)
            assert error.value.code() == status
    finally:
        await invoke(c.close)


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("mode", ["tls", "mtls"])
async def test_live_claim_lifecycle(live, asynchronous, mode):
    c = await connect(live, asynchronous, mode)
    queues = ["sdk_" + uuid4().hex for _ in range(2)]
    owners = []
    try:
        for queue in queues:
            await invoke(
                c.create_queue,
                queue,
                QueueOptions(
                    lease_policy=LeasePolicyOptions(
                        base_lease="2s", max_extension="300s", heartbeat_timeout="1s", extend_step="2s"
                    )
                ),
            )
            await invoke(c.post_message, PostMessageParams("same_message", {"value": 1}, queue))
            response = await invoke(c.get_next_message, queue, "2s", enable_heartbeat=True)
            owner = response.claim
            assert owner.queue_name == queue and owner.worker_id and owner.attempt_id
            assert response.to_dict()["workerId"] == owner.worker_id
            owners.append(owner)
        assert set(c.get_active_heartbeats()) == set(owners)
        await asyncio.sleep(2.2)
        for owner in owners:
            assert c.get_heartbeat_stats()[owner]["heartbeats_sent"] > 0
            with pytest.raises(RpcOperationError) as rejected:
                await invoke(c.acknowledge_message, owner.acknowledge(MessageState.RUNNING))
            assert rejected.value.code() == grpc.StatusCode.INVALID_ARGUMENT
            assert not c._heartbeats.workers[owner].stop.is_set()
            stale = Claim(owner.queue_name, owner.message_id, owner.worker_id, "wrong-attempt")
            with pytest.raises(RpcOperationError) as error:
                await invoke(c.acknowledge_message, stale.acknowledge(MessageState.COMPLETED))
            assert error.value.code() == grpc.StatusCode.FAILED_PRECONDITION
            assert owner in c.get_active_heartbeats()
            await invoke(c.renew_message_lease, **owner.to_dict(), new_lease_duration="2s")
            await invoke(c.acknowledge_message, owner.acknowledge(MessageState.COMPLETED))
        await until(lambda: c.get_active_heartbeat_count() == 0)
    finally:
        await invoke(c.close)


@pytest.mark.parametrize("asynchronous", [False, True])
async def test_live_expiry_reclaim_rejects_stale_owner(live, asynchronous):
    c = await connect(live, asynchronous)
    queue = "expiry_" + uuid4().hex
    try:
        await invoke(
            c.create_queue,
            queue,
            QueueOptions(
                lease_policy=LeasePolicyOptions(
                    base_lease="1s", max_extension="30s", heartbeat_timeout="1s", extend_step="1s"
                )
            ),
        )
        await invoke(c.post_message, PostMessageParams("reclaimed", {"value": 1}, queue))
        old = (await invoke(c.get_next_message, queue, "1s")).claim
        deadline = time.monotonic() + 30
        replacement = None
        while replacement is None:
            await asyncio.sleep(0.3)
            try:
                replacement = (await invoke(c.get_next_message, queue, "10s", enable_heartbeat=True)).claim
            except RpcOperationError as error:
                assert error.code() == grpc.StatusCode.NOT_FOUND
            assert time.monotonic() < deadline, "message was not reclaimed"
        assert old.attempt_id != replacement.attempt_id
        for method, kwargs in [
            (c.acknowledge_message, {"params": old.acknowledge(MessageState.COMPLETED)}),
            (c.send_message_heartbeat, old.to_dict()),
            (c.renew_message_lease, dict(**old.to_dict(), new_lease_duration="1s")),
        ]:
            with pytest.raises(RpcOperationError) as error:
                await invoke(method, **kwargs)
            assert error.value.code() == grpc.StatusCode.FAILED_PRECONDITION
        assert replacement in c.get_active_heartbeats()
        await invoke(c.acknowledge_message, replacement.acknowledge(MessageState.COMPLETED))
        await until(lambda: c.get_active_heartbeat_count() == 0)
    finally:
        await invoke(c.close)


@pytest.mark.parametrize("asynchronous", [False, True])
async def test_live_renewal_limit_retains_heartbeat(live, asynchronous):
    c = await connect(live, asynchronous)
    queue = "limit_" + uuid4().hex
    try:
        await invoke(
            c.create_queue,
            queue,
            QueueOptions(
                lease_policy=LeasePolicyOptions(
                    base_lease="10s", max_extension="30s", heartbeat_timeout="2s", extend_step="1s", max_renewals=1
                )
            ),
        )
        await invoke(c.post_message, PostMessageParams("limit", {"value": 1}, queue))
        owner = (await invoke(c.get_next_message, queue, "10s", enable_heartbeat=True)).claim
        await until(lambda: c.get_heartbeat_stats()[owner]["heartbeats_sent"] >= 2)
        with pytest.raises(RpcOperationError) as error:
            await invoke(c.renew_message_lease, **owner.to_dict(), new_lease_duration="1s")
        assert error.value.code() == grpc.StatusCode.FAILED_PRECONDITION
        assert not c._heartbeats.workers[owner].stop.is_set()
        await invoke(c.acknowledge_message, owner.acknowledge(MessageState.COMPLETED))
        await until(lambda: c.get_active_heartbeat_count() == 0)
    finally:
        await invoke(c.close)

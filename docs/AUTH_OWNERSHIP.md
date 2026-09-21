# Authentication and claim ownership

Both clients default to verified TLS on port 9000. Set `api_key` for the server's
`api-key` metadata and `rpc_timeout` for a default deadline on every unary RPC.
Use `TlsConfig(ca_path=...)` for a private CA; otherwise system roots apply.
Optional `client_crt_path` and `client_key_path` must be supplied together for
mTLS. Plaintext requires explicit `use_tls=False`.

```python
from nzovu import NzovuClient, TlsConfig, MessageState

client = NzovuClient(
    "queue.example.com",
    api_key="your-key",
    rpc_timeout=10,
    tls_config=TlsConfig(ca_path="ca.pem"),
)
try:
    response = client.get_next_message("orders", "30s", enable_heartbeat=True)
    claim = response.claim
    if claim is not None:
        # Process response.to_proto().message or response.to_model().message.
        client.acknowledge_message(claim.acknowledge(MessageState.COMPLETED))
finally:
    client.close()
```

`AsyncNzovuClient` accepts the same transport options. Use `await connect()` or
`async with`; await its RPC and `close()` methods. Repeated `connect()` is
idempotent. A closed client cannot reconnect; create a new instance.

## Explicit claims

Every successful `get_next_message` exposes an immutable `response.claim`, even
when automatic heartbeat is disabled. It contains `queue_name`, `message_id`,
`worker_id` and `attempt_id`. Empty responses have `claim=None` (the server can
also report `NOT_FOUND`). `claim.to_dict()` retains all four identifiers.
Protobuf, JSON/dict and optional typed response forms retain the returned
worker/attempt IDs. The wrapper adds the queue context from the request.

ACK, manual heartbeat and lease renewal require the exact original ownership:

```python
client.send_message_heartbeat(**claim.to_dict())
client.renew_message_lease(**claim.to_dict(), new_lease_duration="30s")
client.acknowledge_message(claim.acknowledge(MessageState.COMPLETED))
client.stop_heartbeat(claim)
```

Equivalent explicit identifiers are accepted. Missing IDs fail before RPC;
there is no lookup that could substitute a newer attempt. `stop_heartbeat`
requires a `Claim`, not a message ID. This is an intentional first-release API
change. A failed or timed-out ACK is not proof of completion; reconcile its
outcome before retrying business effects. No non-idempotent RPC is retried by
the SDK.

## Bounded heartbeat lifecycle

- Sync capacity: `heartbeat_thread_pool_size` (default 20). Async capacity:
  `heartbeat_task_limit` (default 20). Admission is reserved before claiming;
  saturation raises `HeartbeatCapacityError` without fetching another message.
- Workers are keyed by the complete immutable claim. Identical message IDs in
  different queues and replacement attempts cannot stop or remove each other.
- ACK keeps its heartbeat until `success=True`; transient errors and false
  success responses retain it. Terminal `NOT_FOUND`, `FAILED_PRECONDITION` or
  `PERMISSION_DENIED` stops only the matching claim. Renewal
  `FAILED_PRECONDITION` can mean an extension limit; it retains heartbeat until
  ACK or a heartbeat confirms ownership loss.
- Automatic heartbeat RPCs have deadlines capped at 5 seconds (or the smaller
  configured timeout/remaining worker lifetime). `heartbeat_interval` defaults
  to 1 second; configure it below your server heartbeat timeout/lease duration.
- Only automatic heartbeats retry `UNAVAILABLE`, `DEADLINE_EXCEEDED` and
  `RESOURCE_EXHAUSTED`, with interruptible 2/4-second backoff and three failed
  attempts maximum. Other failures stop the worker. A server state other than
  `RUNNING` also ends it. Duration/count limits remain configurable.
- `get_active_heartbeats()` and `get_heartbeat_stats()` return copied mappings
  keyed by `Claim`; completed workers are removed, keeping memory bounded.
- `close()` blocks new claims, signals workers, closes the channel to cancel
  in-flight RPCs, then joins threads or cancels/awaits tasks. Retries wake
  immediately. Repeated close is safe. Error callbacks run inline and must be
  brief/nonblocking; Python cannot forcibly terminate a blocked callback.
  A sync shutdown timeout raises instead of reporting success while work remains.

On crash, heartbeats stop and the server eventually reclaims the lease. This
is not an exactly-once processing guarantee; make business effects idempotent.

`RpcOperationError.code()`, `.details()` and `.trailing_metadata()` preserve
the original gRPC status and rich trailing details. The original exception is
also available as `.rpc_error` and `.__cause__`, including in custom handlers.
API keys are excluded from transport-option repr and configuration errors.

## Validation

Unit/loopback tests exercise authenticated transport for all 31 methods, default
and overridden deadlines, explicit ownership, failed ACK, stale attempts,
saturation, retry recovery, cancellation, max count/duration and joined shutdown.
Base installs run the same suite against an installed wheel without Pydantic.

The opt-in live gate starts two isolated SQLite servers with generated temporary
TLS/mTLS certificates and API-key authentication, then checks both clients:
valid credentials, wrong/missing key, untrusted server CA, wrong/missing client
certificate, rejected plaintext, automatic authenticated heartbeat, failed and
successful ACK, duplicate IDs across queues, expiry/reclaim and stale ownership.

Build the pinned server commit recorded in `proto/SOURCE.json` with SQLite:

```bash
# Run in that server checkout.
CGO_ENABLED=1 go build -tags sqlite -o /tmp/nzovu-sdk-server .
# Run in this SDK checkout (requires openssl and the locked dev environment).
make check-live-ownership NZOVU_SERVER_BINARY=/tmp/nzovu-sdk-server
```

The harness prints temporary fixture/log paths and always shuts down its servers.
For Docker Desktop, use `--host host.docker.internal`, mount the fixture's temp
root into the container at the same path, and forward `NZOVU_LIVE_CONFIG`.
PostgreSQL, the broader live API matrix and other Python/OS versions remain
SDK-PR4 gates. Publishing remains disabled.

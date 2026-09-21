# Python SDK API contracts

SDK-PR2 aligns the 31 synchronous/asynchronous RPC wrappers and optional response
models with the pinned server protocol. Protocol sources are unchanged from PR1.
The server revision is `f21477ecab47197f0c8c92dbdde2aab11ef02600`.
See [authentication and ownership](AUTH_OWNERSHIP.md) for SDK-PR3 transport/worker behavior; broader live
server compatibility testing remains the SDK-PR4 release gate.

## Requests

Both clients expose the same RPC parameters. Request builders are shared for
single/bulk messages and schedules. The SDK does not retry RPCs automatically.
Server RPC errors raise `RpcOperationError`, or invoke an explicit error handler.
Local invalid inputs raise `ValueError`; bulk validation retains the existing
`RpcOperationError` wrapper. No request is sent after local validation fails.

- Priorities are integers 0–4; peek ranges must be ordered and within that range.
- Message IDs use 1–256 ASCII letters, digits, underscores or hyphens, matching
  the server validator. IDs are required; duplicate detection remains server-side.
- Payload data/metadata accept JSON objects, including nested values and nulls.
  Content type is JSON (`application/json`, `application/x-json`, or server default).
  Schema ID/version are preserved. Struct payload numbers follow protobuf's
  double representation; use strings for business integers beyond exact double range.
- `Header(key, value)` carries opaque bytes. Order and duplicate keys are retained.
  Keys allow lowercase ASCII letters, digits and hyphens. Prefixes `x-nzovu-`,
  `x-internal-`, `x-system-` are reserved. Values are at most 4096 bytes; keys plus
  values total at most 32768 bytes. Headers are available on messages and schedules.
- Durations accept `s`, `m`, `h`, `d` with exact nanosecond precision. Excess
  precision/range is rejected. Scheduled message time accepts RFC 3339 strings.
- An unset message `lease_duration` inherits the server/queue policy; an explicit
  `"0s"` remains present. `LeasePolicyOptions(max_renewals=None)` inherits;
  explicit zero disables a renewal cap. The SDK preserves both cases.
- Bulk requests contain 1–1000 items targeting the same queue. `ALL_OR_NOTHING`
  and `BEST_EFFORT` results retain overall status, counts and ordered per-item
  status/error codes. Server-dependent errors (duplicates/schema validation)
  remain server decisions; no implicit retries or client-side success synthesis.
- DLQ requeue requires an explicit existing `target_queue`.
- Schema registration uses `json-schema`; version zero means latest for reads
  and all versions for deactivation. Schema lists contain family summaries.
- Calendar configuration uses `calendar_schedule.timezone`. The deprecated
  schedule-level timezone request option is removed. Deprecated response fields
  remain readable; execution records are the durable history source.

```python
from nzovu import Header, LeasePolicyOptions, PostMessageOptions, PostMessageParams

params = PostMessageParams(
    message_id="order_123",
    queue_name="orders",
    data={"order_id": "123"},
    options=PostMessageOptions(
        priority=4,
        headers=[Header("trace-id", b"trace-123")],
        data_metadata={"source": "checkout", "retry": False},
        schema_id="order",
        schema_version=2,
        lease_policy=LeasePolicyOptions(max_renewals=0),
    ),
)
```

## Pagination

Queue/schedule/schema listing, message peek, schedule history and DLQ listing
accept `page_size` (0–1000; zero selects the server default) and `page_token`.
The SDK returns one page per ordinary method call. Filters must remain unchanged
when following a token; the server validates token scope.

`iter_pages("list_queues", prefix="orders", max_pages=10)` yields response
wrappers lazily. The async client provides an async iterator with the same
arguments. Fetching stops on an empty token, the page bound, or an RPC error.
Repeated tokens raise rather than loop indefinitely. Peek parameter objects are
copied when advancing, so caller input is not mutated.

## Response representations

- `to_proto()` returns the original protobuf response.
- `to_dict()` preserves protobuf JSON conventions: camelCase keys, base64 binary
  fields, and decimal strings for int64 fields. It does not narrow large values.
- `to_model()` requires `nzovu[pydantic]`: snake_case fields, bytes for headers,
  Python integers for counts, and precise RFC 3339/protobuf duration strings.
  Absent message/optional scalar fields remain `None`; explicitly present zero
  and empty strings remain distinguishable. Known enums use names; unknown
  numeric enum values survive conversion.

Every RPC has a response model, including bulk posting and cancellation.
False success flags remain false. Message payloads include data, metadata,
content type and schema identity. Claim worker/attempt IDs and runtime fields
are retained even when automatic heartbeat is disabled.

Schedule payloads now use `MessagePayload`, matching message payloads.
`GetScheduleHistoryResponse.schedule_history` holds the protocol's nested history,
including `executions` (message ID, exact execution time, success/error and message
snapshot), timestamps and messages. This replaces the old flattened history model.

## Validation

The suite covers all 31 request encodings, sync/async parameter parity, RPC errors,
custom handlers, all response representations, every populated nested response
field, optional-field presence, large integers, binary headers, zero timestamps,
priority/page/header boundaries and lazy pagination. A separate isolated run
installs the wheel without Pydantic and executes the same suite; only typed-model
checks are skipped there, while every RPC still exercises protobuf/dict/error paths.

```bash
make test
make check-proto
make lint
make typecheck
make format FORMAT_FLAGS=--check
make build
make check-base-tests
make check-artifacts
```

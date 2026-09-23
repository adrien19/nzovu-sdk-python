# Nzovu Python SDK

Python distribution/import: **`nzovu`**. Repository: **`nzovu-sdk-python`**.

This checkout prepares `0.0.1`. Both clients implement all 31 RPCs, authenticated transport, explicit claims and
bounded worker lifecycle. SDK-PR4 adds live backend checks, migrated examples and
installed-distribution validation. Publication remains pending the review and CI gates recorded in [migration status](docs/MIGRATION_STATUS.md).

## Development

Use Python 3.12 and Poetry 2.3.1, or open this repository's devcontainer.

```bash
make install-dev
make check-proto
make test
make lint
make typecheck
make format FORMAT_FLAGS=--check
make build
make check-artifacts
make check-base-tests
make check-identity
```

After installation:

```python
from nzovu import AsyncNzovuClient, NzovuClient
from nzovu.api.queueservice.v1 import request_response_pb2, service_pb2_grpc
```

The generated `QueueServiceStub` uses the pinned `nzovu.api.*` protocol. The
server's default gRPC port is 9000. Protocol provenance/checksums are recorded
in [proto/SOURCE.json](proto/SOURCE.json); generation never fetches a moving branch.

```bash
make gen-proto
make check-proto
```

`make update-proto` requires an explicit local server checkout and full commit
SHA; see [CONTRIBUTING.md](CONTRIBUTING.md). Review and regenerate together.

The future install names are `pip install nzovu` and `pip install 'nzovu[pydantic]'`.
For now, build/install the local wheel. Optional Pydantic models remain available;
See [API contracts](docs/API_CONTRACTS.md) for payloads, headers, pagination and
typed responses and [authentication/ownership](docs/AUTH_OWNERSHIP.md) for secure
connections, claims and bounded workers; remaining release gates are in [migration status](docs/MIGRATION_STATUS.md).

Development versions start at `0.0.1.dev0`, with `0.0.1rc1` candidates and an
intended first final version `0.0.1`. `nzovu.__version__` reads installed metadata;
an uninstalled source checkout reports `0+unknown`. No legacy import aliases are
provided. Publishing remains disabled pending SDK-PR5.

Paged methods accept `page_size` and `page_token`; `limit` is removed.
Read `next_page_token` from protobuf/Pydantic responses, or `nextPageToken`
from `.to_dict()`, and pass it to the next request. An empty token ends paging.
Schedule options no longer accept `exclusivity_key`.

The original MIT license is retained. Source ancestry and bootstrap validation
are recorded in [the baseline](docs/MIGRATION_BASELINE.md).

## Examples and compatibility

The [store API](examples/store-api/README.md) supports sync and async clients with
shared client shutdown, claim-aware workers and PostgreSQL Compose fixtures.
[Bulk posting](examples/bulk.py) demonstrates atomic and best-effort outcomes.

The installed-artifact matrix covers Python 3.10–3.14, wheel/sdist, base/Pydantic,
and minimum/latest runtime dependencies. Linux live tests exercise SQLite and
PostgreSQL; macOS/Windows jobs exercise installed unit and transport tests.
See migration status for validation results and remaining gates, and
[CONTRIBUTING.md](CONTRIBUTING.md) for repeatable commands.

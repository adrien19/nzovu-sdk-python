# Contributing to the Nzovu Python SDK

Work on feature branches from `main`. Commit, push and publish only with explicit
user authorization. This independent repository preserves the original SDK history.

## Environment and checks

Use Python 3.12 and Poetry 2.3.1. The devcontainer pins both and installs the
committed lockfile without host Docker socket access or automatically starting
services. The Python 3.10–3.14 installed-artifact matrix is a compatibility gate.

```bash
make install-dev
make test
make lint
make typecheck
make format FORMAT_FLAGS=--check
make build
make check-artifacts
make check-base-tests
```

Commands run in the project `.venv` through Poetry. `make install` selects only
base runtime dependencies; `make install-dev` includes tools and Pydantic.
Dependency updates are explicit: update `pyproject.toml`, run `make lock`, review
the lockfile, then validate. Keep generator versions pinned and generated files
outside formatter/linter scope. Use `make format` to format handwritten Python.
Lint and typecheck failures are fatal; do not suppress them to pass CI.

## Protocol updates

`proto/SOURCE.json` records the exact Nzovu server commit and SHA-256 hashes.
Protocol updates must come from a reviewed local server checkout:

```bash
make update-proto NZOVU_SERVER_SOURCE=/path/to/nzovu NZOVU_SERVER_COMMIT=<full-commit-sha>
make gen-proto
make check-proto
```

The update validates the explicit commit and replaces vendored protocol sources.
Generation validates all source hashes, runs pinned tools, and relocates only
Python imports/module identities under `nzovu.api`; canonical server descriptor
filenames and gRPC service names remain intact. A second generation must be
byte-identical. Review the manifest, protocol and generated code together.
Never edit generated modules by hand or refresh protos implicitly during a build.

## Tests and releases

Cover successful and rejected inputs, sync/async parity and ownership lifecycle.
Contract tests compare generated descriptors with the pinned server definitions.
Wheel/sdist installation tests must run outside the source checkout, with and
without the Pydantic extra. `make check-artifacts` creates isolated temporary
environments for those checks and requires access to the dependency index.
Live server integration is required before release.

Package version starts at `0.0.1.dev0`; old source-package tags are not new SDK
releases. Publishing workflows and Make targets remain disabled until SDK-PR5.
See [migration status](docs/MIGRATION_STATUS.md) for remaining contract work and
[bootstrap baseline](docs/MIGRATION_BASELINE.md) for inherited validation results.

## Authenticated ownership gate

`make check-live-ownership NZOVU_SERVER_BINARY=/absolute/path/to/nzovu` runs
both clients against temporary SQLite TLS/mTLS servers. Build the pinned server
with `CGO_ENABLED=1 go build -tags sqlite`; OpenSSL supplies test certificates.
See [authentication and ownership](docs/AUTH_OWNERSHIP.md) for Docker execution,
explicit claims, deadlines and worker limits. Normal unit runs skip these live
cases; execute this gate separately before approving transport/lifecycle changes.

## SDK-PR4 compatibility gate

Install the example's separate lockfile before checking its tests:

```bash
(cd examples/store-api && poetry sync --with dev)
make test-examples check-identity
make test-coverage
make build
python scripts/check_artifacts.py --profiles minimum latest --run-tests --report reports/installed.json
```

Use an isolated PostgreSQL database and a server binary built from the exact
`proto/SOURCE.json` revision. The harness reads Go build metadata to check that
revision, starts temporary TLS/mTLS servers for both backends, and stops them
when the test command exits. OpenSSL and Go must be on PATH. The configured
database receives disposable queues, messages, schemas and schedules.

```bash
export NZOVU_TEST_POSTGRES_DSN='postgres://nzovu:sdk-fixture@localhost:5432/nzovu?sslmode=disable'
make check-live NZOVU_SERVER_BINARY=/absolute/path/to/nzovu
NZOVU_REQUIRE_BACKENDS=sqlite,postgres python scripts/run_live_ownership.py \
  --server-binary /absolute/path/to/nzovu -- \
  python scripts/check_artifacts.py --profiles minimum latest --run-tests \
    --report reports/installed-live.json
```

The fixture exports `NZOVU_LIVE_CONFIG` to its child command. Use the same harness
with the example environment's Python and `-m pytest tests/test_live_store.py`
from `examples/store-api` to exercise the real HTTP/worker flows. Installed SDK
checks use fresh environments outside the checkout; the pinned compiler/source
comparison runs separately in the Python 3.12 quality job. Reports record actual
dependencies, artifact hashes and server provenance.

For dependency auditing, install `pip-audit==2.10.1` in a separate tool environment,
then run `make audit PIP_AUDIT=/absolute/path/to/pip-audit`. Audit both SDK and
example lockfile environments. CI checks are fatal and preserve coverage XML.
Keep pinned-server failures distinct from candidate-fix results in validation
reports. Do not mark failures expected or hide them to pass the release gate.

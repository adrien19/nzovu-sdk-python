# SDK migration status

SDK-PR0 through SDK-PR3 are complete. SDK-PR4 implementation and whole-SDK
audit are complete; the server fixes are committed and pinned. Publication
still requires review and hosted live validation. SDK-PR5 remains pending.

## Repository and review state

The independent repository preserves all 79 source commits, including
`843f46e424300fcd3bc1d1706cd1d30d1cd683de`, which seeded the new repository.
SDK-PR0 and SDK-PR1 merged into `main` at `a7f402a`; SDK-PR2 and SDK-PR3
subsequently merged, with `main` at `2eb933c`.
GitHub's default branch is verified as `main`. No release tags or publication
have occurred.

SDK-PR0 is commit `9abde03` on `migration/sdk-pr0-bootstrap`.
SDK-PR1 is `dce1417` on `migration/sdk-pr1-identity-protocol`. SDK-PR2 branches
from merged `main`; SDK-PR3 stacks on SDK-PR2. See [bootstrap results](MIGRATION_BASELINE.md).

## SDK-PR1 changes

- Distribution/import name: `nzovu`; clients: `NzovuClient` and `AsyncNzovuClient`.
- Development version: `0.0.1.dev0`, read from installed package metadata.
- Ten protocol sources pinned to Nzovu server commit
  `f21477ecab47197f0c8c92dbdde2aab11ef02600`, with SHA-256 checksums in
  `proto/SOURCE.json`.
- Reproducible Python/stub generation preserves canonical descriptor paths
  and uses `nzovu.api` Python imports. Source updates require an explicit local
  checkout and full commit SHA.
- Active code/configuration uses the new identity. Historical product names
  remain only in the explicitly retained legacy changelog; Git history is preserved.
- Inherited lint/formatting issues corrected; publishing remains disabled.
- Pagination uses `page_size`/`page_token` across both clients. Queue, schedule,
  schema, history, peek and DLQ models preserve `next_page_token`; dictionary
  output retains the established protobuf JSON key `nextPageToken`.
- Removed schedule-level `exclusivity_key`; fixed public-enum conversion for
  acknowledgment and schedule requests. These compatibility fixes moved into
  PR1 so adopting the current descriptors does not leave failing checks.
- Installed wheel/sdist smoke checks are repeatable via `make check-artifacts`
  and run in CI after the build.

## SDK-PR1 validation (historical)

Executed in the pinned Linux ARM64/Python 3.12.14 development container:

| Check | Result |
| --- | --- |
| Locked dependency install | Passed |
| Regeneration drift check | Passed |
| Descriptor/source equivalence, all 31 RPC paths, protobuf pickle identity | 3 tests passed |
| Source manifest validation | Valid accepted; modified, missing and additional proto files rejected |
| Proto update without explicit source/SHA | Rejected as required |
| Flake8, Black, isort | Passed |
| Wheel and sdist build | Passed |
| Fresh wheel install, base and Pydantic extra | Imports, metadata/version, generated modules and dependency checks passed |
| Fresh sdist install, base | Same checks passed |
| Full unit suite | 196 passed, 1 skipped |
| Mypy | Passed, 9 handwritten source files under inherited configuration |

Fresh installs ran outside the source checkout without a source-tree
`PYTHONPATH`. They verified the absence of the legacy import package. The
skipped unit test requires Pydantic to be absent; base artifact smoke checks
ran without it. The inherited Pydantic `schema` field-shadowing warning remains.
Python 3.10, 3.11, 3.13, 3.14, hosted CI and live server compatibility have not
been validated here.

The 17 failures and 12 type errors exposed during initial regeneration are
resolved. Regression tests cover sync/async pagination filters, empty/final
and continued pages, token preservation in all response forms, RPC failures,
public acknowledgment enums/claim fields, schedule state/model conversion,
removed schedule-option rejection and source-manifest drift. No failure
suppressions or expected-failure markers were added.

## SDK-PR2 completion

Branch: `migration/sdk-pr2-api-contracts`, commit `8d63e8c`, based on merged
`main` at `a7f402a`.
All 31 wrappers have matching sync/async parameters, encoded-request assertions,
error/handler checks and response models. Populated-response tests verify every
nested protocol field; see [API contracts](API_CONTRACTS.md) for the public API.

Implemented binary headers and payload/schema metadata, nanosecond durations and
scheduled times, optional lease renewal limits, complete bulk/cancellation results,
claim fields, schedule executions/calendar results and lazy page iteration.
Corrected the async manual heartbeat RPC spelling. DLQ requeue now requires its
target; schema/page/priority/header/ID validation matches the inspected server.
Unset message lease durations inherit server defaults. Deprecated schedule-level
timezone input is removed in favor of calendar configuration.

Validation on Linux ARM64/Python 3.12.14:

| Check | Result |
| --- | --- |
| Full unit suite with Pydantic | 423 passed, 1 skipped (base-only case) |
| Installed wheel without Pydantic | 345 passed, 79 typed-only tests skipped |
| Typecheck | Passed, 11 handwritten source files under inherited configuration |
| Flake8 / Black / isort / generation drift | Passed |
| Wheel/sdist build and isolated base/extra installs | Passed |
| Workflow syntax and diff whitespace | Passed |

No error suppressions were added. Two inherited model-test modules incorrectly
inferred Pydantic availability from importing SDK fallback classes; they now test
the actual availability flag. All RPCs still run in the base-install suite and
verify that typed conversion reports the missing extra.

At SDK-PR2 completion, live server and ownership/lifecycle gates remained pending.
SDK-PR2 introduced ownership fields; SDK-PR3 below implements heartbeat isolation
and failed-ACK cleanup. The inherited schema-field shadowing warning remains.

Local completion log: `/private/tmp/nzovu-sdk2-final-validation.log`.


## SDK-PR3 completion

Branch: `migration/sdk-pr3-auth-ownership`, stacked on SDK-PR2. See
[authentication and ownership](AUTH_OWNERSHIP.md) for the new public contracts
and repeatable live gate.

- Shared sync/async authenticated transport; port 9000, verified TLS, optional
  mTLS, explicit plaintext, API-key metadata and RPC deadlines.
- Immutable claims retained without heartbeat; exact ownership required for
  ACK/heartbeat/renewal. gRPC codes, details, causes and trailing metadata survive.
- Bounded admission before claiming; workers isolated by complete claim;
  failed ACK retains heartbeat; stale cleanup cannot remove replacement attempts.
- Interruptible retry backoff, bounded RPC deadlines, cancellation and joined
  shutdown. Active-only observability prevents unbounded completed-worker history.
- Live validation identified that renewal `FAILED_PRECONDITION` can mean the
  extension/renewal limit, without ownership loss. That error retains heartbeat;
  both unit and real-server regressions verify successful later ACK.

Validation on Linux ARM64/Python 3.12.14, with a locally built Darwin ARM64
SQLite server at pinned commit `f21477ecab47197f0c8c92dbdde2aab11ef02600`:

| Check | Result |
| --- | --- |
| Unit/loopback suite with Pydantic | 550 passed; 1 base-only and 24 opt-in live cases skipped |
| Installed wheel without Pydantic | 472 passed; 79 typed-only and 24 live cases skipped |
| Separate real Nzovu TLS/mTLS gate | 24 passed, both clients |
| All 31 methods preserve status/details and custom handlers | Passed |
| Flake8 / Black / isort / Mypy / protocol drift | Passed |
| Wheel/sdist and isolated base/extra artifact checks | Passed |
| Workflow syntax and diff whitespace | Passed |

Live cases cover accepted/rejected API keys and TLS credentials, authenticated
background heartbeat, successful/rejected ACK, duplicate IDs across queues,
lease expiry/reclaim, stale-owner rejection and renewal limits. Unit cases also
cover retry recovery, saturation, cancellation and no orphan heartbeat work.

Logs: `/private/tmp/nzovu-sdk3-final-validation.log` and
`/private/tmp/nzovu-sdk3-live5.log`. Hosted CI, PostgreSQL, other advertised
Python versions and the full live RPC matrix remain SDK-PR4 validation work.
No package publication, release tags or default-branch push is part of PR2/PR3.

## SDK-PR4 implementation

Branch: `migration/sdk-pr4-live-examples`, based on SDK-PR3 `4096257`.
Both backends, both clients and all 31 RPCs have live coverage; examples and packaging/CI gates are migrated.
The server pin is now `b4e534a18ff3e44d28e37058410184dcedea4d20`. It contains fixes for
schema payload preservation, cron descriptor execution, SQLite writer reservation
and bounded PostgreSQL deletion-deadlock retries. Protocol definitions and
checksums are unchanged. All 40 local installed/live combinations passed against
the candidate containing these changes; server package/integration tests,
repeated concurrency regressions and lint also passed. Revalidation against the
clean committed server passed 617 installed-wheel tests, including all 68 live
cases; generation checks and base/Pydantic wheel/sdist smoke checks passed.

Hosted quality, macOS and Windows checks passed. Linux live CI awaits the
`NZOVU_SOURCE_TOKEN` secret for the private server repository; the user deferred
configuration. SDK and server PRs remain drafts pending review and hosted live
validation. Earlier result tables record validation performed at each PR.

SDK-PR5 versioning, trusted-publisher configuration, TestPyPI/PyPI publication and
public-install verification remain pending. No version bump, tag, publication or
server PR6 release is authorized by completing this audit.

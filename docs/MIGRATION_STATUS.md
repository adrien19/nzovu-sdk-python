# SDK migration status

SDK-PR0 bootstrap, SDK-PR1 identity/protocol migration and SDK-PR2 API/model
contracts are complete.
Local unit, type, style, generation and installed-artifact gates pass.
The SDK still requires SDK-PR3 authentication/worker lifecycle work and live release
validation before publication.

## Repository and review state

The independent repository preserves all 79 source commits, including
`843f46e424300fcd3bc1d1706cd1d30d1cd683de`. Local `main` identifies that
baseline, now pushed to `adrien19/nzovu-sdk-python` as its initial `main`.
GitHub's default branch is verified as `main`. No release tags or publication
have occurred.

SDK-PR0 is commit `9abde03` on `migration/sdk-pr0-bootstrap`.
SDK-PR1 follows it on `migration/sdk-pr1-identity-protocol`; review PR1 against
the PR0 branch until PR0 is merged. See [bootstrap results](MIGRATION_BASELINE.md).

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

## Validation

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

Branch: `migration/sdk-pr2-api-contracts`, based on SDK-PR1 `dce1417`.
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

The inherited schema-field shadowing warning remains. Hosted CI, the other
advertised Python versions and live server scenarios have not been run here.
SDK-PR3 ownership/lifecycle and authentication, then SDK-PR4 live compatibility,
remain release gates. SDK-PR2 introduced the request/response ownership fields;
it does not yet fix automatic heartbeat claim isolation or failed-ACK cleanup.

Local completion log: `/private/tmp/nzovu-sdk2-final-validation.log`.

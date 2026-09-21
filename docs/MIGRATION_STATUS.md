# SDK migration status

SDK-PR0 bootstrap and SDK-PR1 identity/protocol migration are complete.
Local unit, type, style, generation and installed-artifact gates pass.
The SDK still requires the broader SDK-PR2/PR3 contract work and live release
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

SDK-PR2 must still audit pagination against the live server, headers, optional fields,
execution history, response conversion and priority contracts. Authentication
and claim/heartbeat ownership follow in SDK-PR3. Full compatibility and release
validation remain required before the first server release.

Local completion log: `/private/tmp/nzovu-sdk-pr1-complete-validation.log`.

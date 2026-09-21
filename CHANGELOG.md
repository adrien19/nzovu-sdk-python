# Changelog

## 0.0.1.dev0 — unreleased

- Establish the `nzovu` Python package and renamed synchronous/asynchronous clients.
- Pin Nzovu protocol sources and regenerate Python modules reproducibly.
- Replace pagination `limit` with `page_size`, expose page tokens, and retain
  continuation tokens in typed responses.
- Remove schedule-level exclusivity and correct acknowledgment/schedule enum conversion.
- Validate wheel/sdist installs in isolated environments before distribution.
- Align all 31 sync/async RPC contracts and add complete response conversion,
  including bulk results, cancellation, schedule executions and claim metadata.
- Preserve binary headers, optional lease limits, precise times and large counters.
- Add request validation, explicit DLQ targets and bounded lazy pagination.
- Test the installed base package without Pydantic; retain authentication and
  worker-lifecycle migration gates before the first `0.0.1` release.

Earlier source-package history is preserved in [the historical changelog](docs/LEGACY_CHANGELOG.md).

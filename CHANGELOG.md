# Changelog

## 0.0.1.dev0 — unreleased

- Establish the `nzovu` Python package and renamed synchronous/asynchronous clients.
- Pin Nzovu protocol sources and regenerate Python modules reproducibly.
- Replace pagination `limit` with `page_size`, expose page tokens, and retain
  continuation tokens in typed responses.
- Remove schedule-level exclusivity and correct acknowledgment/schedule enum conversion.
- Validate wheel/sdist installs in isolated environments before distribution.
- Continue API-wrapper migration before the first `0.0.1` release.

Earlier source-package history is preserved in [the historical changelog](docs/LEGACY_CHANGELOG.md).

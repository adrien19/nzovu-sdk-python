# Nzovu Python SDK development

Review these instructions and surrounding code before implementation. Follow established Python conventions; do not introduce dependencies or change public APIs outside the requested scope. Preserve user changes and handle errors explicitly. Add comments only when they explain why.

## Repository and migration

- This is an independent SDK repository with default branch `main`. Work on feature branches and use pull requests.
- SDK-PR0 preserves the source SDK's `chronoqueue/` package and proto snapshot as a baseline. SDK-PR1 migrates package identity and generation; later PRs align client contracts.
- Never commit, tag, push or publish without the user's authorization. Publishing remains disabled until SDK-PR5.
- Keep the MIT license and source attribution. Do not copy untracked files from the source checkout without reviewing their inclusion.

## Structure

- `chronoqueue/`: synchronous/asynchronous clients, request helpers and optional Pydantic models (renamed in SDK-PR1).
- `chronoqueue/api/`: generated protobuf modules; do not edit manually.
- `proto/`: vendored protocol definitions.
- `tests/`: pytest tests; `examples/store-api/`: separate example project.
- `pyproject.toml` and `poetry.lock`: SDK dependencies and reproducible development environment.
- `.github/workflows/ci.yml`: validation only; inactive publishing workflows live outside the workflows directory.

## Development commands

Use Python 3.12 for the bootstrap baseline and Poetry 2.3.1. Run from the repository root:

```bash
make install-dev
make test
make lint
make typecheck
make format FORMAT_FLAGS=--check
make build
```

`make install-dev` validates and installs the committed lockfile into `.venv`. Keep dependency updates explicit; do not replace locked installs with unpinned pip installs. `make format` without flags edits handwritten Python formatting. Use `make gen-proto` only when deliberately changing generation; review every generated diff.

## Validation

- Use pytest and the existing sync/async test patterns. Do not apply Go test flags or Go tooling here.
- Test success and failure paths for changed behavior; preserve gRPC status details and optional-field presence.
- Run relevant tests after changes, plus lint, typecheck, formatting checks and a package build.
- Report inherited failures separately from regressions. Do not suppress failures or broaden exclusions to make CI green.
- Test both optional-Pydantic and base installs when changing exports or models. Test installed artifacts outside the source checkout when changing packaging.

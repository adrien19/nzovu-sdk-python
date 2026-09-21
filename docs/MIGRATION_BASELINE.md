# SDK-PR0 baseline

The independent repository starts at `843f46e424300fcd3bc1d1706cd1d30d1cd683de`
from the source SDK's `fix/adjust_messages_priority_scale` branch, including its
priority-range fix. All 79 reachable commits and the MIT license are preserved.
The source and destination trees matched at clone time. No tags, object
alternates, shared Git objects, credentials or untracked files were copied.
The source's untracked `demo_transaction_mode.py` remains there for later review.

Local `main` identifies that baseline. Work proceeds on migration branches;
`origin` names the proposed `adrien19/nzovu-sdk-python` destination and `legacy`
fetches the local source with pushing disabled. Configuring `origin` does not
create the GitHub repository. Nothing has been committed, pushed or published
as part of bootstrap.

## Development environment

Python 3.12.14, pinned multi-platform container digest, Poetry 2.3.1, and the
unchanged committed `poetry.lock` establish the baseline. A non-root post-create
run successfully installs the development environment and optional Pydantic
models. Desktop keyring discovery is disabled in headless containers/CI after
an ARM64 cryptography import crash was reproduced during Poetry setup.

No Redis startup or host Docker socket is needed. The old Redis Compose file
was removed. Publishing workflows are outside `.github/workflows`; both local
publish targets fail without building or publishing. CI targets `main`, uses
locked installs, and validates the committed generated code. Deterministic
protocol generation is addressed in SDK-PR1.

## Validation results

Compared an untouched archive of the source commit with the bootstrap tree
using the same locked Linux ARM64/Python 3.12 environment:

| Check | Source baseline | SDK-PR0 |
| --- | --- | --- |
| Unit tests | 147 passed, 1 skipped | 147 passed, 1 skipped |
| Typecheck | Passed, 6 handwritten source files under inherited configuration | Same |
| Wheel/sdist build | Passed | Passed |
| Flake8 | One unused import in `tests/test_async_client.py:89` | Same failure, now propagated |
| Black check | Six files require formatting | Same |
| isort check | Three files require import formatting | Same |

The skipped test is for running without Pydantic. The baseline uses Pydantic;
base-install coverage and Python 3.10–3.14 compatibility remain later gates.
Existing mypy suppressions and generated-code exclusions were not expanded.
Lint/style failures are inherited, not a green validation claim; resolve them
with the SDK-PR1 package rename and formatting pass.

Additional passed checks: Docker image build; real post-create install;
post-create from another working directory; failure propagation with a failing
installer; both disabled publish commands; devcontainer JSON/build paths;
workflow syntax; shell syntax; independent Git history and empty tag namespace.
VS Code UI launch and hosted CI have not been run.

Local diagnostic logs are under `/private/tmp/nzovu-sdk-pr0-results` and
`/private/tmp/nzovu-sdk-pr0-{image,setup}.log` in the preparation workspace.
Package names, version and protocol contracts intentionally remain unchanged
in this baseline. This is not a releasable Nzovu package.

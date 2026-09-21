#!/usr/bin/env bash
set -euo pipefail

sdk_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$sdk_root"
make install-dev
poetry run python --version

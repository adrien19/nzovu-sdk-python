import json
import os
from pathlib import Path

import pytest


@pytest.fixture(params=["sqlite", "postgres"])
def live(request):
    path = os.environ.get("NZOVU_LIVE_CONFIG")
    if not path:
        pytest.skip("requires live Nzovu fixture")
    config = json.loads(Path(path).read_text())
    backends = config.get("backends", {"sqlite": config})
    required = os.environ.get("NZOVU_REQUIRE_BACKENDS", "sqlite").split(",")
    assert set(required) <= set(backends), "required storage backend missing"
    if request.param not in backends:
        pytest.skip(f"fixture has no {request.param} backend")
    return dict(config, **backends[request.param], backend=request.param)

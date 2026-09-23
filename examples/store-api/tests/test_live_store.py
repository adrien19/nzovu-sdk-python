import json
import os
import time
from pathlib import Path
from uuid import uuid4

import pytest
from api.application import create_app
from fastapi.testclient import TestClient

from nzovu import AsyncNzovuClient, NzovuClient, TlsConfig

pytestmark = pytest.mark.skipif(not os.environ.get("NZOVU_LIVE_CONFIG"), reason="requires live Nzovu fixture")


@pytest.mark.parametrize("backend", ["sqlite", "postgres"])
@pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
def test_live_cart_end_to_end(backend, asynchronous):
    config = json.loads(Path(os.environ["NZOVU_LIVE_CONFIG"]).read_text())

    def factory(**options):
        options.update(
            host=config["host"],
            port=config["backends"][backend]["tls"],
            use_tls=True,
            api_key=config["api_key"],
            tls_config=TlsConfig(ca_path=str(Path(config["directory"]) / "ca.crt")),
        )
        return (AsyncNzovuClient if asynchronous else NzovuClient)(**options)

    app = create_app(asynchronous, client_factory=factory)
    with TestClient(app) as client:
        before = int(
            client.get("/store/queue/checkout-cart/stats").json()["stats"].get("stateCounts", {}).get("COMPLETED", 0)
        )
        cart_id = uuid4().hex
        response = client.post(
            "/store/cart", params={"cart_id": cart_id}, json={"items": [{"name": "apple", "quantity": 2, "price": 3.5}]}
        )
        assert response.status_code == 200 and response.json()["status"] == "queued"
        deadline = time.monotonic() + 10
        while True:
            stats = client.get("/store/queue/checkout-cart/stats").json()["stats"]
            if int(stats.get("stateCounts", {}).get("COMPLETED", 0)) > before:
                break
            assert time.monotonic() < deadline, stats
            time.sleep(0.1)
    assert all(task.done() for task in app.state.worker_tasks)

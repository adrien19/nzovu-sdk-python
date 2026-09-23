import os

from nzovu import TlsConfig, generate_worker_id

NZOVU_HOST = os.getenv("NZOVU_HOST", "localhost")
NZOVU_PORT = int(os.getenv("NZOVU_PORT", "9000"))
QUEUE_NAME_STORE_CART = os.getenv("QUEUE_NAME_STORE_CART", "store-cart")
QUEUE_NAME_CHECKOUT_CART = os.getenv("QUEUE_NAME_CHECKOUT_CART", "checkout-cart")
CHECKOUT_QUEUE_EXCLUSIVE_KEY = os.getenv("CHECKOUT_QUEUE_EXCLUSIVE_KEY", "checkout-worker-1")


def client_options():
    tls = os.getenv("NZOVU_TLS_ENABLED", "true").lower()
    if tls not in {"true", "false"}:
        raise ValueError("NZOVU_TLS_ENABLED must be true or false")
    return dict(
        host=NZOVU_HOST,
        port=NZOVU_PORT,
        use_tls=tls == "true",
        api_key=os.getenv("NZOVU_API_KEY"),
        rpc_timeout=5,
        worker_id=generate_worker_id("store"),
        tls_config=TlsConfig(
            ca_path=os.getenv("NZOVU_CA_FILE"),
            client_crt_path=os.getenv("NZOVU_CERT_FILE"),
            client_key_path=os.getenv("NZOVU_KEY_FILE"),
        ),
    )

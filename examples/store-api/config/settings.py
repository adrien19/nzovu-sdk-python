import os
from dotenv import load_dotenv

load_dotenv()
# When running in dev container on same Docker network: use container name
# When running on host: use localhost
# With docker-compose: use nzovu_container (service hostname)
NZOVU_HOST = os.getenv("NZOVU_HOST", "host.docker.internal")
NZOVU_PORT = os.getenv("NZOVU_PORT", "9000")
FASTAPI_HOST = os.getenv("FASTAPI_HOST", "localhost")
FASTAPI_PORT = int(os.getenv("FASTAPI_PORT", 8000))
QUEUE_NAME_STORE_CART = os.getenv("QUEUE_NAME_STORE_CART", "store-cart")
QUEUE_NAME_CHECKOUT_CART = os.getenv("QUEUE_NAME_CHECKOUT_CART", "checkout-cart")
CHECKOUT_QUEUE_EXCLUSIVE_KEY = os.getenv("CHECKOUT_QUEUE_EXCLUSIVE_KEY", "checkout-worker-1")

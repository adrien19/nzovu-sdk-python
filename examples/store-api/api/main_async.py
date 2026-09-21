"""
Store API - Async Version

This version demonstrates the AsyncNzovuClient with full async/await support.
Uses async workers and non-blocking I/O for better performance in async applications.

To run:
    poetry run uvicorn api.main_async:app --host 0.0.0.0 --port 8002 --reload
"""

from fastapi import FastAPI
import asyncio
import logging
import os
import socket
from nzovu.async_client import AsyncNzovuClient
from nzovu.utils import TlsConfig

from .routes import store
from config.settings import NZOVU_HOST, NZOVU_PORT
from .workers.process_store_cart_async_worker import (
    process_store_cart_async,
    process_checkout_cart_async,
)
from .workers.queue_manager_async_worker import (
    create_store_cart_queue_async,
    create_checkout_cart_queue_async,
)

app = FastAPI(
    title="Store API - Async Version",
    description="Demonstrates AsyncNzovuClient with full async/await support",
    version="2.0.0-async",
)

# Initialize async client
async_client: AsyncNzovuClient = None

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def heartbeat_error_handler(error_info: dict):
    """
    Custom error handler for heartbeat failures.
    This demonstrates how to monitor and react to heartbeat issues.
    """
    logger.error(
        f"⚠️ ASYNC HEARTBEAT ERROR - Message: {error_info['message_id']}, "
        f"Queue: {error_info['queue_name']}, "
        f"Error: {error_info.get('error_details', 'Unknown')}, "
        f"Retry Count: {error_info.get('retry_count', 0)}, "
        f"Heartbeats Sent: {error_info.get('heartbeats_sent', 0)}"
    )


async def get_async_client() -> AsyncNzovuClient:
    """
    Get or create AsyncNzovu client with TLS if certificates exist.

    The async client provides:
    - Non-blocking I/O operations
    - Asyncio-based heartbeats (tasks instead of threads)
    - Better integration with async applications
    - Same heartbeat features as sync client
    """
    global async_client

    if async_client is None:
        # Check if TLS certs exist
        ca_path = "./certs/ca.crt"
        client_crt_path = "./certs/client.crt"
        client_key_path = "./certs/client.key"

        use_tls = all(os.path.exists(p) for p in [ca_path, client_crt_path, client_key_path])

        # Use hostname as worker ID - stable across application restarts
        worker_id = f"worker-{socket.gethostname()}"

        if use_tls:
            async_client = AsyncNzovuClient(
                host=NZOVU_HOST,
                port=NZOVU_PORT,
                use_tls=True,
                tls_config=TlsConfig(
                    ca_path=ca_path,
                    client_crt_path=client_crt_path,
                    client_key_path=client_key_path,
                ),
                worker_id=worker_id,
                # Heartbeat configuration
                heartbeat_max_duration=120,
                heartbeat_max_count=500,
                heartbeat_error_callback=heartbeat_error_handler,
            )
            logger.info(f"🚀 AsyncNzovu client initialized with TLS (worker_id: {worker_id})")
        else:
            # Use insecure connection for testing/development
            async_client = AsyncNzovuClient(
                host=NZOVU_HOST,
                port=NZOVU_PORT,
                use_tls=False,
                worker_id=worker_id,
                # Heartbeat configuration
                heartbeat_max_duration=120,
                heartbeat_max_count=500,
                heartbeat_error_callback=heartbeat_error_handler,
            )
            logger.info(f"🚀 AsyncNzovu client initialized (no TLS, worker_id: {worker_id})")

        # Connect the async client
        await async_client.connect()
        logger.info("✅ Async client connected successfully")
        logger.info("   - Heartbeat max duration: 120s")
        logger.info("   - Heartbeat max count: 500")
        logger.info("   - Using asyncio tasks (not threads) for heartbeats")

    return async_client


@app.on_event("startup")
async def startup_event():
    """Initialize async client and start background workers."""
    global async_client

    logger.info("=" * 80)
    logger.info("🌟 Starting Store API - ASYNC VERSION")
    logger.info("=" * 80)

    async_client = await get_async_client()

    # Start async workers
    asyncio.create_task(create_store_cart_queue_async(async_client))
    asyncio.create_task(create_checkout_cart_queue_async(async_client))
    asyncio.create_task(process_store_cart_async(async_client))
    asyncio.create_task(process_checkout_cart_async(async_client))

    # Add heartbeat monitoring task
    asyncio.create_task(monitor_heartbeats_async(async_client))

    logger.info("✅ All async workers started")
    logger.info("=" * 80)


async def monitor_heartbeats_async(client: AsyncNzovuClient):
    """
    Background task to monitor active async heartbeats.

    This demonstrates the observability features of the async client.
    Note: Uses the same API as sync client, but in an async context.
    """
    await asyncio.sleep(5)  # Wait for workers to start

    while True:
        try:
            active_count = client.get_active_heartbeat_count()

            if active_count > 0:
                logger.info(f"💓 [ASYNC] Active heartbeats: {active_count}")

                # Get heartbeat statistics
                stats = client.get_heartbeat_stats()
                for msg_id, metric in stats.items():
                    if msg_id in [m for m in stats.keys()]:  # Active ones
                        logger.info(
                            f"   📊 [ASYNC] Message {msg_id[:8]}...: "
                            f"{metric['heartbeats_sent']} sent, "
                            f"{metric['heartbeats_failed']} failed"
                        )

            await asyncio.sleep(10)  # Check every 10 seconds
        except Exception as e:
            logger.error(f"❌ Error in async heartbeat monitor: {e}")
            await asyncio.sleep(10)


@app.on_event("shutdown")
async def shutdown_event():
    """Gracefully close the async client."""
    if async_client:
        logger.info("=" * 80)
        logger.info("🛑 Shutting down AsyncNzovu client...")

        # Show final heartbeat stats
        active_count = async_client.get_active_heartbeat_count()
        if active_count > 0:
            logger.warning(f"⚠️ Closing with {active_count} active async heartbeat(s)")

        await async_client.close(timeout=30.0)
        logger.info("✅ AsyncNzovu client closed successfully")
        logger.info("=" * 80)


# Include API routes
app.include_router(store.router, prefix="/store", tags=["store"])


@app.get("/")
async def root():
    """Root endpoint showing async client status."""
    return {
        "message": "Store API - Async Version",
        "client_type": "AsyncNzovuClient",
        "features": [
            "Non-blocking I/O operations",
            "Asyncio-based heartbeats",
            "Full async/await support",
            "Same heartbeat features as sync client",
        ],
        "active_heartbeats": async_client.get_active_heartbeat_count() if async_client else 0,
    }


@app.get("/health")
async def health():
    """Health check endpoint."""
    if async_client is None:
        return {"status": "initializing", "client": None}

    active_count = async_client.get_active_heartbeat_count()
    return {
        "status": "healthy",
        "client": "AsyncNzovuClient",
        "active_heartbeats": active_count,
    }

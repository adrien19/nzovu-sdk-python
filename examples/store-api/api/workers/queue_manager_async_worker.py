"""
Async Queue Manager Workers

These workers ensure the required queues exist when the async application starts.
Uses AsyncNzovuClient for non-blocking queue creation operations.
"""

import asyncio
import logging
from nzovu.utils import QueueOptions, QueueType, LeasePolicyOptions, MessageRetentionPolicy, RetentionMode
from nzovu.async_client import AsyncNzovuClient
from config.settings import QUEUE_NAME_STORE_CART, QUEUE_NAME_CHECKOUT_CART, CHECKOUT_QUEUE_EXCLUSIVE_KEY

logger = logging.getLogger(__name__)


async def create_store_cart_queue_async(async_client: AsyncNzovuClient):
    """
    Async worker to ensure 'store-cart' queue exists.

    Uses non-blocking async operations for queue creation.
    Retries on failure with exponential backoff.
    """
    max_retries = 3
    retry_delay = 2

    for attempt in range(max_retries):
        try:
            logger.info(f"🔧 [ASYNC] Creating 'store-cart' queue (attempt {attempt + 1}/{max_retries})...")

            # Non-blocking queue creation
            await async_client.create_queue(
                name=QUEUE_NAME_STORE_CART,
                options=QueueOptions(
                    type=QueueType.SIMPLE,
                    max_attempts=max_retries,
                    lease_policy=LeasePolicyOptions(
                        base_lease="5s",
                        max_extension="20m",
                        heartbeat_timeout="5s",
                        extend_step="30s",
                    ),
                    retention_policy=MessageRetentionPolicy(
                        mode=RetentionMode.RETAIN_DURATION,
                        # retention_seconds=86400,  # 24 hours
                        retention_seconds=60,  # 1 hour
                    ),
                ),
            )

            logger.info("✅ [ASYNC] 'store-cart' queue created successfully")
            return

        except Exception as e:
            if "already exists" in str(e).lower():
                logger.info("ℹ️ [ASYNC] 'store-cart' queue already exists")
                return

            logger.error(f"❌ [ASYNC] Failed to create 'store-cart' queue: {e}")

            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                logger.error("❌ [ASYNC] Max retries reached for 'store-cart' queue creation")


async def create_checkout_cart_queue_async(async_client: AsyncNzovuClient):
    """
    Async worker to ensure 'checkout-cart' queue exists.

    Uses non-blocking async operations for queue creation.
    Retries on failure with exponential backoff.
    """
    max_retries = 3
    retry_delay = 2

    for attempt in range(max_retries):
        try:
            logger.info(f"🔧 [ASYNC] Creating 'checkout-cart' queue (attempt {attempt + 1}/{max_retries})...")

            # Non-blocking queue creation
            await async_client.create_queue(
                name=QUEUE_NAME_CHECKOUT_CART,
                options=QueueOptions(
                    type=QueueType.EXCLUSIVE,
                    max_attempts=max_retries,
                    exclusivity_key=CHECKOUT_QUEUE_EXCLUSIVE_KEY,
                    lease_policy=LeasePolicyOptions(
                        base_lease="5s",
                        max_extension="10m",
                        heartbeat_timeout="15s",
                        extend_step="1m",
                    ),
                    retention_policy=MessageRetentionPolicy(
                        mode=RetentionMode.RETAIN_DURATION,
                        # retention_seconds=86400,  # 24 hours
                        retention_seconds=60,  # 1 hour
                    ),
                ),
            )

            logger.info("✅ [ASYNC] 'checkout-cart' queue created successfully")
            return

        except Exception as e:
            if "already exists" in str(e).lower():
                logger.info("ℹ️ [ASYNC] 'checkout-cart' queue already exists")
                return

            logger.error(f"❌ [ASYNC] Failed to create 'checkout-cart' queue: {e}")

            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                logger.error("❌ [ASYNC] Max retries reached for 'checkout-cart' queue creation")

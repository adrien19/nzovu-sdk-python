"""
Async Worker for processing store cart messages.

This worker demonstrates the AsyncNzovuClient with:
- Non-blocking message processing using async/await
- Asyncio-based heartbeat management
- Full async context manager support
- Same heartbeat lifecycle demonstration as sync version
"""

import asyncio
import logging
from nzovu.async_client import AsyncNzovuClient
from config.settings import QUEUE_NAME_STORE_CART, QUEUE_NAME_CHECKOUT_CART, CHECKOUT_QUEUE_EXCLUSIVE_KEY
from nzovu.utils import (
    AcknowledgeMessageParams,
    MessageState,
    PostMessageOptions,
    PostMessageParams,
)

logger = logging.getLogger(__name__)


async def process_store_cart_async(async_client: AsyncNzovuClient):
    """
    Async worker that processes 'store-cart' messages with heartbeat support.

    This demonstrates:
    - Automatic heartbeat initiation when processing takes time
    - Clean heartbeat lifecycle with asyncio tasks
    - Non-blocking I/O during message processing
    - Active heartbeat count monitoring
    """
    logger.info("🚀 [ASYNC] Starting store-cart async worker")

    while True:
        try:
            # Non-blocking message fetch
            response = await async_client.get_next_message(
                queue_name=QUEUE_NAME_STORE_CART, lease_duration="5s", enable_heartbeat=True
            )
            response_proto = response.to_proto()
            message = response.to_model().message

            if message is None:
                logger.info("⏸️ [ASYNC] No messages in store-cart queue, waiting...")
                await asyncio.sleep(2)
                continue

            # Retrieve active heartbeat count before processing
            count_before = async_client.get_active_heartbeat_count()
            logger.info(
                f"📥 [ASYNC] Processing message {message.message_id[:8]}... (Active heartbeats before: {count_before})"
            )

            payload_data = message.metadata.payload.data
            logger.info(f"   📦 [ASYNC] Data: {payload_data}")
            logger.info(f"   📦 [ASYNC] Cart Items: {payload_data.get('items', [])}")

            # Calculate the total price
            total = sum(item["price"] * item["quantity"] for item in payload_data["items"])
            logger.info(f"💰 Calculated total: ${total:.2f} for {len(payload_data['items'])} items")

            # Create a new message for checkout-cart with the total price
            checkout_data = {**payload_data, "total": total}

            # Simulate long processing (60 seconds)
            # During this time, AsyncNzovuClient automatically sends heartbeats via asyncio task
            logger.info(f"⏳ [ASYNC] Starting 60s processing for message {message.message_id[:8]}...")

            for i in range(12):  # 12 iterations of 5 seconds = 60 seconds
                await asyncio.sleep(2)

                # Check heartbeat status during processing (non-blocking)
                active_heartbeats = async_client.get_active_heartbeats()
                if message.message_id in active_heartbeats:
                    stats = async_client.get_heartbeat_stats().get(message.message_id, {})
                    logger.info(
                        f"💓 [ASYNC] [{i*5 + 5}s] Heartbeat active for {message.message_id[:8]}... "
                        f"(sent: {stats.get('heartbeats_sent', 0)}, "
                        f"failed: {stats.get('heartbeats_failed', 0)})"
                    )
                else:
                    logger.warning(f"⚠️ [ASYNC] [{i*5 + 5}s] No heartbeat found for {message.message_id[:8]}...")

            logger.info(f"✅ [ASYNC] Finished processing message {message.message_id[:8]}...")

            # Get final heartbeat stats before acknowledging
            final_stats = async_client.get_heartbeat_stats().get(message.message_id, {})
            logger.info(
                f"📊 [ASYNC] Final heartbeat stats for {message.message_id[:8]}...: "
                f"sent={final_stats.get('heartbeats_sent', 0)}, "
                f"failed={final_stats.get('heartbeats_failed', 0)}"
            )

            post_msg_options = PostMessageOptions(
                state=MessageState.INVISIBLE.value, max_attempts=2
            )
            post_msg_params = PostMessageParams(
                queue_name=QUEUE_NAME_CHECKOUT_CART,
                message_id=message.message_id + "-checkout",
                data=checkout_data,
                options=post_msg_options,
            )

            logger.info(f"📤 Posting message to checkout queue {QUEUE_NAME_CHECKOUT_CART}")
            post_resp_checkout = await async_client.post_message(msg_params=post_msg_params)
            post_resp = post_resp_checkout.to_dict()

            if post_resp.get("success"):
                logger.info(f"✅ [ASYNC] Successfully posted message to checkout queue")

                # Acknowledge message (this automatically stops the heartbeat asyncio task)
                ack_params = AcknowledgeMessageParams(
                    queue_name=QUEUE_NAME_STORE_CART,
                    message_id=message.message_id,
                    state=MessageState.COMPLETED.value,
                    worker_id=response.worker_id,
                    attempt_id=response.attempt_id,
                )
                logger.info(f"🛎️ Acknowledging message {message.message_id[:8]}...: worker_id: {response.worker_id}, attempt_id: {response.attempt_id}")

                await async_client.acknowledge_message(params=ack_params)
            else:
                logger.error(f"❌ [ASYNC] Failed to post message to checkout: {post_resp}")
                ack_params = AcknowledgeMessageParams(
                    queue_name=QUEUE_NAME_STORE_CART,
                    message_id=message.message_id,
                    state=MessageState.ERRORED.value,
                    worker_id=response.worker_id,
                    attempt_id=response.attempt_id,
                )
                logger.info(f"🛎️ Acknowledging message {message.message_id[:8]}...: worker_id: {response.worker_id}, attempt_id: {response.attempt_id}")
                await async_client.acknowledge_message(params=ack_params)

            # Verify heartbeat was stopped
            count_after = async_client.get_active_heartbeat_count()
            logger.info(
                f"🎯 [ASYNC] Message {message.message_id[:8]}... acknowledged "
                f"(Active heartbeats after: {count_after})"
            )

            # Verify heartbeat is no longer active
            await asyncio.sleep(1)
            active_heartbeats_after = async_client.get_active_heartbeats()
            if message.message_id not in active_heartbeats_after:
                logger.info(f"✅ [ASYNC] Heartbeat successfully stopped for {message.message_id[:8]}...: worker_id: {response.worker_id}, attempt_id: {response.attempt_id}")
            else:
                logger.error(f"❌ [ASYNC] Heartbeat still active for {message.message_id[:8]}... (BUG!)")

        except Exception as e:
            logger.error(f"❌ [ASYNC] Error processing store-cart message: {e}", exc_info=True)
            await asyncio.sleep(5)


async def process_checkout_cart_async(async_client: AsyncNzovuClient):
    """
    Async worker for 'checkout-cart' queue - processes quickly WITHOUT heartbeat.

    This demonstrates:
    - Fast async message processing (< 10 seconds)
    - No heartbeat initiated for quick operations
    - Clean async/await patterns
    """
    logger.info("🚀 [ASYNC] Starting checkout-cart async worker")

    while True:
        try:
            # Non-blocking message fetch
            response = await async_client.get_next_message(
                queue_name=QUEUE_NAME_CHECKOUT_CART,
                lease_duration="120s",
                exclusivity_key=CHECKOUT_QUEUE_EXCLUSIVE_KEY,
            )

            response_proto = response.to_proto()
            dict_resp = response.to_dict()
            logger.info(f"🔍 [ASYNC] Polling for next message from checkout-cart queue...resp: {dict_resp}")
            message = dict_resp.get("message", None)
            logger.info(f"🔍 [ASYNC] Polling for next message from checkout-cart queue...{message}")

            if message is None or message.get("messageId") is None:
                logger.info("⏸️ [ASYNC] No messages in checkout-cart queue, waiting...")
                await asyncio.sleep(2)
                continue

            count_before = async_client.get_active_heartbeat_count()
            logger.info(
                f"💳 [ASYNC] Processing checkout {message.get('messageId')[:8]}... (Active heartbeats: {count_before})"
            )

            # Fast processing (5 seconds) - no heartbeat should be initiated
            await asyncio.sleep(5)

            # Verify no heartbeat was started
            active_heartbeats = async_client.get_active_heartbeats()
            if message.get("messageId") not in active_heartbeats:
                logger.info(f"✅ [ASYNC] Quick processing - no heartbeat needed for {message.get('messageId')[:8]}...")
            else:
                logger.warning(f"⚠️ [ASYNC] Unexpected heartbeat for quick message {message.get('messageId')[:8]}...")

            # Acknowledge message
            ack_params = AcknowledgeMessageParams(
                queue_name=QUEUE_NAME_CHECKOUT_CART,
                message_id=message.get("messageId"),
                state=MessageState.COMPLETED.value,
                worker_id=response.worker_id,
                attempt_id=response.attempt_id,
            )
            await async_client.acknowledge_message(params=ack_params)
            logger.info(f"✅ [ASYNC] Checkout {message.get('messageId')[:8]}... completed")

        except Exception as e:
            logger.error(f"❌ [ASYNC] Error processing checkout-cart message: {e}", exc_info=True)
            await asyncio.sleep(5)

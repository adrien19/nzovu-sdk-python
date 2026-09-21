import asyncio
import logging
import time
from uuid import uuid4

from nzovu.client import NzovuClient
from nzovu.utils import (
    AcknowledgeMessageParams,
    MessageState,
    PostMessageOptions,
    PostMessageParams,
)
from config.settings import (
    CHECKOUT_QUEUE_EXCLUSIVE_KEY,
    QUEUE_NAME_CHECKOUT_CART,
    QUEUE_NAME_STORE_CART,
)
from google.protobuf import json_format

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def process_store_cart(client: NzovuClient):
    """
    Process messages from store-cart queue.

    This demonstrates the new heartbeat features:
    1. Automatic heartbeat for long-running tasks
    2. Proper cleanup when message is acknowledged
    3. Empty message handling
    """
    logger.info("🚀 Store cart worker started")

    while True:
        try:
            # Get the next message with heartbeat enabled
            # The new implementation will:
            # - Only start heartbeat if a valid message is returned
            # - Automatically stop heartbeat when we acknowledge the message
            logger.debug("🔍 Polling for next message from store-cart queue...")

            response = client.get_next_message(
                queue_name=QUEUE_NAME_STORE_CART,
                lease_duration="5s",  # Short lease for demo
                enable_heartbeat=True,  # Enable automatic heartbeat
            ).to_proto()

            # Check if we got a message
            if response and response.message and response.message.message_id:
                message_id = response.message.message_id
                logger.info(f"📬 Received message {message_id} from store-cart queue")

                # Show heartbeat status right after getting message
                active_count = client.get_active_heartbeat_count()
                logger.info(f"💓 Heartbeat started (active heartbeats: {active_count})")

                # Convert protobuf Struct to dict
                data = json_format.MessageToDict(
                    response.message.metadata.payload.data, preserving_proto_field_name=True
                )

                # Calculate the total price
                total = sum(item["price"] * item["quantity"] for item in data["items"])
                logger.info(f"💰 Calculated total: ${total:.2f} for {len(data['items'])} items")

                # Create a new message for checkout-cart with the total price
                checkout_data = {**data, "total": total}

                # Simulate long-running processing (60 seconds)
                # During this time, the heartbeat will keep the message lease active
                logger.info("⏳ Simulating 60s processing time (heartbeat keeps message alive)...")
                logger.info("   💓 Heartbeat is automatically renewing the message lease every ~1s")

                time.sleep(60) # Simulate long processing (60 seconds)

                logger.info("✅ Processing completed after 60s")

                post_msg_options = PostMessageOptions(
                    state=MessageState.INVISIBLE.value, max_attempts=2
                )
                post_msg_params = PostMessageParams(
                    queue_name=QUEUE_NAME_CHECKOUT_CART,
                    message_id=response.message.message_id + "-checkout",
                    data=checkout_data,
                    options=post_msg_options,
                )

                logger.info(f"📤 Posting message to checkout queue {QUEUE_NAME_CHECKOUT_CART}")
                post_resp = client.post_message(msg_params=post_msg_params).to_dict()

                ack_msg_params = AcknowledgeMessageParams(
                    message_id=response.message.message_id,
                    queue_name=QUEUE_NAME_STORE_CART,
                    state=MessageState.COMPLETED.value,
                    worker_id=response.worker_id,
                    attempt_id=response.attempt_id,
                )

                if post_resp.get("success"):
                    logger.info(f"✅ Successfully posted message to checkout queue")

                    # Check active heartbeats before acknowledging
                    active_before = client.get_active_heartbeat_count()
                    logger.info(f"💓 Active heartbeats before ACK: {active_before}")

                    # Acknowledge the processed message
                    # The new implementation will AUTOMATICALLY STOP the heartbeat here
                    logger.info(f"📝 Acknowledging message {message_id}...")
                    ack_resp = client.acknowledge_message(params=ack_msg_params).to_dict()

                    # Check active heartbeats after acknowledging
                    active_after = client.get_active_heartbeat_count()
                    logger.info(f"💓 Active heartbeats after ACK: {active_after}")
                    logger.info(f"🛑 Heartbeat automatically stopped (was: {active_before}, now: {active_after})")

                else:
                    logger.error(f"❌ Failed to post message to checkout: {post_resp}")
                    ack_msg_params.state = MessageState.FAILED.value
                    ack_resp = client.acknowledge_message(params=ack_msg_params).to_dict()

                logger.info(f"✅ DONE processing message {message_id}")
                logger.info("=" * 80)
            else:
                # No message available - the new implementation handles this correctly
                # and won't start a heartbeat for empty responses
                logger.debug("📭 No message available in queue (heartbeat not started)")

        except Exception as e:
            logger.error(f"❌ Error in process_store_cart: {e}", exc_info=True)

        await asyncio.sleep(1)  # Sleep for a bit before fetching the next message


async def process_checkout_cart(client: NzovuClient):
    """
    Process messages from checkout-cart queue.

    This demonstrates processing WITHOUT heartbeat for comparison.
    """
    logger.info("🚀 Checkout cart worker started")

    while True:
        try:
            # Get the next message WITHOUT heartbeat (for comparison)
            logger.debug("🔍 Polling for next message from checkout-cart queue...")

            response = client.get_next_message(
                queue_name=QUEUE_NAME_CHECKOUT_CART,
                lease_duration="5s",
                exclusivity_key=CHECKOUT_QUEUE_EXCLUSIVE_KEY,
                enable_heartbeat=False,  # No heartbeat for this worker (fast processing)
            ).to_proto()

            # Check if we got a message
            if response and response.message and response.message.message_id:
                message_id = response.message.message_id
                logger.info(f"📬 Received message {message_id} from checkout-cart queue")
                logger.info(f"💓 Heartbeat: DISABLED (fast processing, no need)")

                # Convert message to dict for display
                message_dict = json_format.MessageToDict(response.message, preserving_proto_field_name=True)

                # Quick processing (no heartbeat needed)
                logger.info(f"⚡ Quick processing (no heartbeat needed)")

                # Acknowledge the processed message
                params = AcknowledgeMessageParams(
                    message_id=response.message.message_id,
                    queue_name=QUEUE_NAME_CHECKOUT_CART,
                    state=MessageState.COMPLETED.value,
                    worker_id=response.worker_id,
                    attempt_id=response.attempt_id,
                )
                acknow_resp = client.acknowledge_message(params=params).to_dict()
                logger.info(f"✅ Message {message_id} acknowledged")

                # Demo: Try to acknowledge again with invalid state transition (should fail)
                logger.info("⚠️ Testing invalid state transition (COMPLETED -> PENDING)...")
                params = AcknowledgeMessageParams(
                    message_id=response.message.message_id,
                    queue_name=QUEUE_NAME_CHECKOUT_CART,
                    state=MessageState.PENDING.value,
                )
                try:
                    acknow_resp = client.acknowledge_message(params=params).to_dict()
                    logger.info(f"Response: {acknow_resp}")
                except Exception as e:
                    logger.warning(f"❌ Expected failure: {e}")

                logger.info(f"✅ DONE processing checkout message {message_id}")
                logger.info("=" * 80)
            else:
                logger.debug("📭 No message available in checkout queue")

        except Exception as e:
            logger.error(f"❌ Error in process_checkout_cart: {e}", exc_info=True)

        await asyncio.sleep(1)  # Sleep for a bit before fetching the next message

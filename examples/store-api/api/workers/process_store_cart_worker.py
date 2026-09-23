import asyncio
import logging

import grpc
from config.settings import CHECKOUT_QUEUE_EXCLUSIVE_KEY, QUEUE_NAME_CHECKOUT_CART, QUEUE_NAME_STORE_CART
from google.protobuf.json_format import MessageToDict

from nzovu import HeartbeatCapacityError, MessageState, PostMessageParams, RpcOperationError

from ..models.request_models import CartItems
from ..sdk import invoke

logger = logging.getLogger(__name__)


async def process_once(client, checkout=False):
    queue = QUEUE_NAME_CHECKOUT_CART if checkout else QUEUE_NAME_STORE_CART
    response = await invoke(
        client.get_next_message,
        queue,
        "30s",
        enable_heartbeat=True,
        exclusivity_key=CHECKOUT_QUEUE_EXCLUSIVE_KEY if checkout else "",
    )
    claim = response.claim
    if claim is None:
        return False
    try:
        data = MessageToDict(response.to_proto().message.metadata.payload.data)
        if not checkout:
            cart = CartItems.model_validate(data)
            total = sum(item.price * item.quantity for item in cart.items)
            params = PostMessageParams(
                claim.message_id + "-checkout", dict(data, total=total), QUEUE_NAME_CHECKOUT_CART
            )
            try:
                posted = await invoke(client.post_message, params)
                if not posted.to_proto().success:
                    raise RuntimeError("Checkout message was not accepted")
            except RpcOperationError as error:
                # A retry after an uncertain ACK uses the same downstream message ID.
                if error.code() != grpc.StatusCode.ALREADY_EXISTS:
                    raise
        else:
            logger.info("Checkout %s total=%s", claim.message_id, data.get("total"))
        acknowledged = await invoke(client.acknowledge_message, claim.acknowledge(MessageState.COMPLETED))
        if not acknowledged.to_proto().success:
            raise RuntimeError("Cart acknowledgment was not accepted")
        return True
    except BaseException:
        # An abandoned claim must expire so another worker can retry it.
        await invoke(client.stop_heartbeat, claim)
        raise


async def run_worker(client, stop, checkout=False):
    while not stop.is_set():
        try:
            await process_once(client, checkout)
        except RpcOperationError as error:
            if error.code() != grpc.StatusCode.NOT_FOUND:
                logger.warning("Cart RPC failed: %s", error)
        except HeartbeatCapacityError:
            logger.warning("Heartbeat capacity exhausted")
        except Exception:
            logger.exception("Cart processing failed")
        try:
            await asyncio.wait_for(stop.wait(), timeout=0.5)
        except asyncio.TimeoutError:
            pass


async def process_store_cart(client, stop):
    await run_worker(client, stop)


async def process_checkout_cart(client, stop):
    await run_worker(client, stop, checkout=True)

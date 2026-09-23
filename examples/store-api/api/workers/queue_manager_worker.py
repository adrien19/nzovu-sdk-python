import grpc
from config.settings import CHECKOUT_QUEUE_EXCLUSIVE_KEY, QUEUE_NAME_CHECKOUT_CART, QUEUE_NAME_STORE_CART

from nzovu import LeasePolicyOptions, MessageRetentionPolicy, QueueOptions, QueueType, RetentionMode, RpcOperationError

from ..sdk import invoke


def options(checkout=False):
    return QueueOptions(
        type=QueueType.EXCLUSIVE if checkout else QueueType.SIMPLE,
        exclusivity_key=CHECKOUT_QUEUE_EXCLUSIVE_KEY if checkout else "",
        max_attempts=3,
        retention_policy=MessageRetentionPolicy(RetentionMode.RETAIN_DURATION, retention_seconds=3600),
        lease_policy=LeasePolicyOptions(
            base_lease="30s", max_extension="5m", heartbeat_timeout="10s", extend_step="5s"
        ),
    )


async def ensure_queue(client, name, checkout=False):
    try:
        await invoke(client.create_queue, name=name, options=options(checkout))
    except RpcOperationError as error:
        if error.code() != grpc.StatusCode.ALREADY_EXISTS:
            raise


async def create_store_cart_queue(client):
    await ensure_queue(client, QUEUE_NAME_STORE_CART)


async def create_checkout_cart_queue(client):
    await ensure_queue(client, QUEUE_NAME_CHECKOUT_CART, checkout=True)

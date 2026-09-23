from .queue_manager_worker import create_checkout_cart_queue, create_store_cart_queue


async def create_store_cart_queue_async(client):
    await create_store_cart_queue(client)


async def create_checkout_cart_queue_async(client):
    await create_checkout_cart_queue(client)

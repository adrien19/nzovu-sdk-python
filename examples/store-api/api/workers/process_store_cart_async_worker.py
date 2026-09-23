from .process_store_cart_worker import run_worker


async def process_store_cart_async(client, stop):
    await run_worker(client, stop)


async def process_checkout_cart_async(client, stop):
    await run_worker(client, stop, checkout=True)

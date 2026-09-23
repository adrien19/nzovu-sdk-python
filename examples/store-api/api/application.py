import asyncio
from contextlib import asynccontextmanager

from config.settings import client_options
from fastapi import FastAPI

from nzovu import AsyncNzovuClient, NzovuClient

from .routes.store import router
from .sdk import invoke
from .workers.process_store_cart_worker import process_checkout_cart, process_store_cart
from .workers.queue_manager_worker import create_checkout_cart_queue, create_store_cart_queue


def create_app(asynchronous=False, client_factory=None, workers_enabled=True):
    factory = client_factory or (AsyncNzovuClient if asynchronous else NzovuClient)

    @asynccontextmanager
    async def lifespan(app):
        client = factory(**client_options())
        stop = asyncio.Event()
        tasks = []
        try:
            if asynchronous:
                await client.connect()
            await create_store_cart_queue(client)
            await create_checkout_cart_queue(client)
            app.state.nzovu = client
            if workers_enabled:
                tasks = [
                    asyncio.create_task(worker(client, stop)) for worker in (process_store_cart, process_checkout_cart)
                ]
            app.state.worker_tasks = tasks
            yield
        finally:
            stop.set()
            try:
                await invoke(client.close)
            finally:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                app.state.nzovu = None

    app = FastAPI(title="Nzovu Store API", lifespan=lifespan)
    app.include_router(router, prefix="/store", tags=["store"])
    return app

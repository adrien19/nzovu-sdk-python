"""Run either SDK without blocking the HTTP event loop."""

import asyncio
import inspect


async def invoke(method, *args, **kwargs):
    if inspect.iscoroutinefunction(method):
        return await method(*args, **kwargs)
    task = asyncio.create_task(asyncio.to_thread(method, *args, **kwargs))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        # Wait for the bounded RPC to settle instead of abandoning a worker thread.
        try:
            await task
        finally:
            raise

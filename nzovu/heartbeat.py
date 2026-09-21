"""Bounded, claim-scoped heartbeat workers with interruptible shutdown."""

import asyncio
import logging
import math
import threading
import time
from concurrent.futures import ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from typing import Any, Dict

import grpc

from .api.message.v1.message_pb2 import Message
from .api.queueservice.v1.request_response_pb2 import SendMessageHeartBeatRequest
from .exceptions import InitializationError, NzovuError
from .ownership import Claim

RETRYABLE = {grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED, grpc.StatusCode.RESOURCE_EXHAUSTED}
OWNERSHIP_LOST = {grpc.StatusCode.NOT_FOUND, grpc.StatusCode.FAILED_PRECONDITION, grpc.StatusCode.PERMISSION_DENIED}


class HeartbeatCapacityError(NzovuError):
    """All heartbeat slots are reserved; no new message was claimed."""


@dataclass
class Worker:
    claim: Claim
    stop: Any
    future: Any = None
    started: float = field(default_factory=time.monotonic)
    sent: int = 0
    failed: int = 0
    retries: int = 0
    last_error: Any = None


class Registry:
    def __init__(self, capacity, max_duration, max_count, interval, rpc_timeout, callback):
        for name, value in (("capacity", capacity), ("max_count", max_count)):
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise InitializationError(f"heartbeat {name} must be a positive integer")
        for name, value in (("max_duration", max_duration), ("interval", interval)):
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
                raise InitializationError(f"heartbeat {name} must be finite and positive")
        self.capacity = capacity
        self.max_duration = max_duration
        self.max_count = max_count
        self.interval = interval
        self.rpc_timeout = min(rpc_timeout or 5.0, 5.0)
        self.callback = callback
        self.lock = threading.RLock()
        self.closed = False
        self.reservations = 0
        self.workers: Dict[Claim, Worker] = {}

    def reserve(self):
        with self.lock:
            if self.closed:
                raise NzovuError("Client is closed")
            if self.reservations >= self.capacity:
                raise HeartbeatCapacityError("Heartbeat capacity exhausted; retry after a claim finishes")
            self.reservations += 1

    def release(self):
        with self.lock:
            self.reservations -= 1

    def ensure_open(self):
        with self.lock:
            if self.closed:
                raise NzovuError("Client is closed")

    def stop(self, claim):
        with self.lock:
            worker = self.workers.get(claim)
            if worker is None:
                return False
            worker.stop.set()
            return True

    def begin_close(self):
        with self.lock:
            self.closed = True
            for worker in self.workers.values():
                worker.stop.set()
            return [worker.future for worker in self.workers.values()]

    def snapshot(self):
        with self.lock:
            return {
                claim: dict(
                    **claim.to_dict(),
                    heartbeats_sent=w.sent,
                    heartbeats_failed=w.failed,
                    last_error=w.last_error,
                    duration=time.monotonic() - w.started,
                )
                for claim, w in self.workers.items()
            }

    def finish(self, worker):
        with self.lock:
            if self.workers.get(worker.claim) is worker:
                del self.workers[worker.claim]
            self.reservations -= 1

    def remaining(self, worker):
        return self.max_duration - (time.monotonic() - worker.started)

    def notify(self, worker, error):
        logging.warning("Heartbeat stopped for claim %r: %s", worker.claim, error)
        if self.callback:
            try:
                self.callback(
                    dict(
                        **worker.claim.to_dict(),
                        error=error,
                        timestamp=time.time(),
                        retry_count=worker.retries,
                        heartbeats_sent=worker.sent,
                    )
                )
            except Exception:
                logging.exception("Heartbeat error callback failed")

    def failed(self, worker, error):
        with self.lock:
            worker.failed += 1
            worker.retries += 1
            worker.last_error = error.details()
        if error.code() not in RETRYABLE or worker.retries >= 3:
            self.notify(worker, error)
            return False
        return True

    def succeeded(self, worker):
        with self.lock:
            worker.sent += 1
            worker.retries = 0

    def register(self, worker):
        self.ensure_open()
        if worker.claim in self.workers:
            raise NzovuError("Heartbeat already active for this exact claim")
        self.workers[worker.claim] = worker


class SyncHeartbeats(Registry):
    def __init__(self, *args):
        super().__init__(*args)
        self.executor = ThreadPoolExecutor(max_workers=self.capacity, thread_name_prefix="nzovu-heartbeat")

    def start(self, claim, stub):
        worker = Worker(claim, threading.Event())
        with self.lock:
            self.register(worker)
            try:
                worker.future = self.executor.submit(self.run, worker, stub)
            except BaseException:
                del self.workers[claim]
                raise

    def run(self, worker, stub):
        try:
            while not worker.stop.is_set() and self.remaining(worker) > 0 and worker.sent < self.max_count:
                try:
                    response = stub.SendMessageHeartBeat(
                        SendMessageHeartBeatRequest(**worker.claim.to_dict()),
                        timeout=max(0.001, min(self.rpc_timeout, self.remaining(worker))),
                    )
                    self.succeeded(worker)
                    if response.state != Message.Metadata.RUNNING:
                        return
                    delay = self.interval
                except grpc.RpcError as error:
                    if worker.stop.is_set() or not self.failed(worker, error):
                        return
                    delay = 2**worker.retries
                worker.stop.wait(min(delay, max(0, self.remaining(worker))))
        except Exception as error:
            self.notify(worker, error)
        finally:
            self.finish(worker)

    def join(self, futures, timeout):
        _, pending = wait(futures, timeout=timeout)
        if pending:
            self.executor.shutdown(wait=False)
            raise TimeoutError("Heartbeat shutdown timed out; a callback or RPC is still running")
        self.executor.shutdown(wait=True)


class AsyncHeartbeats(Registry):
    def start(self, claim, stub):
        worker = Worker(claim, asyncio.Event())
        with self.lock:
            self.register(worker)
            worker.future = asyncio.create_task(self.run(worker, stub))
            # A task cancelled before its first instruction still releases its slot.
            worker.future.add_done_callback(lambda _: self.finish(worker))

    async def run(self, worker, stub):
        try:
            while not worker.stop.is_set() and self.remaining(worker) > 0 and worker.sent < self.max_count:
                try:
                    response = await stub.SendMessageHeartBeat(
                        SendMessageHeartBeatRequest(**worker.claim.to_dict()),
                        timeout=max(0.001, min(self.rpc_timeout, self.remaining(worker))),
                    )
                    self.succeeded(worker)
                    if response.state != Message.Metadata.RUNNING:
                        return
                    delay = self.interval
                except grpc.RpcError as error:
                    if worker.stop.is_set() or not self.failed(worker, error):
                        return
                    delay = 2**worker.retries
                try:
                    await asyncio.wait_for(worker.stop.wait(), timeout=min(delay, max(0, self.remaining(worker))))
                except asyncio.TimeoutError:
                    pass
        except Exception as error:
            self.notify(worker, error)

    async def join(self, tasks, timeout):
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=timeout)

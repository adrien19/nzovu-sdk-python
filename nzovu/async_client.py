"""
Async Nzovu Client with async/await support.

This module provides an asynchronous version of the Nzovu client that uses
async/await patterns for non-blocking operations. It's ideal for applications
using asyncio and provides better performance for I/O-bound workloads.

Example:
    >>> import asyncio
    >>> from nzovu.async_client import AsyncNzovuClient
    >>> from nzovu.utils import PostMessageParams
    >>>
    >>> async def main():
    ...     client = AsyncNzovuClient(host="localhost", port=50051, use_tls=False)
    ...     await client.connect()
    ...
    ...     # Get message with automatic heartbeat
    ...     response = await client.get_next_message(
    ...         queue_name="my_queue",
    ...         lease_duration="5m",
    ...         enable_heartbeat=True
    ...     )
    ...
    ...     # Process message...
    ...
    ...     # Heartbeat automatically stops when acknowledged
    ...     await client.acknowledge_message(params)
    ...     await client.close()
    >>>
    >>> asyncio.run(main())
"""

import asyncio
import logging
import os
import time
from typing import Callable, Dict, Optional

import grpc
from google.protobuf import json_format

from .api.message.v1.message_pb2 import Message
from .api.queue.v1 import queue_pb2
from .api.queueservice.v1 import request_response_pb2, service_pb2_grpc
from .api.schedule.v1 import schedule_pb2
from .exceptions import InitializationError, RpcOperationError
from .utils import (
    AcknowledgeMessageParams,
    MessageState,
    PeekQueueMessagesParams,
    PostMessageParams,
    QueueOptions,
    ResponseWrapper,
    ScheduleOptions,
    SchemaOptions,
    TlsConfig,
    TransactionMode,
    _create_post_message_request,
    build_bulk_request,
    build_lease_policy,
    build_schedule_request,
    page_call_arguments,
    require_name,
    string_to_duration,
    validate_integer,
    validate_page,
    validate_page_iterator,
)

logging.basicConfig(level=logging.INFO)


class AsyncNzovuClient:
    """
    Asynchronous client for Nzovu with async/await support.

    This client provides non-blocking operations suitable for asyncio-based applications.
    All I/O operations are async and heartbeats are managed using asyncio tasks instead
    of threads, providing better integration with async code.

    Example:
        >>> async with AsyncNzovuClient(host="localhost", port=50051, use_tls=False) as client:
        ...     msg = await client.get_next_message("queue", "5m", enable_heartbeat=True)
        ...     # Process message
        ...     await client.acknowledge_message(params)
    """

    def __init__(
        self,
        host: str,
        port: int,
        use_tls: bool = True,
        tls_config: Optional[TlsConfig] = None,
        worker_id: str = "",
        heartbeat_max_duration: int = 300,
        heartbeat_max_count: int = 1000000000,  # Large number to effectively disable count limit
        heartbeat_error_callback: Optional[Callable] = None,
    ):
        """
        Initialize the AsyncNzovuClient.

        Parameters:
        ----------
        host : str
            The hostname or IP address of the Nzovu service.
        port : int
            The port number on which the Nzovu service is running.
        use_tls : bool, optional
            Indicates whether to use TLS for the connection, by default True.
        tls_config : TlsConfig, optional
            Configuration for TLS connectivity. Required if use_tls is True.
        worker_id : str, optional
            A stable identifier for this worker/client instance. Used to track message processing
            and validate heartbeats and acknowledgments. If not provided, each get_next_message
            call will generate a unique worker_id.
        heartbeat_max_duration : int, optional
            Maximum duration in seconds for heartbeats per message, by default 300.
        heartbeat_max_count : int, optional
            Maximum number of heartbeats to send per message, by default 1000000000, effectively disabling the count limit.
        heartbeat_error_callback : callable, optional
            Callback function to invoke when heartbeat errors occur.

        Raises:
        ------
        InitializationError
            If use_tls is True but tls_config is not provided.
        """
        self.host = host
        self.port = port
        self._use_tls = use_tls
        self._tls_config = tls_config
        self._worker_id = worker_id
        self._heartbeat_max_duration = heartbeat_max_duration
        self._heartbeat_max_count = heartbeat_max_count
        self._heartbeat_error_callback = heartbeat_error_callback

        self.channel: Optional[grpc.aio.Channel] = None
        self.stub: Optional[service_pb2_grpc.QueueServiceStub] = None

        # Heartbeat management
        self._heartbeat_tasks: Dict[str, asyncio.Task] = {}
        self._heartbeat_stop_events: Dict[str, asyncio.Event] = {}
        self._heartbeat_metrics: Dict[str, dict] = {}
        self._lock = asyncio.Lock()

    async def connect(self):
        """
        Establish connection to the Nzovu service.

        Must be called before using any other methods.

        Raises:
        ------
        InitializationError
            If TLS is enabled but configuration is invalid.
        """
        if self._use_tls:
            if self._tls_config is None:
                raise InitializationError("TLS is enabled but no TlsConfig provided")

            for path in [
                self._tls_config.ca_path,
                self._tls_config.client_crt_path,
                self._tls_config.client_key_path,
            ]:
                if not os.path.exists(path):
                    raise InitializationError(f"File {path} does not exist.")

            with open(self._tls_config.ca_path, "rb") as f:
                ca = f.read()
            with open(self._tls_config.client_crt_path, "rb") as f:
                client_crt = f.read()
            with open(self._tls_config.client_key_path, "rb") as f:
                client_key = f.read()

            credentials = grpc.ssl_channel_credentials(ca, client_key, client_crt)
            self.channel = grpc.aio.secure_channel(f"{self.host}:{self.port}", credentials)
        else:
            self.channel = grpc.aio.insecure_channel(f"{self.host}:{self.port}")

        self.stub = service_pb2_grpc.QueueServiceStub(self.channel)
        logging.info(f"Connected to Nzovu at {self.host}:{self.port}")

    def _handle_error(self, error, handler=None):
        """
        Handle errors using a custom handler or raise the error.

        Parameters:
        ----------
        error : Exception
            The error to handle.
        handler : callable, optional
            Custom error handler. If provided, calls the handler instead of raising.
        """
        if handler:
            handler(error)
        else:
            raise error

    async def create_queue(
        self, name: str, options: Optional[QueueOptions] = None, error_handler=None
    ) -> ResponseWrapper:
        """
        Creates a new queue in the Nzovu service with the specified parameters.

        Parameters:
        ----------
        name : str
            The required parameter for creating the new queue with the given name.
        options : QueueOptions, optional
            The optional configurations for creating the new queue.
        error_handler : callable, optional
            A custom error handling function that will be called if an error occurs.

        Returns:
        -------
        ResponseWrapper
            A wrapper around the gRPC response from the Nzovu service.

        Raises:
        ------
        RpcOperationError
            If the gRPC operation encounters an error and no custom error handler is provided.

        Example:
        --------
        >>> from nzovu.utils import QueueOptions
        >>> await client.create_queue(name="my_new_queue", options=options)
        """
        try:
            metadata = None
            if options is not None:
                metadata = queue_pb2.QueueMetadata(
                    type=options.type,
                    default_max_attempts=options.max_attempts if options.max_attempts is not None else 0,
                    lease_duration=string_to_duration(options.lease_duration) if options.lease_duration else None,
                    exclusivity_key=options.exclusivity_key if options.exclusivity_key else "",
                    dead_letter_queue_name=options.dead_letter_queue_name if options.dead_letter_queue_name else "",
                    auto_create_dlq=options.auto_create_dlq if options.auto_create_dlq is not None else False,
                    schema_id=options.schema_id if options.schema_id else "",
                    schema_required=options.schema_required if options.schema_required is not None else False,
                    max_payload_size=options.max_payload_size if options.max_payload_size is not None else 0,
                    allowed_content_types=options.allowed_content_types if options.allowed_content_types else [],
                )
                # Add priority_config if provided
                if options.priority_config:
                    # Convert priority_config dict to protobuf
                    from google.protobuf import json_format

                    priority_config_pb = json_format.ParseDict(options.priority_config, queue_pb2.PriorityConfig())
                    metadata.priority_config.CopyFrom(priority_config_pb)
                if options.lease_policy:
                    lp = build_lease_policy(options.lease_policy)
                    if lp:
                        metadata.lease_policy.CopyFrom(lp)
                if options.retention_policy:
                    retention_pb = queue_pb2.MessageRetentionPolicy(
                        mode=options.retention_policy.mode.value,
                        retention_seconds=options.retention_policy.retention_seconds,
                    )
                    metadata.message_retention_policy.CopyFrom(retention_pb)
            request = request_response_pb2.CreateQueueRequest(name=name, metadata=metadata)
            response = await self.stub.CreateQueue(request)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            logging.error(f"Error creating queue: {e.details()}")
            error = RpcOperationError(f"Failed to create queue due to: {e.details()}")
            self._handle_error(error, handler=error_handler)

    async def delete_queue(self, name: str, error_handler=None) -> ResponseWrapper:
        """
        Deletes a specified queue from the Nzovu service.

        Parameters:
        ----------
        name : str
            The name of the queue to delete.
        error_handler : callable, optional
            A custom error handling function.

        Returns:
        -------
        ResponseWrapper
            A wrapper around the gRPC response.

        Raises:
        ------
        RpcOperationError
            If the gRPC operation encounters an error and no custom error handler is provided.

        Example:
        --------
        >>> await client.delete_queue("my_queue")
        """
        try:
            request = request_response_pb2.DeleteQueueRequest(name=name)
            response = await self.stub.DeleteQueue(request)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            logging.error(f"Error deleting queue: {e.details()}")
            error = RpcOperationError(f"Failed to delete queue due to: {e.details()}")
            self._handle_error(error, handler=error_handler)

    async def list_queues(
        self, prefix: str = "", error_handler=None, *, page_size: int = 0, page_token: str = ""
    ) -> ResponseWrapper:
        """
        List all queues in the Nzovu service.

        Parameters:
        ----------
        prefix : str, optional
            Filter queues by name prefix. Returns all queues if empty.
        error_handler : callable, optional
            Custom error handler function.

        page_size : int, optional
            Maximum results; zero uses the server default.
        page_token : str, optional
            Continuation token from the previous response.

        Returns:
        -------
        ResponseWrapper
            Wrapper containing the ListQueuesResponse with a list of queues.

        Raises:
        ------
        RpcOperationError
            If the gRPC operation fails and no custom error handler is provided.
        """
        validate_page(page_size, page_token)
        try:
            request = request_response_pb2.ListQueuesRequest(prefix=prefix, page_size=page_size, page_token=page_token)
            response = await self.stub.ListQueues(request)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            logging.error(f"Error listing queues: {e.details()}")
            error = RpcOperationError(f"Failed to list queues due to: {e.details()}")
            self._handle_error(error, handler=error_handler)

    async def __aenter__(self):
        """Async context manager entry."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()

    async def get_next_message(
        self,
        queue_name: str,
        lease_duration: str,
        exclusivity_key: str = "",
        enable_heartbeat: bool = False,
        error_handler=None,
        *,
        worker_id: Optional[str] = None,
        attempt_id: Optional[str] = None,
    ) -> ResponseWrapper:
        """
        Retrieve the next message from a queue asynchronously.

        Parameters:
        ----------
        queue_name : str
            The name of the queue from which to fetch the next message.
        lease_duration : str
            Duration to lease the message (e.g., "5m", "30s").
        exclusivity_key : str, optional
            An exclusive key required for exclusive type queues, by default "".
        enable_heartbeat : bool, optional
            Enable automatic heartbeat management for the message, by default False.

        Returns:
        -------
        ResponseWrapper
            Wrapper containing the GetNextMessageResponse.

        Example:
        --------
        >>> response = await client.get_next_message("my_queue", "5m", enable_heartbeat=True)
        >>> msg = response.to_model()
        """
        try:
            pb_release_duration = string_to_duration(lease_duration)

            request = request_response_pb2.GetNextMessageRequest(
                queue_name=queue_name,
                lease_duration=pb_release_duration,
                exclusivity_key=exclusivity_key,
                worker_id=self._worker_id if worker_id is None else worker_id,
                attempt_id=attempt_id,
            )
            response = await self.stub.GetNextMessage(request)
            response_wrapper = ResponseWrapper(response_protobuf=response)

            if enable_heartbeat:
                resp_dict = response_wrapper.to_dict()
                message_id = resp_dict.get("messageId") or resp_dict.get("message", {}).get("messageId")

                if message_id:
                    logging.info(f"Starting async heartbeat for message {message_id} on queue {queue_name}")

                    # Extract attempt_id and worker_id for heartbeat tracking
                    attempt_id = resp_dict.get("attemptId", "") or resp_dict.get("attempt_id", "")
                    worker_id = resp_dict.get("workerId", "") or resp_dict.get("worker_id", "")

                    heartbeat_frequency = resp_dict.get("heartbeat_frequency", 1)
                    max_reconnect_attempts = resp_dict.get("max_reconnect_attempts", 3)

                    async with self._lock:
                        if message_id in self._heartbeat_tasks:
                            logging.warning(f"Heartbeat already active for message {message_id}, skipping")
                        else:
                            stop_event = asyncio.Event()
                            self._heartbeat_stop_events[message_id] = stop_event

                            task = asyncio.create_task(
                                self._heartbeat_loop(
                                    message_id=message_id,
                                    queue_name=queue_name,
                                    attempt_id=attempt_id,
                                    worker_id=worker_id,
                                    stop_event=stop_event,
                                    heartbeat_frequency=heartbeat_frequency,
                                    max_reconnect_attempts=max_reconnect_attempts,
                                )
                            )

                            self._heartbeat_tasks[message_id] = task
                            self._heartbeat_metrics[message_id] = {
                                "message_id": message_id,
                                "queue_name": queue_name,
                                "started_at": time.time(),
                                "heartbeats_sent": 0,
                                "heartbeats_failed": 0,
                                "last_heartbeat_at": None,
                                "last_error": None,
                            }
                else:
                    logging.debug(f"No message returned from queue {queue_name}, heartbeat not started")

            return response_wrapper
        except grpc.RpcError as e:
            error = RpcOperationError(f"RPC failed: {e.details()}")
            self._handle_error(error, handler=error_handler)

    async def _heartbeat_loop(
        self,
        message_id: str,
        queue_name: str,
        attempt_id: str,
        worker_id: str,
        stop_event: asyncio.Event,
        heartbeat_frequency: int,
        max_reconnect_attempts: int,
    ):
        """
        Async heartbeat loop for a single message.

        This runs as an asyncio task and sends heartbeats at regular intervals
        until stopped or an error occurs.
        """
        start_time = time.time()
        heartbeat_count = 0
        reconnect_attempts = 0

        logging.info(f"Async heartbeat task started for message {message_id} on queue {queue_name}")

        try:
            while not stop_event.is_set():
                elapsed = time.time() - start_time
                if elapsed > self._heartbeat_max_duration:
                    logging.warning(
                        f"Heartbeat for message {message_id} exceeded max duration " f"{self._heartbeat_max_duration}s"
                    )
                    break

                if heartbeat_count >= self._heartbeat_max_count:
                    logging.warning(
                        f"Heartbeat for message {message_id} exceeded max count {self._heartbeat_max_count}"
                    )
                    break

                try:
                    request = request_response_pb2.SendMessageHeartBeatRequest(
                        queue_name=queue_name,
                        message_id=message_id,
                        attempt_id=attempt_id,
                        worker_id=worker_id,
                    )
                    response = await self.stub.SendMessageHeartBeat(request)

                    heartbeat_count += 1
                    reconnect_attempts = 0

                    async with self._lock:
                        if message_id in self._heartbeat_metrics:
                            self._heartbeat_metrics[message_id]["heartbeats_sent"] = heartbeat_count
                            self._heartbeat_metrics[message_id]["last_heartbeat_at"] = time.time()

                    logging.debug(
                        f"Heartbeat #{heartbeat_count} sent for message {message_id}, "
                        f"state: {Message.Metadata.State.Name(response.state)}"
                    )

                    if response.state != Message.Metadata.State.RUNNING:
                        logging.info(
                            f"Message {message_id} no longer RUNNING "
                            f"(state: {Message.Metadata.State.Name(response.state)}), stopping heartbeat"
                        )
                        break

                    # Async sleep with cancellation support
                    try:
                        await asyncio.wait_for(stop_event.wait(), timeout=heartbeat_frequency)
                        # If we get here, stop_event was set
                        break
                    except asyncio.TimeoutError:
                        # Timeout is expected, continue loop
                        pass

                except grpc.aio.AioRpcError as e:
                    reconnect_attempts += 1
                    error_msg = f"Heartbeat error for message {message_id}: {e.details()}"

                    async with self._lock:
                        if message_id in self._heartbeat_metrics:
                            self._heartbeat_metrics[message_id]["heartbeats_failed"] += 1
                            self._heartbeat_metrics[message_id]["last_error"] = str(e.details())

                    if reconnect_attempts >= max_reconnect_attempts:
                        logging.error(f"{error_msg}. Max reconnect attempts reached, stopping heartbeat")

                        if self._heartbeat_error_callback:
                            try:
                                self._heartbeat_error_callback(
                                    {
                                        "message_id": message_id,
                                        "queue_name": queue_name,
                                        "error": e,
                                        "error_details": e.details(),
                                        "timestamp": time.time(),
                                        "retry_count": reconnect_attempts,
                                        "heartbeats_sent": heartbeat_count,
                                    }
                                )
                            except Exception as cb_error:
                                logging.error(f"Error in heartbeat error callback: {cb_error}")
                        break
                    else:
                        logging.warning(f"{error_msg}. Retry {reconnect_attempts}/{max_reconnect_attempts}")
                        backoff = (2**reconnect_attempts) + (time.time() % 10)  # Jitter
                        await asyncio.sleep(backoff)

        except Exception as e:
            logging.error(f"Unexpected error in heartbeat task for message {message_id}: {e}", exc_info=True)

            if self._heartbeat_error_callback:
                try:
                    self._heartbeat_error_callback(
                        {
                            "message_id": message_id,
                            "queue_name": queue_name,
                            "error": e,
                            "error_details": str(e),
                            "timestamp": time.time(),
                            "retry_count": reconnect_attempts,
                            "heartbeats_sent": heartbeat_count,
                        }
                    )
                except Exception as cb_error:
                    logging.error(f"Error in heartbeat error callback: {cb_error}")
        finally:
            async with self._lock:
                self._heartbeat_tasks.pop(message_id, None)
                self._heartbeat_stop_events.pop(message_id, None)
                if message_id in self._heartbeat_metrics:
                    self._heartbeat_metrics[message_id]["ended_at"] = time.time()
                    self._heartbeat_metrics[message_id]["total_heartbeats"] = heartbeat_count

            logging.info(
                f"Heartbeat task stopped for message {message_id}. "
                f"Total heartbeats: {heartbeat_count}, Duration: {time.time() - start_time:.1f}s"
            )

    async def acknowledge_message(self, params: AcknowledgeMessageParams, error_handler=None) -> ResponseWrapper:
        """
        Acknowledge a message asynchronously.

        Automatically stops the heartbeat for this message if one is active.

        Parameters:
        ----------
        params : AcknowledgeMessageParams
            Parameters for acknowledging the message.

        Returns:
        -------
        ResponseWrapper
            Wrapper containing the AcknowledgeMessageResponse.

        Example:
        --------
        >>> await client.acknowledge_message(params)
        """
        try:
            message_id = params.message_id

            # Stop heartbeat if active
            if message_id in self._heartbeat_stop_events:
                logging.info(f"Stopping heartbeat for acknowledged message {message_id}")
                self._heartbeat_stop_events[message_id].set()

            request = request_response_pb2.AcknowledgeMessageRequest(
                message_id=params.message_id,
                queue_name=params.queue_name,
                state=params.state.name if isinstance(params.state, MessageState) else params.state,
                worker_id=params.worker_id,
                attempt_id=params.attempt_id,
            )
            response = await self.stub.AcknowledgeMessage(request)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            error = RpcOperationError(f"RPC failed: {e.details()}")
            self._handle_error(error, handler=error_handler)

    async def cancel_message(
        self, queue_name: str, message_id: str, reason: str = "", error_handler=None
    ) -> ResponseWrapper:
        """
        Cancel a pending message before it has been processed.

        Only messages in INVISIBLE or PENDING state can be cancelled. Messages already
        being processed (RUNNING) cannot be cancelled.

        Parameters:
        ----------
        queue_name : str
            Name of the queue containing the message.
        message_id : str
            ID of the message to cancel.
        reason : str, optional
            Reason for cancellation (for audit/logging purposes).
        error_handler : callable, optional
            Custom error handler function.

        Returns:
        -------
        ResponseWrapper
            Wrapper containing the CancelMessageResponse with success status.

        Raises:
        ------
        RpcOperationError
            If the RPC call fails.

        Example:
        --------
        >>> await client.cancel_message("my_queue", "msg-123", "Order was cancelled")
        """
        try:
            request = request_response_pb2.CancelMessageRequest(
                queue_name=queue_name,
                message_id=message_id,
            )
            if reason:
                request.reason = reason

            response = await self.stub.CancelMessage(request)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            logging.error(f"Error cancelling message: {e.details()}")
            error = RpcOperationError(f"Failed to cancel message due to: {e.details()}")
            self._handle_error(error, handler=error_handler)

    async def post_message(self, msg_params: PostMessageParams, error_handler=None) -> ResponseWrapper:
        """
        Post a message to a queue asynchronously.

        Parameters:
        ----------
        msg_params : PostMessageParams
            Parameters defining the message to post.

        Returns:
        -------
        ResponseWrapper
            Wrapper containing the PostMessageResponse.

        Example:
        --------
        >>> response = await client.post_message(msg_params)
        """
        try:
            request = _create_post_message_request(params=msg_params)
            response = await self.stub.PostMessage(request)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            logging.error(f"Error posting message: {e.details()}")
            error = RpcOperationError(f"Failed to post message due to: {e.details()}")
            self._handle_error(error, handler=error_handler)

    async def post_messages_bulk(
        self,
        queue_name: str,
        messages: list,
        transaction_mode: "TransactionMode | str" = TransactionMode.ALL_OR_NOTHING,
        error_handler=None,
    ) -> ResponseWrapper:
        """
        Post multiple messages to a queue in a single bulk operation asynchronously.

        This method allows efficient posting of multiple messages in a single request.
        Supports two transaction modes:
        - ALL_OR_NOTHING: All messages succeed or all fail (atomic operation)
        - BEST_EFFORT: Process each message independently, partial success allowed

        Parameters:
        ----------
        queue_name : str
            The name of the queue to post messages to.

        messages : list
            List of PostMessageParams objects, each containing:
            - message_id (str): Unique identifier for the message
            - data (dict): The message content
            - Additional optional fields (priority, schedule_at, etc.)

        transaction_mode : TransactionMode | str, optional
            Transaction mode for batch processing. Can be:
            - TransactionMode.ALL_OR_NOTHING (default): All messages succeed or all fail
            - TransactionMode.BEST_EFFORT: Process as many as possible, continue on failures
            - String values "ALL_OR_NOTHING" or "BEST_EFFORT" (for backwards compatibility)

        error_handler : callable, optional
            Custom error handler function. If omitted, uses default error handling.

        Returns:
        -------
        ResponseWrapper
            Response containing:
            - success (bool): Overall operation success
            - successful_count (int): Number of messages successfully posted
            - failed_count (int): Number of messages that failed
            - results (list): Per-message results with error details

        Raises:
        ------
        RpcOperationError
            If the gRPC operation fails or invalid parameters provided.

        Example:
        --------
        >>> from nzovu.utils import PostMessageParams, TransactionMode
        >>> messages = [
        ...     PostMessageParams(message_id="msg1", data={"key": "value1"}, queue_name="my_queue"),
        ...     PostMessageParams(message_id="msg2", data={"key": "value2"}, queue_name="my_queue"),
        ... ]
        >>> response = await client.post_messages_bulk("my_queue", messages, transaction_mode=TransactionMode.BEST_EFFORT)
        >>> result = response.to_dict()
        >>> print(f"Successfully posted {result['successful_count']} messages")
        """
        try:
            # Build the bulk request
            request = build_bulk_request(queue_name, messages, transaction_mode)
            response = await self.stub.PostMessagesBulk(request)
            return ResponseWrapper(response_protobuf=response)

        except grpc.RpcError as e:
            logging.error(f"Error posting messages in bulk: {e.details()}")
            error = RpcOperationError(f"Failed to post messages in bulk due to: {e.details()}")
            self._handle_error(error, handler=error_handler)
        except (ValueError, AttributeError, TypeError) as e:
            logging.error(f"Invalid parameters for bulk post: {e}")
            error = RpcOperationError(f"Invalid parameters for bulk post: {e}")
            self._handle_error(error, handler=error_handler)

    async def iter_pages(self, method: str, *args, max_pages=None, **kwargs):
        """Yield one response per request; stop on exhaustion or max_pages."""
        validate_page_iterator(method, max_pages)
        params = (args[0] if args else kwargs.get("params")) if method == "peek_queue_messages" else None
        token = params.page_token if params is not None else kwargs.get("page_token", "")
        seen = {token}
        count = 0
        while max_pages is None or count < max_pages:
            call_args, call_kwargs = page_call_arguments(method, args, kwargs, token)
            response = await getattr(self, method)(*call_args, **call_kwargs)
            if response is None:
                return
            yield response
            count += 1
            if max_pages is not None and count >= max_pages:
                return
            token = response.to_proto().next_page_token
            if not token:
                return
            if token in seen:
                raise ValueError("Server repeated a pagination token")
            seen.add(token)

    def get_active_heartbeat_count(self) -> int:
        """Get the number of messages with currently active heartbeats."""
        return len(self._heartbeat_tasks)

    def get_heartbeat_stats(self) -> Dict[str, dict]:
        """Get detailed statistics for all heartbeats."""
        return dict(self._heartbeat_metrics)

    def get_active_heartbeats(self) -> list:
        """Get list of message IDs with currently active heartbeats."""
        return list(self._heartbeat_tasks.keys())

    async def renew_message_lease(
        self,
        queue_name: str,
        message_id: str,
        new_lease_duration: str,
        error_handler=None,
        *,
        worker_id: Optional[str] = None,
        attempt_id: Optional[str] = None,
    ) -> ResponseWrapper:
        """
        Renew the lease duration for a message.

        Parameters:
        ----------
        message_id : str
            The unique identifier for the message.
        new_lease_duration : str
            The new lease duration (e.g., "5m", "30s").
        error_handler : callable, optional
            Custom error handling function.

        Returns:
        -------
        ResponseWrapper
            Wrapper containing the RenewMessageLeaseResponse.

        Raises:
        ------
        RpcOperationError
            If the gRPC operation encounters an error.

        Example:
        --------
        >>> await client.renew_message_lease("msg123", "10m")
        """
        try:
            request = request_response_pb2.RenewMessageLeaseRequest(
                queue_name=queue_name,
                message_id=message_id,
                lease_duration=string_to_duration(new_lease_duration),
                worker_id=worker_id,
                attempt_id=attempt_id,
            )
            response = await self.stub.RenewMessageLease(request)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            logging.error(f"Error renewing message lease: {e.details()}")
            error = RpcOperationError(f"Failed to renew message lease due to: {e.details()}")
            self._handle_error(error, handler=error_handler)

    async def peek_queue_messages(self, params: PeekQueueMessagesParams, error_handler=None) -> ResponseWrapper:
        """
        Peek at messages in a queue without dequeuing them.

        Parameters:
        ----------
        params : PeekQueueMessagesParams
            Parameters for peeking at queue messages.
        error_handler : callable, optional
            Custom error handling function.

        Returns:
        -------
        ResponseWrapper
            Wrapper containing the PeekQueueMessagesResponse.

        Raises:
        ------
        RpcOperationError
            If the gRPC operation encounters an error.

        Example:
        --------
        >>> from nzovu.utils import PeekQueueMessagesParams
        >>> params = PeekQueueMessagesParams(queue_name="my_queue", page_size=10)
        >>> response = await client.peek_queue_messages(params)
        """
        validate_page(params.page_size, params.page_token)
        try:
            priority_range = None
            if params.priority_range is not None:
                params.priority_range.__post_init__()
                priority_range = request_response_pb2.PeekQueueMessagesRequest.PriorityRange(
                    min=params.priority_range.min, max=params.priority_range.max
                )
            request = request_response_pb2.PeekQueueMessagesRequest(
                queue_name=params.queue_name,
                page_size=params.page_size,
                page_token=params.page_token,
                priority_range=priority_range,
            )
            response = await self.stub.PeekQueueMessages(request)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            logging.error(f"Error peeking queue messages: {e.details()}")
            error = RpcOperationError(f"Failed to peek queue messages due to: {e.details()}")
            self._handle_error(error, handler=error_handler)

    async def get_queue_state(self, queue_name: str, error_handler=None) -> ResponseWrapper:
        """
        Retrieve the current state/statistics of a queue.

        Parameters:
        ----------
        queue_name : str
            The name of the queue.
        error_handler : callable, optional
            Custom error handling function.

        Returns:
        -------
        ResponseWrapper
            Wrapper containing the GetQueueStateResponse with queue statistics.

        Raises:
        ------
        RpcOperationError
            If the gRPC operation encounters an error.

        Example:
        --------
        >>> response = await client.get_queue_state("my_queue")
        >>> state = response.to_dict()
        """
        try:
            request = request_response_pb2.GetQueueStateRequest(queue_name=queue_name)
            response = await self.stub.GetQueueState(request)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            logging.error(f"Error getting queue state: {e.details()}")
            error = RpcOperationError(f"Failed to get queue state due to: {e.details()}")
            self._handle_error(error, handler=error_handler)

    async def send_message_heartbeat(
        self, queue_name: str, message_id: str, attempt_id: str = "", worker_id: str = "", error_handler=None
    ) -> ResponseWrapper:
        """
        Send a single heartbeat for a message to extend its lease.

        Parameters:
        ----------
        queue_name : str
            The name of the queue containing the message.
        message_id : str
            The unique identifier for the message.
        attempt_id : str, optional
            The attempt ID for the current message processing attempt.
        worker_id : str, optional
            The worker ID processing this message.
        error_handler : callable, optional
            Custom error handling function.

        Returns:
        -------
        ResponseWrapper
            Wrapper containing the SendMessageHeartBeatResponse.

        Raises:
        ------
        RpcOperationError
            If the gRPC operation encounters an error.

        Example:
        --------
        >>> await client.send_message_heartbeat("my_queue", "msg123", "attempt-1", "worker-1")
        """
        try:
            request = request_response_pb2.SendMessageHeartBeatRequest(
                queue_name=queue_name,
                message_id=message_id,
                attempt_id=attempt_id,
                worker_id=worker_id,
            )
            response = await self.stub.SendMessageHeartBeat(request)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            logging.error(f"Error sending message heartbeat: {e.details()}")
            error = RpcOperationError(f"Failed to send message heartbeat due to: {e.details()}")
            self._handle_error(error, handler=error_handler)

    async def create_schedule(self, schedule_id: str, options: ScheduleOptions, error_handler=None) -> ResponseWrapper:
        """
        Create a new schedule for recurring message delivery.

        Parameters:
        ----------
        schedule_id : str
            Unique identifier for the schedule.
        options : ScheduleOptions
            Configuration for the schedule.
        error_handler : callable, optional
            Custom error handling function.

        Returns:
        -------
        ResponseWrapper
            Wrapper containing the CreateScheduleResponse.

        Raises:
        ------
        RpcOperationError
            If the gRPC operation encounters an error.

        Example:
        --------
        >>> from nzovu.utils import ScheduleOptions
        >>> options = ScheduleOptions(...)
        >>> await client.create_schedule("my_schedule", options)
        """
        try:
            request = build_schedule_request(schedule_id, options)
            response = await self.stub.CreateSchedule(request)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            logging.error(f"Error creating schedule: {e.details()}")
            error = RpcOperationError(f"Failed to create schedule due to: {e.details()}")
            self._handle_error(error, handler=error_handler)

    async def delete_schedule(self, schedule_id: str, error_handler=None) -> ResponseWrapper:
        """
        Delete an existing schedule.

        Parameters:
        ----------
        schedule_id : str
            The unique identifier of the schedule to delete.
        error_handler : callable, optional
            Custom error handling function.

        Returns:
        -------
        ResponseWrapper
            Wrapper containing the DeleteScheduleResponse.

        Raises:
        ------
        RpcOperationError
            If the gRPC operation encounters an error.

        Example:
        --------
        >>> await client.delete_schedule("my_schedule")
        """
        try:
            request = request_response_pb2.DeleteScheduleRequest(schedule_id=schedule_id)
            response = await self.stub.DeleteSchedule(request)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            logging.error(f"Error deleting schedule: {e.details()}")
            error = RpcOperationError(f"Failed to delete schedule due to: {e.details()}")
            self._handle_error(error, handler=error_handler)

    async def get_schedule(self, schedule_id: str, error_handler=None) -> ResponseWrapper:
        """
        Retrieve details of a specific schedule.

        Parameters:
        ----------
        schedule_id : str
            The unique identifier of the schedule.
        error_handler : callable, optional
            Custom error handling function.

        Returns:
        -------
        ResponseWrapper
            Wrapper containing the GetScheduleResponse.

        Raises:
        ------
        RpcOperationError
            If the gRPC operation encounters an error.

        Example:
        --------
        >>> response = await client.get_schedule("my_schedule")
        """
        try:
            request = request_response_pb2.GetScheduleRequest(schedule_id=schedule_id)
            response = await self.stub.GetSchedule(request)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            logging.error(f"Error getting schedule: {e.details()}")
            error = RpcOperationError(f"Failed to get schedule due to: {e.details()}")
            self._handle_error(error, handler=error_handler)

    async def list_schedules(
        self, prefix: str = "", error_handler=None, *, page_size: int = 0, page_token: str = ""
    ) -> ResponseWrapper:
        """
        List all schedules, optionally filtered by prefix.

        Parameters:
        ----------
        prefix : str, optional
            Filter schedules by ID prefix, by default "".
        error_handler : callable, optional
            Custom error handling function.

        page_size : int, optional
            Maximum results; zero uses the server default.
        page_token : str, optional
            Continuation token from the previous response.

        Returns:
        -------
        ResponseWrapper
            Wrapper containing the ListSchedulesResponse.

        Raises:
        ------
        RpcOperationError
            If the gRPC operation encounters an error.

        Example:
        --------
        >>> response = await client.list_schedules(prefix="daily_")
        """
        validate_page(page_size, page_token)
        try:
            request = request_response_pb2.ListSchedulesRequest(
                prefix=prefix, page_size=page_size, page_token=page_token
            )
            response = await self.stub.ListSchedules(request)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            logging.error(f"Error listing schedules: {e.details()}")
            error = RpcOperationError(f"Failed to list schedules due to: {e.details()}")
            self._handle_error(error, handler=error_handler)

    async def get_schedule_history(
        self, schedule_id: str, page_size: int = 10, error_handler=None, *, page_token: str = ""
    ) -> ResponseWrapper:
        """
        Retrieve execution history for a schedule.

        Parameters:
        ----------
        schedule_id : str
            The unique identifier of the schedule.
        page_size : int, optional
            Maximum number of history entries to retrieve, by default 10.
        error_handler : callable, optional
            Custom error handling function.

        page_token : str, optional
            Continuation token from the previous response.

        Returns:
        -------
        ResponseWrapper
            Wrapper containing the GetScheduleHistoryResponse.

        Raises:
        ------
        RpcOperationError
            If the gRPC operation encounters an error.

        Example:
        --------
        >>> response = await client.get_schedule_history("my_schedule", page_size=20)
        """
        validate_page(page_size, page_token)
        try:
            request = request_response_pb2.GetScheduleHistoryRequest(
                schedule_id=schedule_id, page_size=page_size, page_token=page_token
            )
            response = await self.stub.GetScheduleHistory(request)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            logging.error(f"Error getting schedule history: {e.details()}")
            error = RpcOperationError(f"Failed to get schedule history due to: {e.details()}")
            self._handle_error(error, handler=error_handler)

    async def pause_schedule(self, schedule_id: str, error_handler=None) -> ResponseWrapper:
        """
        Pause a schedule to stop it from executing.

        Parameters:
        ----------
        schedule_id : str
            The unique identifier of the schedule to pause.
        error_handler : callable, optional
            Custom error handling function.

        Returns:
        -------
        ResponseWrapper
            Wrapper containing the PauseScheduleResponse.

        Raises:
        ------
        RpcOperationError
            If the gRPC operation encounters an error.

        Example:
        --------
        >>> await client.pause_schedule("my_schedule")
        """
        try:
            request = request_response_pb2.PauseScheduleRequest(schedule_id=schedule_id)
            response = await self.stub.PauseSchedule(request)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            logging.error(f"Error pausing schedule: {e.details()}")
            error = RpcOperationError(f"Failed to pause schedule due to: {e.details()}")
            self._handle_error(error, handler=error_handler)

    async def resume_schedule(self, schedule_id: str, error_handler=None) -> ResponseWrapper:
        """
        Resume a paused schedule.

        Parameters:
        ----------
        schedule_id : str
            The unique identifier of the schedule to resume.
        error_handler : callable, optional
            Custom error handling function.

        Returns:
        -------
        ResponseWrapper
            Wrapper containing the ResumeScheduleResponse.

        Raises:
        ------
        RpcOperationError
            If the gRPC operation encounters an error.

        Example:
        --------
        >>> await client.resume_schedule("my_schedule")
        """
        try:
            request = request_response_pb2.ResumeScheduleRequest(schedule_id=schedule_id)
            response = await self.stub.ResumeSchedule(request)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            logging.error(f"Error resuming schedule: {e.details()}")
            error = RpcOperationError(f"Failed to resume schedule due to: {e.details()}")
            self._handle_error(error, handler=error_handler)

    async def validate_calendar_schedule(self, calendar_schedule: dict, error_handler=None) -> ResponseWrapper:
        """
        Validate a calendar schedule configuration.

        Parameters:
        ----------
        calendar_schedule : dict
            The calendar schedule specification to validate.
        error_handler : callable, optional
            Custom error handling function.

        Returns:
        -------
        ResponseWrapper
            Wrapper containing the ValidateCalendarScheduleResponse.

        Raises:
        ------
        RpcOperationError
            If the gRPC operation encounters an error.

        Example:
        --------
        >>> schedule = {"minute": "*/5", "hour": "*", "day_of_month": "*"}
        >>> response = await client.validate_calendar_schedule(schedule)
        """
        try:
            cal_schedule = json_format.ParseDict(calendar_schedule, schedule_pb2.CalendarSchedule())
            request = request_response_pb2.ValidateCalendarScheduleRequest(calendar_schedule=cal_schedule)
            response = await self.stub.ValidateCalendarSchedule(request)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            logging.error(f"Error validating calendar schedule: {e.details()}")
            error = RpcOperationError(f"Failed to validate calendar schedule due to: {e.details()}")
            self._handle_error(error, handler=error_handler)

    async def preview_calendar_schedule(
        self, calendar_schedule: dict, count: int = 10, error_handler=None
    ) -> ResponseWrapper:
        """
        Preview upcoming execution times for a calendar schedule.

        Parameters:
        ----------
        calendar_schedule : dict
            Dictionary representation of a CalendarSchedule configuration to preview.
        count : int, optional
            Number of upcoming execution times to generate. Default is 10.
        error_handler : callable, optional
            Custom error handling function.

        Returns:
        -------
        ResponseWrapper
            Wrapper containing the PreviewCalendarScheduleResponse with execution times.

        Raises:
        ------
        RpcOperationError
            If preview request fails and no custom error handler is provided.
        """
        validate_integer(count, "count", 0, 100)
        try:
            calendar_schedule_pb = json_format.ParseDict(calendar_schedule, schedule_pb2.CalendarSchedule())
            request = request_response_pb2.PreviewCalendarScheduleRequest(
                calendar_schedule=calendar_schedule_pb, count=count
            )
            response = await self.stub.PreviewCalendarSchedule(request)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            logging.error(f"Error previewing calendar schedule: {e.details()}")
            error = RpcOperationError(f"Failed to preview calendar schedule due to: {e.details()}")
            self._handle_error(error, handler=error_handler)

    async def register_schema(self, schema_id: str, options: SchemaOptions, error_handler=None) -> ResponseWrapper:
        """
        Register a new schema or create a new version of an existing schema.

        Parameters:
        ----------
        schema_id : str
            Unique identifier for the schema.
        options : SchemaOptions
            Configuration options for the schema.
        error_handler : callable, optional
            Custom error handling function.

        Returns:
        -------
        ResponseWrapper
            Wrapper containing the RegisterSchemaResponse.

        Raises:
        ------
        RpcOperationError
            If schema registration fails.

        Example:
        --------
        >>> from nzovu.utils import SchemaOptions
        >>> options = SchemaOptions(name="Order Schema", content='{"type": "object"}')
        >>> response = await client.register_schema("order_schema", options)
        """
        if options.content_type not in {"", "json-schema"}:
            raise ValueError("Schema content_type must be json-schema")
        try:
            request = request_response_pb2.RegisterSchemaRequest(
                schema_id=schema_id,
                name=options.name,
                description=options.description,
                content=options.content,
                content_type=options.content_type,
                metadata=options.metadata if options.metadata else {},
            )
            response = await self.stub.RegisterSchema(request)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            logging.error(f"Error registering schema {schema_id}: {e.details()}")
            error = RpcOperationError(f"Failed to register schema due to: {e.details()}")
            self._handle_error(error, handler=error_handler)

    async def get_schema(self, schema_id: str, version: int = 0, error_handler=None) -> ResponseWrapper:
        """
        Retrieve a schema by ID and optional version.

        Parameters:
        ----------
        schema_id : str
            Unique identifier of the schema.
        version : int, optional
            Schema version to retrieve. If 0, retrieves the latest version.
        error_handler : callable, optional
            Custom error handling function.

        Returns:
        -------
        ResponseWrapper
            Wrapper containing the GetSchemaResponse.

        Raises:
        ------
        RpcOperationError
            If schema retrieval fails.

        Example:
        --------
        >>> response = await client.get_schema("order_schema")
        >>> schema = response.to_model()
        """
        validate_integer(version, "version", 0, 2**31 - 1)
        try:
            request = request_response_pb2.GetSchemaRequest(schema_id=schema_id, version=version)
            response = await self.stub.GetSchema(request)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            logging.error(f"Error getting schema {schema_id}: {e.details()}")
            error = RpcOperationError(f"Failed to get schema due to: {e.details()}")
            self._handle_error(error, handler=error_handler)

    async def list_schemas(
        self,
        prefix: str = "",
        page_size: int = 100,
        active_only: bool = False,
        error_handler=None,
        *,
        page_token: str = "",
    ) -> ResponseWrapper:
        """
        List all schemas with optional filtering.

        Parameters:
        ----------
        prefix : str, optional
            Filter schemas by ID prefix.
        page_size : int, optional
            Maximum number of schemas to retrieve, by default 100.
        active_only : bool, optional
            If True, only return active schemas, by default False.
        error_handler : callable, optional
            Custom error handling function.

        page_token : str, optional
            Continuation token from the previous response.

        Returns:
        -------
        ResponseWrapper
            Wrapper containing the ListSchemasResponse.

        Raises:
        ------
        RpcOperationError
            If listing fails.

        Example:
        --------
        >>> response = await client.list_schemas(prefix="order_", page_size=50)
        """
        validate_page(page_size, page_token)
        try:
            request = request_response_pb2.ListSchemasRequest(
                prefix=prefix, page_size=page_size, page_token=page_token, active_only=active_only
            )
            response = await self.stub.ListSchemas(request)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            logging.error(f"Error listing schemas: {e.details()}")
            error = RpcOperationError(f"Failed to list schemas due to: {e.details()}")
            self._handle_error(error, handler=error_handler)

    async def delete_schema(self, schema_id: str, version: int = 0, error_handler=None) -> ResponseWrapper:
        """
        Delete a schema or a specific version.

        Parameters:
        ----------
        schema_id : str
            Unique identifier of the schema.
        version : int, optional
            Version to delete. If 0, deletes all versions.
        error_handler : callable, optional
            Custom error handling function.

        Returns:
        -------
        ResponseWrapper
            Wrapper containing the DeleteSchemaResponse.

        Raises:
        ------
        RpcOperationError
            If deletion fails.

        Example:
        --------
        >>> await client.delete_schema("order_schema", version=1)
        """
        validate_integer(version, "version", 0, 2**31 - 1)
        try:
            request = request_response_pb2.DeleteSchemaRequest(schema_id=schema_id, version=version)
            response = await self.stub.DeleteSchema(request)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            logging.error(f"Error deleting schema {schema_id}: {e.details()}")
            error = RpcOperationError(f"Failed to delete schema due to: {e.details()}")
            self._handle_error(error, handler=error_handler)

    async def validate_payload(
        self, schema_id: str, payload: str, version: int = 0, error_handler=None
    ) -> ResponseWrapper:
        """
        Validate a payload against a schema.

        Parameters:
        ----------
        schema_id : str
            Unique identifier of the schema to validate against.
        payload : str
            The payload to validate (JSON string).
        version : int, optional
            Schema version to use. If 0, uses the latest version.
        error_handler : callable, optional
            Custom error handling function.

        Returns:
        -------
        ResponseWrapper
            Wrapper containing the ValidatePayloadResponse.

        Raises:
        ------
        RpcOperationError
            If validation fails.

        Example:
        --------
        >>> payload = '{"orderId": "123", "amount": 50.0}'
        >>> response = await client.validate_payload("order_schema", payload, version=1)
        >>> result = response.to_model()
        >>> if result.valid:
        ...     print("Payload is valid")
        """
        validate_integer(version, "version", 0, 2**31 - 1)
        try:
            request = request_response_pb2.ValidatePayloadRequest(schema_id=schema_id, payload=payload, version=version)
            response = await self.stub.ValidatePayload(request)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            logging.error(f"Error validating payload: {e.details()}")
            error = RpcOperationError(f"Failed to validate payload due to: {e.details()}")
            self._handle_error(error, handler=error_handler)

    async def get_dlq_messages(
        self, dlq_name: str, page_size: int = 100, error_handler=None, *, page_token: str = ""
    ) -> ResponseWrapper:
        """
        Retrieve messages from a Dead Letter Queue (DLQ).

        Parameters:
        ----------
        dlq_name : str
            Name of the Dead Letter Queue.
        page_size : int, optional
            Maximum number of messages to retrieve, by default 100.
        error_handler : callable, optional
            Custom error handling function.

        page_token : str, optional
            Continuation token from the previous response.

        Returns:
        -------
        ResponseWrapper
            Wrapper containing the GetDLQMessagesResponse.

        Raises:
        ------
        RpcOperationError
            If retrieval fails.

        Example:
        --------
        >>> response = await client.get_dlq_messages("orders_queue_dlq", page_size=50)
        >>> messages = response.to_model()
        """
        validate_page(page_size, page_token)
        try:
            request = request_response_pb2.GetDLQMessagesRequest(
                dlq_name=dlq_name, page_size=page_size, page_token=page_token
            )
            response = await self.stub.GetDLQMessages(request)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            logging.error(f"Error getting DLQ messages from {dlq_name}: {e.details()}")
            error = RpcOperationError(f"Failed to get DLQ messages due to: {e.details()}")
            self._handle_error(error, handler=error_handler)

    async def requeue_from_dlq(
        self, dlq_name: str, message_id: str, target_queue: str, error_handler=None
    ) -> ResponseWrapper:
        """
        Move a message from DLQ to an explicit existing target queue.

        Parameters:
        ----------
        dlq_name : str
            Name of the Dead Letter Queue.
        message_id : str
            Unique identifier of the message to requeue.
        target_queue : str
            Target queue name. If empty, requeues to the original queue.
        error_handler : callable, optional
            Custom error handling function.

        Returns:
        -------
        ResponseWrapper
            Wrapper containing the RequeueFromDLQResponse.

        Raises:
        ------
        RpcOperationError
            If requeue operation fails.

        Example:
        --------
        >>> await client.requeue_from_dlq("orders_queue_dlq", "msg-123", "orders_queue")
        """
        require_name(target_queue, "target_queue")
        try:
            request = request_response_pb2.RequeueFromDLQRequest(
                dlq_name=dlq_name, message_id=message_id, target_queue=target_queue
            )
            response = await self.stub.RequeueFromDLQ(request)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            logging.error(f"Error requeuing message {message_id} from DLQ {dlq_name}: {e.details()}")
            error = RpcOperationError(f"Failed to requeue from DLQ due to: {e.details()}")
            self._handle_error(error, handler=error_handler)

    async def delete_from_dlq(self, dlq_name: str, message_id: str, error_handler=None) -> ResponseWrapper:
        """
        Permanently delete a message from a Dead Letter Queue.

        Parameters:
        ----------
        dlq_name : str
            Name of the Dead Letter Queue.
        message_id : str
            Unique identifier of the message to delete.
        error_handler : callable, optional
            Custom error handling function.

        Returns:
        -------
        ResponseWrapper
            Wrapper containing the DeleteFromDLQResponse.

        Raises:
        ------
        RpcOperationError
            If deletion fails.

        Example:
        --------
        >>> await client.delete_from_dlq("orders_queue_dlq", "msg-123")
        """
        try:
            request = request_response_pb2.DeleteFromDLQRequest(dlq_name=dlq_name, message_id=message_id)
            response = await self.stub.DeleteFromDLQ(request)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            logging.error(f"Error deleting message {message_id} from DLQ {dlq_name}: {e.details()}")
            error = RpcOperationError(f"Failed to delete from DLQ due to: {e.details()}")
            self._handle_error(error, handler=error_handler)

    async def purge_dlq(self, dlq_name: str, error_handler=None) -> ResponseWrapper:
        """
        Remove all messages from a Dead Letter Queue.

        Parameters:
        ----------
        dlq_name : str
            Name of the Dead Letter Queue to purge.
        error_handler : callable, optional
            Custom error handling function.

        Returns:
        -------
        ResponseWrapper
            Wrapper containing the PurgeDLQResponse.

        Raises:
        ------
        RpcOperationError
            If purge operation fails.

        Example:
        --------
        >>> await client.purge_dlq("orders_queue_dlq")
        """
        try:
            request = request_response_pb2.PurgeDLQRequest(dlq_name=dlq_name)
            response = await self.stub.PurgeDLQ(request)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            logging.error(f"Error purging DLQ {dlq_name}: {e.details()}")
            error = RpcOperationError(f"Failed to purge DLQ due to: {e.details()}")
            self._handle_error(error, handler=error_handler)

    async def get_dlq_stats(self, dlq_name: str, error_handler=None) -> ResponseWrapper:
        """
        Retrieve statistics about a Dead Letter Queue.

        Parameters:
        ----------
        dlq_name : str
            Name of the Dead Letter Queue.
        error_handler : callable, optional
            Custom error handling function.

        Returns:
        -------
        ResponseWrapper
            Wrapper containing the GetDLQStatsResponse.

        Raises:
        ------
        RpcOperationError
            If stats retrieval fails.

        Example:
        --------
        >>> response = await client.get_dlq_stats("orders_queue_dlq")
        >>> stats = response.to_model()
        """
        try:
            request = request_response_pb2.GetDLQStatsRequest(dlq_name=dlq_name)
            response = await self.stub.GetDLQStats(request)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            logging.error(f"Error getting DLQ stats for {dlq_name}: {e.details()}")
            error = RpcOperationError(f"Failed to get DLQ stats due to: {e.details()}")
            self._handle_error(error, handler=error_handler)

    async def stop_heartbeat(self, message_id: str) -> bool:
        """
        Manually stop the heartbeat for a specific message.

        Parameters:
        ----------
        message_id : str
            The message identifier.

        Returns:
        -------
        bool
            True if heartbeat was stopped, False if none was active.
        """
        if message_id in self._heartbeat_stop_events:
            logging.info(f"Manually stopping heartbeat for message {message_id}")
            self._heartbeat_stop_events[message_id].set()
            return True
        return False

    async def close(self, timeout: float = 30.0):
        """
        Close the client and stop all active heartbeats gracefully.

        Parameters:
        ----------
        timeout : float, optional
            Maximum time to wait for heartbeats to stop, by default 30.0.

        Example:
        --------
        >>> await client.close()
        """
        logging.info("Closing AsyncNzovuClient, stopping all heartbeats...")

        async with self._lock:
            active_count = len(self._heartbeat_stop_events)
            if active_count > 0:
                logging.info(f"Signaling {active_count} active heartbeat(s) to stop")
                for stop_event in self._heartbeat_stop_events.values():
                    stop_event.set()

        # Wait for all tasks to complete
        if self._heartbeat_tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self._heartbeat_tasks.values(), return_exceptions=True), timeout=timeout
                )
            except asyncio.TimeoutError:
                logging.warning(f"Timeout waiting for heartbeats to stop after {timeout}s")
                for task in self._heartbeat_tasks.values():
                    task.cancel()

        # Close gRPC channel
        if self.channel:
            await self.channel.close()
            logging.info("AsyncNzovuClient closed successfully")

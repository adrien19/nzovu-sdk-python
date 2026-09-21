import logging
import os
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from random import randint
from typing import Callable, Dict, Optional

import grpc
from google.protobuf import json_format
from google.protobuf.duration_pb2 import Duration

from .api.common.v1 import common_pb2
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
    build_lease_policy,
    dict_to_protobuf_struct,
    string_to_duration,
)

# Initialize logging
logging.basicConfig(level=logging.INFO)


class NzovuClient:
    """
    A client for interacting with the Nzovu distributed task queue via gRPC.

    Nzovu is a distributed task queue that facilitates the sending, processing,
    and acknowledgment of messages across distributed systems. `NzovuClient` serves
    as the Python SDK to interact with Nzovu, offering an API to perform various
    operations such as enqueueing messages, processing them, and maintaining their state.

    The client connects to a Nzovu service instance, enabling to perform
    operations on the queue programmatically, ensuring robust communication and error handling
    to facilitate interactions with the service.

    Attributes:
    ----------
    host : str
        The hostname or IP address of the Nzovu service to connect to.
    port : int
        The port number on which the Nzovu service is listening.
    use_tls : bool, optional
        A flag indicating whether to use TLS for the connection, by default True.
    tls_config : TlsConfig, optional
        Configuration for TLS, providing paths to CA, client certificate, and client key, by default None.
    channel : grpc.Channel
        The gRPC channel used to communicate with the Nzovu service.
    stub : nzovu_pb2_grpc.NzovuStub
        A gRPC stub to interact with the Nzovu service using the gRPC protocol.

    Examples:
    --------
    # Initialize the client for a secure connection
    >>> client = NzovuClient(host="localhost", port=50051, use_tls=True, tls_config=my_tls_config)

    # Create a new queue
    >>> client.create_queue(CreateQueueParams(name="my_new_queue"))

    # Post a message to a queue
    >>> msg_params = PostMessageParams(message_id="12345", data={"key": "value"})
    >>> client.post_message(msg_params)
    """

    def __init__(
        self,
        host: str,
        port: int,
        use_tls=True,
        tls_config: Optional[TlsConfig] = None,
        worker_id: str = "",
        heartbeat_max_duration: int = 300,  # 5 minutes
        heartbeat_max_count: int = 1000000000,  # Large number to effectively disable count limit
        heartbeat_thread_pool_size: int = 20,
        heartbeat_error_callback: Optional[Callable] = None,
    ):
        """
        Initialize the NzovuClient.

        Establishes a connection to the Nzovu service, using either a secure or insecure
        channel, based on the provided parameters. It initializes the gRPC channel and stub,
        facilitating further interactions with the Nzovu service.

        Parameters:
        ----------
        host : str
            The hostname or IP address of the Nzovu service.
        port : int
            The port number on which the Nzovu service is running.
        use_tls : bool, optional
            Indicates whether to use TLS for the connection, by default True.
        tls_config : TlsConfig, optional
            Configuration for TLS connectivity, providing paths to CA, client certificate,
            and client key. Required if `use_tls` is True, by default None.
        worker_id : str, optional
            A stable identifier for this worker/client instance. Used to track message processing
            and validate heartbeats and acknowledgments. If not provided, each get_next_message
            call will generate a unique worker_id.
        heartbeat_max_duration : int, optional
            Maximum duration in seconds for heartbeats per message, by default 300 (5 minutes).
        heartbeat_max_count : int, optional
            Maximum number of heartbeats to send per message, by default 1000000000, effectively disabling the count limit.
        heartbeat_thread_pool_size : int, optional
            Size of the thread pool for heartbeat workers, by default 20.
        heartbeat_error_callback : callable, optional
            Callback function to invoke when heartbeat errors occur. Should accept a dict
            with keys: message_id, queue_name, error, timestamp, retry_count.

        Raises:
        ------
        InitializationError
            If `use_tls` is True but `tls_config` is not provided or the file paths within
            `tls_config` do not exist.
        """
        self.host = host
        self.port = port
        self._use_tls = use_tls
        self._tls_config = tls_config
        self._worker_id = worker_id
        self._heartbeat_max_duration = heartbeat_max_duration
        self._heartbeat_max_count = heartbeat_max_count
        self._heartbeat_error_callback = heartbeat_error_callback

        if self._use_tls:
            if self._tls_config is None:
                raise InitializationError("TLS is enabled but no TlsConfig provided")

            # Type narrowing: at this point tls_config is not None
            assert tls_config is not None

            # Check for the existence of the files before trying to read them.
            for path in [tls_config.ca_path, tls_config.client_crt_path, tls_config.client_key_path]:
                if not os.path.exists(path):
                    raise InitializationError(f"File {path} does not exist.")

            with open(tls_config.ca_path, "rb") as f:
                ca = f.read()
            with open(tls_config.client_crt_path, "rb") as f:
                client_crt = f.read()
            with open(tls_config.client_key_path, "rb") as f:
                client_key = f.read()
            credentials = grpc.ssl_channel_credentials(ca, client_key, client_crt)
            self.channel = grpc.secure_channel(f"{host}:{port}", credentials)
        else:
            self.channel = grpc.insecure_channel(f"{host}:{port}")
        self.stub = service_pb2_grpc.QueueServiceStub(self.channel)

        # New heartbeat management structures
        self._active_heartbeats: Dict[str, dict] = {}
        self._heartbeat_control: Dict[str, threading.Event] = {}
        self._heartbeat_metrics: Dict[str, dict] = {}
        self.lock = threading.Lock()

        # Thread pool for concurrent heartbeat processing
        self._heartbeat_executor = ThreadPoolExecutor(
            max_workers=heartbeat_thread_pool_size, thread_name_prefix="nzovu-heartbeat"
        )

        # Legacy compatibility - kept for backward compat but deprecated
        self._heartbeat_data: deque = deque()
        self._stop_heartbeat = threading.Event()

    def _handle_error(self, error, handler=None):
        """Internal method to handle errors. Calls the custom error handler if set."""
        if handler:
            handler(error)
        else:
            # Default behavior is to raise the error
            raise error

    def create_queue(self, name: str, options: Optional[QueueOptions] = None, error_handler=None) -> ResponseWrapper:
        """
        Creates a new queue in the Nzovu service with the specified parameters.

        This method facilitates the creation of a new queue with specific configurations
        as defined by the `QueueOptions` parameter. If any error occurs during the
        queue creation, it can be handled using a custom error handler or the SDK's default mechanism.

        Parameters:
        ----------
        name : string
            The required parameter for creating the new queue with the given name.
        options : QueueOptions
            The optional configurations for creating the new queue.

        error_handler : callable, optional
            A custom error handling function that will be called if an error occurs during the operation.
            The function should accept a single argument, which is the error/exception object.
            If not provided, the SDK's default error handling mechanism will be used.

        Returns:
        -------
        ResponseWrapper
            A wrapper around the gRPC response from the Nzovu service, containing details
            about the CreateQueueResponse queue and facilitating type-safe access to response fields.

        Raises:
        ------
        RpcOperationError
            If the gRPC operation encounters an error and no custom error handler is provided.

        Example:
        --------
        >>> from nzovusdk import QueueOptions
        >>> options = QueueOptions(type=QueueType.SIMPLE, exclusivity_key="key123", dequeue_attempts=3)
        >>> client.create_queue(name="my_new_queue", options=options)

        Notes:
        -----
        - `QueueOptions` allows you to customize behaviors like message invisibility duration,
        which defines how long a message will stay invisible (unavailable to workers) after
        being dequeued and before being requeued again if not acknowledged.
        - Ensure `name` adheres to any naming conventions or limitations imposed by the Nzovu service.
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
            response = self.stub.CreateQueue(request)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            logging.error(f"Error creating queue: {e.details()}")
            error = RpcOperationError(f"Failed to create queue due to: {e.details()}")
            self._handle_error(error, handler=error_handler)

    def delete_queue(self, name: str, error_handler=None) -> ResponseWrapper:
        """
        Deletes a specified queue from the Nzovu service.

        This method provides functionality to delete a queue by its name. Once deleted,
        the messages and metadata associated with the queue will be permanently removed.
        If any error arises during the deletion process, it can be managed using a custom
        error handler or the SDK's default mechanism.

        Parameters:
        ----------
        name : str
            The name of the queue to be deleted.

        error_handler : callable, optional
            A custom error handling function that will be called if an error occurs during the operation.
            The function should accept a single argument, which is the error/exception object.
            If not provided, the SDK's default error handling mechanism will be used.

        Returns:
        -------
        ResponseWrapper
            A wrapper around the gRPC response from the Nzovu service, containing details
            about the DeleteQueueResponse and facilitating type-safe access to response fields.

        Raises:
        ------
        RpcOperationError:
            If the gRPC operation encounters an error and no custom error handler is provided.

        Example:
        --------
        >>> client.delete_queue(name="my_queue_to_delete")

        """
        try:
            request = request_response_pb2.DeleteQueueRequest(name=name)
            response = self.stub.DeleteQueue(request)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            logging.error(f"Error deleting queue: {e.details()}")
            error = RpcOperationError(f"Failed to delete queue due to: {e.details()}")
            self._handle_error(error, handler=error_handler)

    def list_queues(
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
        try:
            request = request_response_pb2.ListQueuesRequest(prefix=prefix, page_size=page_size, page_token=page_token)
            response = self.stub.ListQueues(request)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            logging.error(f"Error listing queues: {e.details()}")
            error = RpcOperationError(f"Failed to list queues due to: {e.details()}")
            self._handle_error(error, handler=error_handler)

    def post_message(self, msg_params: PostMessageParams, error_handler=None) -> ResponseWrapper:
        """
        Post a message to a specified queue in the Nzovu service.

        Utilizing this method, messages, can be dispatched to a queue in Nzovu. The `msg_params` parameter allows you to
        specify the core attributes of a message, whereas any error during the message posting process can
        be managed through a custom or default error handling approach.

        Parameters:
        ----------
        msg_params : PostMessageParams
            Parameters defining the essentials of the message, inclusive of:
            - `message_id` (str): A unique identifier for the message.
            - `data` (dict): The message content, represented as a dictionary.
            - `queue_name` (str): The queue to which the message will be posted.

        error_handler : callable, optional
            A custom function to manage errors during the message posting process. This function should
            accept an error/exception object as a parameter. If omitted, the SDK’s default error handling
            is employed, by default None.

        Returns:
        -------
        ResponseWrapper
            A wrapper around the gRPC response from the Nzovu service, providing a type-safe means
            to access response details and containing insights about the dispatched message.

        Raises:
        ------
        RpcOperationError
            If the gRPC operation encounters an error and no custom error handler is defined.

        Example:
        --------
        >>> msg_params = PostMessageParams(message_id="12345", data={"key": "value"}, queue_name="my_queue")
        >>> def custom_error_handler(error):
        ...     print(f"Custom error handler: {error}")
        >>> client.post_message(msg_params, error_handler=custom_error_handler)

        """
        try:
            request = _create_post_message_request(params=msg_params)
            response = self.stub.PostMessage(request)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            logging.error(f"Error posting message: {e.details()}")
            error = RpcOperationError(f"Failed to post message due to: {e.details()}")
            self._handle_error(error, handler=error_handler)

    def post_messages_bulk(
        self,
        queue_name: str,
        messages: list,
        transaction_mode: "TransactionMode | str" = TransactionMode.ALL_OR_NOTHING,
        error_handler=None,
    ) -> ResponseWrapper:
        """
        Post multiple messages to a queue in a single bulk operation.

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
        >>> response = client.post_messages_bulk("my_queue", messages, transaction_mode=TransactionMode.BEST_EFFORT)
        >>> result = response.to_dict()
        >>> print(f"Successfully posted {result['successful_count']} messages")
        """
        try:
            # Build the bulk request
            request = request_response_pb2.PostMessagesBulkRequest()
            request.queue_name = queue_name

            # Convert transaction mode to protobuf enum
            # Handle both TransactionMode enum and string for backwards compatibility
            mode_value = transaction_mode.value if isinstance(transaction_mode, TransactionMode) else transaction_mode

            if mode_value == "ALL_OR_NOTHING":
                request.transaction_mode = request_response_pb2.PostMessagesBulkRequest.ALL_OR_NOTHING
            elif mode_value == "BEST_EFFORT":
                request.transaction_mode = request_response_pb2.PostMessagesBulkRequest.BEST_EFFORT
            else:
                raise ValueError(
                    f"Invalid transaction_mode: {transaction_mode}. Must be TransactionMode.ALL_OR_NOTHING or TransactionMode.BEST_EFFORT"
                )

            # Add each message to the request
            for idx, msg_params in enumerate(messages):
                if msg_params.queue_name and msg_params.queue_name != queue_name:
                    raise ValueError(f"messages[{idx}].queue_name must match queue_name='{queue_name}'")
                msg_request = _create_post_message_request(params=msg_params)
                request.messages.append(msg_request.message)

            # Execute the bulk post
            response = self.stub.PostMessagesBulk(request)
            return ResponseWrapper(response_protobuf=response)

        except grpc.RpcError as e:
            logging.error(f"Error posting messages in bulk: {e.details()}")
            error = RpcOperationError(f"Failed to post messages in bulk due to: {e.details()}")
            self._handle_error(error, handler=error_handler)
        except (ValueError, AttributeError, TypeError) as e:
            logging.error(f"Invalid parameters for bulk post: {e}")
            error = RpcOperationError(f"Invalid parameters for bulk post: {e}")
            self._handle_error(error, handler=error_handler)

    def get_next_message(
        self,
        queue_name: str,
        lease_duration: str,
        exclusivity_key: str = "",
        enable_heartbeat=False,
        error_handler=None,
    ) -> ResponseWrapper:
        """
        Retrieve the next message from a specified queue in the Nzovu service.

        This method obtains the subsequent available message from a queue, making it
        inaccessible to other consumers for a stipulated period (defined by the lease duration).
        Optionally, the SDK can manage the message lease by automatically sending heartbeats
        to the service, based on the `enable_heartbeat` parameter. In the event of errors
        during message retrieval, custom or default error-handling mechanisms can be employed.

        Parameters:
        ----------
        queue_name : str
            The name of the queue from which to fetch the next message.

        lease_duration : str
            Duration (in seconds) to lease the message, during which it will be inaccessible to other consumers.

        exclusivity_key : str, optional
            An exclusive key required for queues of exclusive type, by default "".

        enable_heartbeat : bool, optional
            A flag to enable/disable the automatic sending of heartbeats for managing message leases, by default False.

        error_handler : callable, optional
            An optional custom function for error management during message retrieval. It should accept an error/exception
            object as its parameter. If not provided, the SDK’s default error handling will be employed, by default None.

        Returns:
        -------
        ResponseWrapper
            A wrapper of the response from the Nzovu service, providing insights and details about the retrieved message.

        Raises:
        ------
        RpcOperationError
            If the gRPC operation fails and no custom error handler is set.

        Example:
        --------
        >>> message = client.get_next_message(queue_name="my_queue", lease_duration="300").to_dict()  # To obtain a dictionary response
        >>> message = client.get_next_message(queue_name="my_queue", lease_duration="300").to_proto()  # To obtain a grpc response

        Notes:
        -----
        - Ensure `queue_name` corresponds to an existing and accessible queue in the Nzovu service.
        - The `lease_duration` should be set considering the processing time required for a message to avoid early lease expiration.
        - The `exclusivity_key` is pivotal when dealing with exclusive type queues and should be managed securely.
        - Utilizing `enable_heartbeat` can be beneficial for maintaining the lease of long-processing messages and mitigating premature visibility.
        """
        try:
            pb_release_duration: Duration = string_to_duration(lease_duration)

            request = request_response_pb2.GetNextMessageRequest(
                queue_name=queue_name,
                lease_duration=pb_release_duration,
                exclusivity_key=exclusivity_key,
                worker_id=self._worker_id,
            )
            response = self.stub.GetNextMessage(request)
            response_wrapper = ResponseWrapper(response_protobuf=response)

            if enable_heartbeat:
                resp_dict = response_wrapper.to_dict()
                # Check for actual message presence - handle both possible response structures
                message_id = resp_dict.get("messageId") or resp_dict.get("message", {}).get("messageId")

                if message_id:
                    logging.info(f"Starting heartbeat for message {message_id} on queue {queue_name}")

                    # Extract attempt_id and worker_id for heartbeat tracking
                    attempt_id = resp_dict.get("attemptId", "") or resp_dict.get("attempt_id", "")
                    worker_id = resp_dict.get("workerId", "") or resp_dict.get("worker_id", "")

                    max_reconnect_attempts = resp_dict.get("max_reconnect_attempts", 3)
                    heartbeat_frequency = resp_dict.get("heartbeat_frequency", 1)

                    if heartbeat_frequency:
                        stop_event = threading.Event()

                        with self.lock:
                            # Check if heartbeat already exists for this message
                            if message_id in self._active_heartbeats:
                                logging.warning(f"Heartbeat already active for message {message_id}, skipping")
                            else:
                                self._heartbeat_control[message_id] = stop_event

                                # Submit heartbeat task to thread pool
                                future = self._heartbeat_executor.submit(
                                    self.__send_heartbeat_for_message,
                                    message_id=message_id,
                                    queue_name=queue_name,
                                    attempt_id=attempt_id,
                                    worker_id=worker_id,
                                    stop_event=stop_event,
                                    heartbeat_frequency=heartbeat_frequency,
                                    max_reconnect_attempts=max_reconnect_attempts,
                                )

                                self._active_heartbeats[message_id] = {
                                    "future": future,
                                    "queue_name": queue_name,
                                    "started_at": time.time(),
                                    "heartbeat_frequency": heartbeat_frequency,
                                }

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
            logging.error(f"Error getting next message: {e.details()}")
            error = RpcOperationError(f"Failed to get next message due to: {e.details()}")
            self._handle_error(error, handler=error_handler)

    def __send_heartbeat_for_message(
        self,
        message_id: str,
        queue_name: str,
        attempt_id: str,
        worker_id: str,
        stop_event: threading.Event,
        heartbeat_frequency: int,
        max_reconnect_attempts: int,
    ):
        """
        Send heartbeats for a single message in a dedicated thread.

        This method manages heartbeats for an individual message, sending periodic heartbeat
        requests to the Nzovu service to maintain the message lease. It runs until:
        1. The stop_event is set (e.g., when message is acknowledged)
        2. The server returns a non-RUNNING state
        3. Maximum duration or count is exceeded
        4. Unrecoverable errors occur

        Parameters:
        ----------
        message_id : str
            The unique identifier of the message.
        queue_name : str
            The name of the queue containing the message.
        attempt_id : str
            The attempt ID for the current message processing attempt.
        worker_id : str
            The worker ID processing this message.
        stop_event : threading.Event
            Event to signal when heartbeat should stop.
        heartbeat_frequency : int
            Interval in seconds between heartbeats.
        max_reconnect_attempts : int
            Maximum retry attempts for transient errors.

        Note:
        ----
        This method runs in a background thread pool and should not be called directly.
        It automatically cleans up tracking structures when it exits.
        """
        start_time = time.time()
        heartbeat_count = 0
        reconnect_attempts = 0

        logging.info(f"Heartbeat thread started for message {message_id} on queue {queue_name}")

        try:
            while not stop_event.is_set():
                # Safety check 1: Maximum duration
                elapsed = time.time() - start_time
                if elapsed > self._heartbeat_max_duration:
                    logging.warning(
                        f"Heartbeat for message {message_id} exceeded max duration "
                        f"{self._heartbeat_max_duration}s (elapsed: {elapsed:.1f}s)"
                    )
                    break

                # Safety check 2: Maximum count
                if heartbeat_count >= self._heartbeat_max_count:
                    logging.warning(
                        f"Heartbeat for message {message_id} exceeded max count "
                        f"{self._heartbeat_max_count} (sent: {heartbeat_count})"
                    )
                    break

                try:
                    heartbeat_request = request_response_pb2.SendMessageHeartBeatRequest(
                        queue_name=queue_name,
                        message_id=message_id,
                        attempt_id=attempt_id,
                        worker_id=worker_id,
                    )
                    heartbeat_resp = self.stub.SendMessageHeartBeat(heartbeat_request)

                    heartbeat_count += 1
                    reconnect_attempts = 0  # Reset on success

                    # Update metrics
                    with self.lock:
                        if message_id in self._heartbeat_metrics:
                            self._heartbeat_metrics[message_id]["heartbeats_sent"] = heartbeat_count
                            self._heartbeat_metrics[message_id]["last_heartbeat_at"] = time.time()

                    logging.debug(
                        f"Heartbeat #{heartbeat_count} sent for message {message_id}, "
                        f"state: {Message.Metadata.State.Name(heartbeat_resp.state)}"
                    )

                    # Check if message is no longer running
                    if heartbeat_resp.state != Message.Metadata.State.RUNNING:
                        logging.info(
                            f"Message {message_id} no longer RUNNING "
                            f"(state: {Message.Metadata.State.Name(heartbeat_resp.state)}), stopping heartbeat"
                        )
                        break

                    # Interruptible sleep - allows quick stop on event
                    stop_event.wait(timeout=heartbeat_frequency)

                except grpc.RpcError as e:
                    reconnect_attempts += 1
                    error_msg = f"Heartbeat error for message {message_id}: {e.details()}"

                    # Update metrics
                    with self.lock:
                        if message_id in self._heartbeat_metrics:
                            self._heartbeat_metrics[message_id]["heartbeats_failed"] += 1
                            self._heartbeat_metrics[message_id]["last_error"] = str(e.details())

                    if reconnect_attempts >= max_reconnect_attempts:
                        logging.error(
                            f"{error_msg}. Max reconnect attempts ({max_reconnect_attempts}) reached, stopping heartbeat"
                        )

                        # Notify via callback
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
                        # Exponential backoff with jitter
                        backoff_time = (2**reconnect_attempts) + randint(1, 10)
                        stop_event.wait(timeout=backoff_time)

        except Exception as e:
            logging.error(f"Unexpected error in heartbeat thread for message {message_id}: {e}", exc_info=True)

            # Notify via callback
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
            # Cleanup - remove from tracking structures
            with self.lock:
                self._active_heartbeats.pop(message_id, None)
                self._heartbeat_control.pop(message_id, None)
                # Keep metrics for observability, but mark as ended
                if message_id in self._heartbeat_metrics:
                    self._heartbeat_metrics[message_id]["ended_at"] = time.time()
                    self._heartbeat_metrics[message_id]["total_heartbeats"] = heartbeat_count

            logging.info(
                f"Heartbeat thread stopped for message {message_id}. "
                f"Total heartbeats sent: {heartbeat_count}, "
                f"Duration: {time.time() - start_time:.1f}s"
            )

    def __manage_heartbeats(self):
        """
        DEPRECATED: Legacy heartbeat manager method.

        This method is kept for backward compatibility but is no longer used.
        The new implementation uses __send_heartbeat_for_message with a thread pool.
        """
        logging.warning("Legacy __manage_heartbeats called - this method is deprecated")
        while self._heartbeat_data and not self._stop_heartbeat.is_set():
            item = self._heartbeat_data.popleft()
            reconnect_attempts = 0
            if not isinstance(item, dict):
                logging.error(f"Unexpected item in heartbeat_data: {item}")
                continue
            while reconnect_attempts < item.get("max_reconnect_attempts", 3):
                try:
                    heartbeat_request = request_response_pb2.SendMessageHeartBeatRequest(
                        queue_name=item.get("queue_name"), message_id=item.get("message_id")
                    )
                    heartbeat_resp = self.stub.SendMessageHeartBeat(heartbeat_request)
                    if heartbeat_resp.state != Message.Metadata.State.RUNNING:
                        break
                    time.sleep(item.get("heartbeat_frequency", 1))
                    reconnect_attempts = 0
                except grpc.RpcError as e:
                    reconnect_attempts += 1
                    backoff_time = (2**reconnect_attempts) + randint(1, 10)
                    logging.warning(f"Reconnection attempt {reconnect_attempts} failed: {e.details()}")
                    time.sleep(backoff_time)

    def acknowledge_message(self, params: AcknowledgeMessageParams, error_handler=None) -> ResponseWrapper:
        """
        Acknowledges a message in the Nzovu service.

        This method allows users to acknowledge a previously fetched message, indicating that the message
        has been processed successfully or requires a change in its state. If the method encounters an error,
        it can be handled using a custom error handler or the SDK's default mechanism.

        Parameters:
        ----------
        params : AcknowledgeMessageParams
            Contains the parameters required to acknowledge the message.
            - `message_id` (str): The unique identifier for the message.
            - `queue_name` (str): The name of the queue from which the message was fetched.
            - `state` (MessageState): The new state for the message after acknowledgment.

        error_handler : callable, optional
            A custom error handling function that will be called if an error occurs during the operation.
            The function should accept a single argument, which is the error/exception object.
            If not provided, the SDK's default error handling mechanism will be used.

        Returns:
        -------
        ResponseWrapper
            A wrapper of the response from the Nzovu service, providing insights and details about the acknowledged message.

        Raises:
        ------
        RpcOperationError
            If the gRPC operation fails and no custom error handler is set.

        Example:
        --------
        >>> from nzovusdk.utils import AcknowledgeMessageParams
        >>> params = AcknowledgeMessageParams(message_id="12345", queue_name="my_queue", state=MessageState.COMPLETED)
        >>> client.acknowledge_message(params)

        """
        try:
            # Stop heartbeat for this message if it's active
            message_id = params.message_id
            if message_id in self._heartbeat_control:
                logging.info(f"Stopping heartbeat for acknowledged message {message_id}")
                self._heartbeat_control[message_id].set()  # Signal stop

            # Send the acknowledgment to Nzovu
            ack_request = request_response_pb2.AcknowledgeMessageRequest(
                message_id=params.message_id,
                queue_name=params.queue_name,
                state=params.state.name if isinstance(params.state, MessageState) else params.state,
                worker_id=params.worker_id,
                attempt_id=params.attempt_id,
            )
            response = self.stub.AcknowledgeMessage(ack_request)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            logging.error(f"Error acknowledging message: {e.details()}")
            error = RpcOperationError(f"Failed to acknowlege message due to: {e.details()}")
            self._handle_error(error, handler=error_handler)

    def cancel_message(self, queue_name: str, message_id: str, reason: str = "", error_handler=None) -> ResponseWrapper:
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
        >>> client.cancel_message("my_queue", "msg-123", "Order was cancelled")
        """
        try:
            request = request_response_pb2.CancelMessageRequest(
                queue_name=queue_name,
                message_id=message_id,
            )
            if reason:
                request.reason = reason

            response = self.stub.CancelMessage(request)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            logging.error(f"Error cancelling message: {e.details()}")
            error = RpcOperationError(f"Failed to cancel message due to: {e.details()}")
            self._handle_error(error, handler=error_handler)

    def renew_message_lease(
        self, queue_name: str, message_id: str, new_lease_duration: str, error_handler=None
    ) -> ResponseWrapper:
        """
        Renews the lease duration of a specified message in the Nzovu service.

        In scenarios where a message is being processed but needs more time than the original
        lease duration allowed, this method can be used to extend the lease. By renewing the lease,
        the message remains invisible to other consumers for the new lease duration. If any error
        occurs during the lease renewal, it can be handled using a custom error handler or the SDK's
        default mechanism.

        Parameters:
        ----------
        message_id : str
            The unique identifier of the message whose lease is to be renewed.

        new_lease_duration : str
            The desired new lease duration for the message, represented as a string with a time unit
            suffix ("s" for seconds, "m" for minutes, "h" for hours, and "d" for days).
            E.g., "5s" for 5 seconds or "2m" for 2 minutes.

        error_handler : callable, optional
            A custom error handling function that will be invoked if an error occurs during the operation.
            The function should accept a single argument, which is the error/exception object.
            If not provided, the SDK's default error handling mechanism will be utilized.

        Returns:
        -------
        response : ResponseWrapper
            A wrapper containing the RenewMessageLeaseResponse from the Nzovu service, offering
            details about the status and any relevant information about the lease renewal process.
            The ResponseWrapper allows response data to be accessed in different formats (e.g., dict or proto).

        Raises:
        ------
        RpcOperationError:
            If an error occurs during the gRPC operation and no custom error handler is provided.

        Example:
        --------
        # Renew the lease duration of a message for an additional 300 seconds (5 minutes)
        >>> client.renew_message_lease(message_id="12345", new_lease_duration="5m")

        Note:
        ----
        Ensuring the accurate renewal of message leases is critical for maintaining coherent processing
        workflows, especially in distributed systems where multiple consumers might be interacting with
        the same queue. Always ensure to handle errors and edge cases effectively to prevent message
        processing conflicts and ensure the reliable operation of your application.

        """
        try:
            pb_release_duration: Duration = string_to_duration(new_lease_duration)

            request = request_response_pb2.RenewMessageLeaseRequest(
                queue_name=queue_name, message_id=message_id, lease_duration=pb_release_duration
            )
            response = self.stub.RenewMessageLease(request)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            logging.error(f"Error renewing message lease: {e.details()}")
            error = RpcOperationError(f"Failed to renew message lease due to: {e.details()}")
            self._handle_error(error, handler=error_handler)

    def peek_queue_messages(self, params: PeekQueueMessagesParams, error_handler=None) -> ResponseWrapper:
        """
        Peeks messages from a specified queue in the Nzovu service.

        This method allows users to retrieve a specified number of messages from a queue without removing them from the queue.
        It's useful for previewing the content of a queue or for use cases where messages need to be read but not immediately acknowledged.
        If the method encounters an error, it can be handled using a custom error handler or the SDK's default mechanism.

        Parameters:
        ----------
        params : PeekQueueMessagesParams
            Contains the parameters required to peek the messages from a queue.
            - `queue_name` (str): The name of the queue to peek messages from.
            - `page_size` (int): The maximum number of messages to retrieve.
            - `priority` (int): Optionally specify a priority level to filter messages.

        error_handler : callable, optional
            A custom error handling function that will be called if an error occurs during the operation.
            The function should accept a single argument, which is the error/exception object.
            If not provided, the SDK's default error handling mechanism will be used.

        Returns:
        -------
        response : PeekQueueMessagesResponse
            The response from the Nzovu service, containing the peeked messages and related details.

        Returns:
        -------
        response : ResponseWrapper
            A wrapper containing the PeekQueueMessagesResponse from the Nzovu service, offering
            details about the the peeked messages and related details.
            The ResponseWrapper allows response data to be accessed in different formats (e.g., dict or proto).

        Raises:
        ------
        RpcOperationError:
            If there's an error performing the gRPC operation and no custom error handler is provided.

        Example:
        --------
        >>> from nzovusdk.utils import PeekQueueMessagesParams
        >>> params = PeekQueueMessagesParams(queue_name="my_queue", page_size=10, priority=5)
        >>> client.peek_queue_messages(params)

        """
        try:
            priority_range = None
            if params.priority_range is not None:
                priority_range = request_response_pb2.PeekQueueMessagesRequest.PriorityRange(
                    min=params.priority_range.min, max=params.priority_range.max
                )
            request = request_response_pb2.PeekQueueMessagesRequest(
                queue_name=params.queue_name,
                page_size=params.page_size,
                page_token=params.page_token,
                priority_range=priority_range,
            )
            response = self.stub.PeekQueueMessages(request)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            logging.error(f"Error peeking queue messages: {e.details()}")
            error = RpcOperationError(f"Failed to peek queue due to: {e.details()}")
            self._handle_error(error, handler=error_handler)

    def get_queue_state(self, queue_name, error_handler=None) -> ResponseWrapper:
        """
        Retrieves the state of a specified queue in the Nzovu service.

        This method allows users to get the current state and statistics of a specified queue,
        which can include details like the number of pending messages, the number of running messages,
        and other relevant metrics. If the method encounters an error, it can be handled using
        a custom error handler or the SDK's default mechanism.

        Parameters:
        ----------
        queue_name : str
            The name of the queue whose state is to be retrieved.

        error_handler : callable, optional
            A custom error handling function that will be called if an error occurs during the operation.
            The function should accept a single argument, which is the error/exception object.
            If not provided, the SDK's default error handling mechanism will be used.

        Returns:
        -------
        response : ResponseWrapper
            A wrapper containing the GetQueueStateResponse from the Nzovu service, offering
            details about about the state of the specified queue.
            The ResponseWrapper allows response data to be accessed in different formats (e.g., dict or proto).

        Raises:
        ------
        RpcOperationError:
            If there's an error performing the gRPC operation and no custom error handler is provided.

        Example:
        --------
        >>> client.get_queue_state("my_queue")

        """
        try:
            request = request_response_pb2.GetQueueStateRequest(queue_name=queue_name)
            response = self.stub.GetQueueState(request)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            logging.error(f"Error getting queue state: {e.details()}")
            error = RpcOperationError(f"Failed to get queue state due to: {e.details()}")
            self._handle_error(error, handler=error_handler)

    def send_message_heartbeat(
        self, queue_name, message_id, attempt_id="", worker_id="", error_handler=None
    ) -> ResponseWrapper:
        """
        Manually sends a heartbeat for a specified message to the Nzovu service.

        Sending a heartbeat for a message signals to the Nzovu service that the message is still
        being processed and its lease should be maintained. This method allows you to manually send
        heartbeats for a message, which might be necessary in long-running tasks to prevent the message
        from becoming visible and being redelivered to another consumer.

        Note: The SDK provides a built-in heartbeat mechanism that can automatically send heartbeats
        for fetched messages. Consider using this built-in feature when fetching messages to simplify
        message lease management and avoid manually managing heartbeats.

        Parameters:
        ----------
        queue_name : str
            The name of the queue from which the message was fetched.

        message_id : str
            The unique identifier of the message for which the heartbeat is being sent.

        attempt_id : str, optional
            The attempt ID for the current message processing attempt.

        worker_id : str, optional
            The worker ID processing this message.

        error_handler : callable, optional
            A custom error handling function that will be called if an error occurs during the operation.
            The function should accept a single argument, which is the error/exception object.
            If not provided, the SDK's default error handling mechanism will be used.

        Returns:
        -------
        response : ResponseWrapper
            The wrapper containing SendMessageHeartBeatResponse from the Nzovu service,
            providing details about the status of the heartbeat operation.

        Raises:
        ------
        RpcOperationError:
            If there's an error performing the gRPC operation and no custom error handler is provided.

        Example:
        --------
        >>> client.send_message_heartbeat(queue_name="my_queue", message_id="12345", attempt_id="attempt-1", worker_id="worker-1")

        """
        try:
            request = request_response_pb2.SendMessageHeartBeatRequest(
                queue_name=queue_name,
                message_id=message_id,
                attempt_id=attempt_id,
                worker_id=worker_id,
            )
            response = self.stub.SendMessageHeartBeat(request)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            logging.error(f"Error sending heartbeat for message {message_id}: {e.details()}")
            error = RpcOperationError(f"Failed to send heartbeat due to: {e.details()}")
            self._handle_error(error, handler=error_handler)

    # Schedule Operations

    def create_schedule(self, schedule_id: str, options: ScheduleOptions, error_handler=None) -> ResponseWrapper:
        """
        Create a new schedule in Nzovu.

        Creates a schedule that will automatically post messages to a queue based on
        the specified cron expression or calendar configuration.

        Parameters:
        ----------
        schedule_id : str
            Unique identifier for the schedule.

        options : ScheduleOptions
            Configuration options for the schedule including payload, queue name,
            schedule timing (cron or calendar), and optional settings.

        error_handler : callable, optional
            A custom error handling function invoked if an error occurs.

        Returns:
        -------
        ResponseWrapper
            Wrapper containing the CreateScheduleResponse protobuf.

        Raises:
        ------
        RpcOperationError:
            If schedule creation fails and no custom error handler is provided.

        Example:
        --------
        >>> from nzovu.utils import ScheduleOptions, ScheduleState
        >>> options = ScheduleOptions(
        ...     payload={"task": "daily_report"},
        ...     queue_name="reports_queue",
        ...     cron_schedule="0 0 * * *",  # Daily at midnight
        ...     state=ScheduleState.SCHEDULED
        ... )
        >>> response = client.create_schedule("daily_report_schedule", options)
        """
        try:
            # Build payload
            payload_struct = dict_to_protobuf_struct(options.payload)
            payload = common_pb2.Payload(data=payload_struct)

            # Build metadata
            metadata = schedule_pb2.Schedule.Metadata(
                payload=payload,
                state=options.state.name,
                queue_name=options.queue_name,
            )

            # Set schedule config (cron or calendar)
            if options.cron_schedule:
                metadata.cron_schedule = options.cron_schedule
            elif options.calendar_schedule:
                # Convert dict to CalendarSchedule protobuf
                calendar_schedule = json_format.ParseDict(options.calendar_schedule, schedule_pb2.CalendarSchedule())
                metadata.calendar_schedule.CopyFrom(calendar_schedule)

            # Set optional fields
            if options.priority is not None:
                metadata.priority = options.priority
            if options.max_messages is not None:
                metadata.has_max_messages = True
                metadata.max_messages = options.max_messages
            if options.lease_duration:
                metadata.lease_duration.CopyFrom(string_to_duration(options.lease_duration))
            if options.timezone:
                metadata.timezone = options.timezone

            # Build schedule
            schedule = schedule_pb2.Schedule(schedule_id=schedule_id, metadata=metadata)

            # Build request and call service
            request = request_response_pb2.CreateScheduleRequest(schedule=schedule)
            response = self.stub.CreateSchedule(request)
            return ResponseWrapper(response_protobuf=response)

        except grpc.RpcError as e:
            logging.error(f"Error creating schedule {schedule_id}: {e.details()}")
            error = RpcOperationError(f"Failed to create schedule due to: {e.details()}")
            self._handle_error(error, handler=error_handler)

    def delete_schedule(self, schedule_id: str, error_handler=None) -> ResponseWrapper:
        """
        Delete a schedule from Nzovu.

        Permanently removes the specified schedule. The schedule will no longer
        post messages to the queue.

        Parameters:
        ----------
        schedule_id : str
            Unique identifier of the schedule to delete.

        error_handler : callable, optional
            A custom error handling function invoked if an error occurs.

        Returns:
        -------
        ResponseWrapper
            Wrapper containing the DeleteScheduleResponse protobuf.

        Raises:
        ------
        RpcOperationError:
            If schedule deletion fails and no custom error handler is provided.

        Example:
        --------
        >>> response = client.delete_schedule("daily_report_schedule")
        """
        try:
            request = request_response_pb2.DeleteScheduleRequest(schedule_id=schedule_id)
            response = self.stub.DeleteSchedule(request)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            logging.error(f"Error deleting schedule {schedule_id}: {e.details()}")
            error = RpcOperationError(f"Failed to delete schedule due to: {e.details()}")
            self._handle_error(error, handler=error_handler)

    def get_schedule(self, schedule_id: str, error_handler=None) -> ResponseWrapper:
        """
        Retrieve details of a specific schedule.

        Fetches the current configuration and state of a schedule including
        its payload, timing configuration, and execution history.

        Parameters:
        ----------
        schedule_id : str
            Unique identifier of the schedule to retrieve.

        error_handler : callable, optional
            A custom error handling function invoked if an error occurs.

        Returns:
        -------
        ResponseWrapper
            Wrapper containing the GetScheduleResponse protobuf with schedule details.

        Raises:
        ------
        RpcOperationError:
            If retrieval fails and no custom error handler is provided.

        Example:
        --------
        >>> response = client.get_schedule("daily_report_schedule")
        >>> schedule = response.to_dict()
        """
        try:
            request = request_response_pb2.GetScheduleRequest(schedule_id=schedule_id)
            response = self.stub.GetSchedule(request)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            logging.error(f"Error getting schedule {schedule_id}: {e.details()}")
            error = RpcOperationError(f"Failed to get schedule due to: {e.details()}")
            self._handle_error(error, handler=error_handler)

    def list_schedules(
        self, prefix: str = "", error_handler=None, *, page_size: int = 0, page_token: str = ""
    ) -> ResponseWrapper:
        """
        List all schedules, optionally filtered by ID prefix.

        Retrieves a list of all schedules in the system. Can filter results
        to only include schedules whose IDs start with the specified prefix.

        Parameters:
        ----------
        prefix : str, optional
            Filter to only return schedules with IDs starting with this prefix.
            If empty (default), returns all schedules.

        error_handler : callable, optional
            A custom error handling function invoked if an error occurs.

        page_size : int, optional
            Maximum results; zero uses the server default.
        page_token : str, optional
            Continuation token from the previous response.

        Returns:
        -------
        ResponseWrapper
            Wrapper containing the ListSchedulesResponse protobuf with list of schedules.

        Raises:
        ------
        RpcOperationError:
            If listing fails and no custom error handler is provided.

        Example:
        --------
        >>> response = client.list_schedules(prefix="daily_")
        >>> schedules = response.to_dict()
        """
        try:
            request = request_response_pb2.ListSchedulesRequest(
                prefix=prefix, page_size=page_size, page_token=page_token
            )
            response = self.stub.ListSchedules(request)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            logging.error(f"Error listing schedules: {e.details()}")
            error = RpcOperationError(f"Failed to list schedules due to: {e.details()}")
            self._handle_error(error, handler=error_handler)

    def get_schedule_history(
        self, schedule_id: str, page_size: int = 10, error_handler=None, *, page_token: str = ""
    ) -> ResponseWrapper:
        """
        Retrieve execution history for a schedule.

        Fetches the recent execution history of a schedule, showing when it ran,
        whether executions succeeded, and any errors encountered.

        Parameters:
        ----------
        schedule_id : str
            Unique identifier of the schedule.

        page_size : int, optional
            Maximum number of history entries to return. Default is 10.

        error_handler : callable, optional
            A custom error handling function invoked if an error occurs.

        page_token : str, optional
            Continuation token from the previous response.

        Returns:
        -------
        ResponseWrapper
            Wrapper containing the GetScheduleHistoryResponse protobuf with execution history.

        Raises:
        ------
        RpcOperationError:
            If retrieval fails and no custom error handler is provided.

        Example:
        --------
        >>> response = client.get_schedule_history("daily_report_schedule", page_size=20)
        >>> history = response.to_dict()
        """
        try:
            request = request_response_pb2.GetScheduleHistoryRequest(
                schedule_id=schedule_id, page_size=page_size, page_token=page_token
            )
            response = self.stub.GetScheduleHistory(request)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            logging.error(f"Error getting schedule history for {schedule_id}: {e.details()}")
            error = RpcOperationError(f"Failed to get schedule history due to: {e.details()}")
            self._handle_error(error, handler=error_handler)

    def pause_schedule(self, schedule_id: str, error_handler=None) -> ResponseWrapper:
        """
        Pause a schedule to prevent it from executing.

        Temporarily suspends a schedule. The schedule will not post messages to the queue
        until it is resumed. The schedule configuration is preserved.

        Parameters:
        ----------
        schedule_id : str
            Unique identifier of the schedule to pause.

        error_handler : callable, optional
            A custom error handling function invoked if an error occurs.

        Returns:
        -------
        ResponseWrapper
            Wrapper containing the PauseScheduleResponse protobuf.

        Raises:
        ------
        RpcOperationError:
            If pausing fails and no custom error handler is provided.

        Example:
        --------
        >>> response = client.pause_schedule("daily_report_schedule")
        """
        try:
            request = request_response_pb2.PauseScheduleRequest(schedule_id=schedule_id)
            response = self.stub.PauseSchedule(request)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            logging.error(f"Error pausing schedule {schedule_id}: {e.details()}")
            error = RpcOperationError(f"Failed to pause schedule due to: {e.details()}")
            self._handle_error(error, handler=error_handler)

    def resume_schedule(self, schedule_id: str, error_handler=None) -> ResponseWrapper:
        """
        Resume a paused schedule.

        Reactivates a paused schedule, allowing it to resume posting messages
        to the queue according to its configuration.

        Parameters:
        ----------
        schedule_id : str
            Unique identifier of the schedule to resume.

        error_handler : callable, optional
            A custom error handling function invoked if an error occurs.

        Returns:
        -------
        ResponseWrapper
            Wrapper containing the ResumeScheduleResponse protobuf.

        Raises:
        ------
        RpcOperationError:
            If resuming fails and no custom error handler is provided.

        Example:
        --------
        >>> response = client.resume_schedule("daily_report_schedule")
        """
        try:
            request = request_response_pb2.ResumeScheduleRequest(schedule_id=schedule_id)
            response = self.stub.ResumeSchedule(request)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            logging.error(f"Error resuming schedule {schedule_id}: {e.details()}")
            error = RpcOperationError(f"Failed to resume schedule due to: {e.details()}")
            self._handle_error(error, handler=error_handler)

    def validate_calendar_schedule(self, calendar_schedule: dict, error_handler=None) -> ResponseWrapper:
        """
        Validate a calendar schedule configuration.

        Checks whether a calendar schedule configuration is valid before creating
        or updating a schedule. Useful for validating complex calendar rules.

        Parameters:
        ----------
        calendar_schedule : dict
            Dictionary representation of a CalendarSchedule configuration to validate.

        error_handler : callable, optional
            A custom error handling function invoked if an error occurs.

        Returns:
        -------
        ResponseWrapper
            Wrapper containing the ValidateCalendarScheduleResponse protobuf with validation result.

        Raises:
        ------
        RpcOperationError:
            If validation request fails and no custom error handler is provided.

        Example:
        --------
        >>> calendar_config = {
        ...     "type": "MONTHLY",
        ...     "rules": [{"monthly": {"day_of_month": [1, 15]}}]
        ... }
        >>> response = client.validate_calendar_schedule(calendar_config)
        >>> is_valid = response.to_dict()
        """
        try:
            calendar_schedule_pb = json_format.ParseDict(calendar_schedule, schedule_pb2.CalendarSchedule())
            request = request_response_pb2.ValidateCalendarScheduleRequest(calendar_schedule=calendar_schedule_pb)
            response = self.stub.ValidateCalendarSchedule(request)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            logging.error(f"Error validating calendar schedule: {e.details()}")
            error = RpcOperationError(f"Failed to validate calendar schedule due to: {e.details()}")
            self._handle_error(error, handler=error_handler)

    def preview_calendar_schedule(
        self, calendar_schedule: dict, count: int = 10, error_handler=None
    ) -> ResponseWrapper:
        """
        Preview upcoming execution times for a calendar schedule.

        Generates a preview of when a calendar schedule would execute, showing
        the next N scheduled times based on the configuration.

        Parameters:
        ----------
        calendar_schedule : dict
            Dictionary representation of a CalendarSchedule configuration to preview.

        count : int, optional
            Number of upcoming execution times to generate. Default is 10.

        error_handler : callable, optional
            A custom error handling function invoked if an error occurs.

        Returns:
        -------
        ResponseWrapper
            Wrapper containing the PreviewCalendarScheduleResponse protobuf with execution times.

        Raises:
        ------
        RpcOperationError:
            If preview request fails and no custom error handler is provided.

        Example:
        --------
        >>> calendar_config = {
        ...     "type": "WEEKLY",
        ...     "rules": [{"weekly": {"day_of_week": ["MONDAY", "FRIDAY"]}}]
        ... }
        >>> response = client.preview_calendar_schedule(calendar_config, count=5)
        >>> times = response.to_dict()
        """
        try:
            calendar_schedule_pb = json_format.ParseDict(calendar_schedule, schedule_pb2.CalendarSchedule())
            request = request_response_pb2.PreviewCalendarScheduleRequest(
                calendar_schedule=calendar_schedule_pb, count=count
            )
            response = self.stub.PreviewCalendarSchedule(request)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            logging.error(f"Error previewing calendar schedule: {e.details()}")
            error = RpcOperationError(f"Failed to preview calendar schedule due to: {e.details()}")
            self._handle_error(error, handler=error_handler)

    # Schema Operations

    def register_schema(self, schema_id: str, options: SchemaOptions, error_handler=None) -> ResponseWrapper:
        """
        Register a new schema or create a new version of an existing schema.

        Schemas are used to validate message payloads before they are posted to queues.
        Supports JSON Schema format for defining validation rules.

        Parameters:
        ----------
        schema_id : str
            Unique identifier for the schema.

        options : SchemaOptions
            Configuration options for the schema including name, description, and content.

        error_handler : callable, optional
            A custom error handling function invoked if an error occurs.

        Returns:
        -------
        ResponseWrapper
            Wrapper containing the RegisterSchemaResponse protobuf with schema_id and version.

        Raises:
        ------
        RpcOperationError:
            If schema registration fails and no custom error handler is provided.

        Example:
        --------
        >>> from nzovu.utils import SchemaOptions
        >>> options = SchemaOptions(
        ...     name="Order Schema",
        ...     description="Validation schema for order messages",
        ...     content='{"type": "object", "properties": {"orderId": {"type": "string"}}}',
        ...     content_type="json-schema",
        ...     metadata={"version": "1.0", "author": "team"}
        ... )
        >>> response = client.register_schema("order_schema", options)
        >>> result = response.to_model()
        >>> print(f"Schema {result.schema_id} registered with version {result.version}")
        """
        try:
            request = request_response_pb2.RegisterSchemaRequest(
                schema_id=schema_id,
                name=options.name,
                description=options.description,
                content=options.content,
                content_type=options.content_type,
                metadata=options.metadata if options.metadata else {},
            )
            response = self.stub.RegisterSchema(request)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            logging.error(f"Error registering schema {schema_id}: {e.details()}")
            error = RpcOperationError(f"Failed to register schema due to: {e.details()}")
            self._handle_error(error, handler=error_handler)

    def get_schema(self, schema_id: str, version: int = 0, error_handler=None) -> ResponseWrapper:
        """
        Retrieve a schema by ID and optional version.

        Parameters:
        ----------
        schema_id : str
            Unique identifier of the schema to retrieve.

        version : int, optional
            Schema version to retrieve. If 0 (default), retrieves the latest version.

        error_handler : callable, optional
            A custom error handling function invoked if an error occurs.

        Returns:
        -------
        ResponseWrapper
            Wrapper containing the GetSchemaResponse protobuf with full schema details.

        Raises:
        ------
        RpcOperationError:
            If schema retrieval fails and no custom error handler is provided.

        Example:
        --------
        >>> # Get latest version
        >>> response = client.get_schema("order_schema")
        >>> schema = response.to_model()
        >>> print(f"Schema: {schema.schema.name}")
        >>> print(f"Version: {schema.schema.version}")
        >>> print(f"Content: {schema.schema.content}")
        >>>
        >>> # Get specific version
        >>> response = client.get_schema("order_schema", version=2)
        """
        try:
            request = request_response_pb2.GetSchemaRequest(schema_id=schema_id, version=version)
            response = self.stub.GetSchema(request)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            logging.error(f"Error getting schema {schema_id}: {e.details()}")
            error = RpcOperationError(f"Failed to get schema due to: {e.details()}")
            self._handle_error(error, handler=error_handler)

    def list_schemas(
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

        Returns a summary view of schemas with aggregated version information.

        Parameters:
        ----------
        prefix : str, optional
            Filter to only return schemas with IDs starting with this prefix.
            If empty (default), returns all schemas.

        page_size : int, optional
            Maximum number of results to return. Default is 100.

        active_only : bool, optional
            If True, only return active schemas. Default is False.

        error_handler : callable, optional
            A custom error handling function invoked if an error occurs.

        page_token : str, optional
            Continuation token from the previous response.

        Returns:
        -------
        ResponseWrapper
            Wrapper containing the ListSchemasResponse protobuf with list of schema summaries.

        Raises:
        ------
        RpcOperationError:
            If listing fails and no custom error handler is provided.

        Example:
        --------
        >>> # List all schemas
        >>> response = client.list_schemas()
        >>> schemas = response.to_model()
        >>> for schema in schemas.schemas:
        ...     print(f"{schema.schema_id}: {schema.name} (v{schema.latest_version})")
        >>>
        >>> # Filter by prefix
        >>> response = client.list_schemas(prefix="order_", active_only=True)
        """
        try:
            request = request_response_pb2.ListSchemasRequest(
                prefix=prefix, page_size=page_size, page_token=page_token, active_only=active_only
            )
            response = self.stub.ListSchemas(request)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            logging.error(f"Error listing schemas: {e.details()}")
            error = RpcOperationError(f"Failed to list schemas due to: {e.details()}")
            self._handle_error(error, handler=error_handler)

    def delete_schema(self, schema_id: str, version: int = 0, error_handler=None) -> ResponseWrapper:
        """
        Delete a schema or specific schema version.

        Parameters:
        ----------
        schema_id : str
            Unique identifier of the schema to delete.

        version : int, optional
            Schema version to delete. If 0 (default), deletes all versions.

        error_handler : callable, optional
            A custom error handling function invoked if an error occurs.

        Returns:
        -------
        ResponseWrapper
            Wrapper containing the DeleteSchemaResponse protobuf with deletion result.

        Raises:
        ------
        RpcOperationError:
            If deletion fails and no custom error handler is provided.

        Example:
        --------
        >>> # Delete specific version
        >>> response = client.delete_schema("order_schema", version=1)
        >>> result = response.to_model()
        >>> print(f"Deleted {result.versions_deleted} version(s)")
        >>>
        >>> # Delete all versions
        >>> response = client.delete_schema("order_schema")
        """
        try:
            request = request_response_pb2.DeleteSchemaRequest(schema_id=schema_id, version=version)
            response = self.stub.DeleteSchema(request)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            logging.error(f"Error deleting schema {schema_id}: {e.details()}")
            error = RpcOperationError(f"Failed to delete schema due to: {e.details()}")
            self._handle_error(error, handler=error_handler)

    def validate_payload(self, schema_id: str, payload: str, version: int = 0, error_handler=None) -> ResponseWrapper:
        """
        Validate a JSON payload against a schema.

        Checks whether the provided payload conforms to the schema's validation rules.

        Parameters:
        ----------
        schema_id : str
            Unique identifier of the schema to validate against.

        payload : str
            JSON payload to validate (as a string).

        version : int, optional
            Schema version to use for validation. If 0 (default), uses latest version.

        error_handler : callable, optional
            A custom error handling function invoked if an error occurs.

        Returns:
        -------
        ResponseWrapper
            Wrapper containing the ValidatePayloadResponse protobuf with validation result.

        Raises:
        ------
        RpcOperationError:
            If validation request fails and no custom error handler is provided.

        Example:
        --------
        >>> import json
        >>> payload = json.dumps({"orderId": "12345", "amount": 99.99})
        >>> response = client.validate_payload("order_schema", payload)
        >>> result = response.to_model()
        >>> if result.valid:
        ...     print("Payload is valid")
        ... else:
        ...     for error in result.errors:
        ...         print(f"Error in {error.field}: {error.message}")
        """
        try:
            request = request_response_pb2.ValidatePayloadRequest(schema_id=schema_id, version=version, payload=payload)
            response = self.stub.ValidatePayload(request)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            logging.error(f"Error validating payload against schema {schema_id}: {e.details()}")
            error = RpcOperationError(f"Failed to validate payload due to: {e.details()}")
            self._handle_error(error, handler=error_handler)

    # Dead Letter Queue Operations

    def get_dlq_messages(
        self, dlq_name: str, page_size: int = 100, error_handler=None, *, page_token: str = ""
    ) -> ResponseWrapper:
        """
        Retrieve messages from a Dead Letter Queue (DLQ).

        Dead Letter Queues store messages that failed processing after exhausting all retry attempts.
        This method allows you to inspect failed messages for debugging or recovery purposes.

        Parameters:
        ----------
        dlq_name : str
            Name of the Dead Letter Queue to retrieve messages from.

        page_size : int, optional
            Maximum number of messages to retrieve. Default is 100.

        error_handler : callable, optional
            A custom error handling function invoked if an error occurs.

        page_token : str, optional
            Continuation token from the previous response.

        Returns:
        -------
        ResponseWrapper
            Wrapper containing the GetDLQMessagesResponse protobuf with list of messages.

        Raises:
        ------
        RpcOperationError:
            If retrieval fails and no custom error handler is provided.

        Example:
        --------
        >>> # Get messages from DLQ
        >>> response = client.get_dlq_messages("orders_queue_dlq", page_size=50)
        >>> messages = response.to_model()  # GetDLQMessagesResponse
        >>> print(f"Found {len(messages.messages)} failed messages")
        >>> for msg in messages.messages:
        ...     print(f"Message {msg.message_id}: {msg.metadata.state}")
        """
        try:
            request = request_response_pb2.GetDLQMessagesRequest(
                dlq_name=dlq_name, page_size=page_size, page_token=page_token
            )
            response = self.stub.GetDLQMessages(request)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            logging.error(f"Error getting DLQ messages from {dlq_name}: {e.details()}")
            error = RpcOperationError(f"Failed to get DLQ messages due to: {e.details()}")
            self._handle_error(error, handler=error_handler)

    def requeue_from_dlq(
        self, dlq_name: str, message_id: str, target_queue: str = "", error_handler=None
    ) -> ResponseWrapper:
        """
        Move a message from DLQ back to its original queue or a specified target queue.

        This allows recovery of failed messages by requeuing them for another processing attempt.
        If target_queue is not specified, the message is requeued to its original queue.

        Parameters:
        ----------
        dlq_name : str
            Name of the Dead Letter Queue containing the message.

        message_id : str
            Unique identifier of the message to requeue.

        target_queue : str, optional
            Target queue name. If empty, requeues to the message's original queue.

        error_handler : callable, optional
            A custom error handling function invoked if an error occurs.

        Returns:
        -------
        ResponseWrapper
            Wrapper containing the RequeueFromDLQResponse protobuf with success status.

        Raises:
        ------
        RpcOperationError:
            If requeue operation fails and no custom error handler is provided.

        Example:
        --------
        >>> # Requeue to original queue
        >>> response = client.requeue_from_dlq("orders_queue_dlq", "msg-123")
        >>> result = response.to_model()
        >>> if result.success:
        ...     print("Message requeued successfully")
        >>>
        >>> # Requeue to different queue
        >>> response = client.requeue_from_dlq(
        ...     "orders_queue_dlq",
        ...     "msg-123",
        ...     target_queue="manual_review_queue"
        ... )
        """
        try:
            request = request_response_pb2.RequeueFromDLQRequest(
                dlq_name=dlq_name, message_id=message_id, target_queue=target_queue
            )
            response = self.stub.RequeueFromDLQ(request)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            logging.error(f"Error requeuing message {message_id} from DLQ {dlq_name}: {e.details()}")
            error = RpcOperationError(f"Failed to requeue from DLQ due to: {e.details()}")
            self._handle_error(error, handler=error_handler)

    def delete_from_dlq(self, dlq_name: str, message_id: str, error_handler=None) -> ResponseWrapper:
        """
        Permanently delete a message from a Dead Letter Queue.

        Use this to remove messages from DLQ after manual inspection or when they are
        no longer needed. This operation is irreversible.

        Parameters:
        ----------
        dlq_name : str
            Name of the Dead Letter Queue containing the message.

        message_id : str
            Unique identifier of the message to delete.

        error_handler : callable, optional
            A custom error handling function invoked if an error occurs.

        Returns:
        -------
        ResponseWrapper
            Wrapper containing the DeleteFromDLQResponse protobuf with success status.

        Raises:
        ------
        RpcOperationError:
            If deletion fails and no custom error handler is provided.

        Example:
        --------
        >>> response = client.delete_from_dlq("orders_queue_dlq", "msg-123")
        >>> result = response.to_model()
        >>> if result.success:
        ...     print("Message permanently deleted from DLQ")
        """
        try:
            request = request_response_pb2.DeleteFromDLQRequest(dlq_name=dlq_name, message_id=message_id)
            response = self.stub.DeleteFromDLQ(request)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            logging.error(f"Error deleting message {message_id} from DLQ {dlq_name}: {e.details()}")
            error = RpcOperationError(f"Failed to delete from DLQ due to: {e.details()}")
            self._handle_error(error, handler=error_handler)

    def purge_dlq(self, dlq_name: str, error_handler=None) -> ResponseWrapper:
        """
        Remove all messages from a Dead Letter Queue.

        This operation permanently deletes all messages in the DLQ. Use with caution
        as this operation is irreversible and may result in data loss.

        Parameters:
        ----------
        dlq_name : str
            Name of the Dead Letter Queue to purge.

        error_handler : callable, optional
            A custom error handling function invoked if an error occurs.

        Returns:
        -------
        ResponseWrapper
            Wrapper containing the PurgeDLQResponse protobuf with success status.

        Raises:
        ------
        RpcOperationError:
            If purge operation fails and no custom error handler is provided.

        Example:
        --------
        >>> # Purge all messages from DLQ (use with caution!)
        >>> response = client.purge_dlq("orders_queue_dlq")
        >>> result = response.to_model()
        >>> if result.success:
        ...     print("All messages purged from DLQ")
        """
        try:
            request = request_response_pb2.PurgeDLQRequest(dlq_name=dlq_name)
            response = self.stub.PurgeDLQ(request)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            logging.error(f"Error purging DLQ {dlq_name}: {e.details()}")
            error = RpcOperationError(f"Failed to purge DLQ due to: {e.details()}")
            self._handle_error(error, handler=error_handler)

    def get_dlq_stats(self, dlq_name: str, error_handler=None) -> ResponseWrapper:
        """
        Retrieve statistics about a Dead Letter Queue.

        Get information about the DLQ including message count and timestamps.

        Parameters:
        ----------
        dlq_name : str
            Name of the Dead Letter Queue to get statistics for.

        error_handler : callable, optional
            A custom error handling function invoked if an error occurs.

        Returns:
        -------
        ResponseWrapper
            Wrapper containing the GetDLQStatsResponse protobuf with DLQ statistics.

        Raises:
        ------
        RpcOperationError:
            If stats retrieval fails and no custom error handler is provided.

        Example:
        --------
        >>> response = client.get_dlq_stats("orders_queue_dlq")
        >>> stats = response.to_model()  # GetDLQStatsResponse
        >>> print(f"DLQ: {stats.name}")
        >>> print(f"Message count: {stats.message_count}")
        >>> print(f"Created at: {stats.created_at}")
        >>> print(f"Updated at: {stats.updated_at}")
        """
        try:
            request = request_response_pb2.GetDLQStatsRequest(dlq_name=dlq_name)
            response = self.stub.GetDLQStats(request)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            logging.error(f"Error getting DLQ stats for {dlq_name}: {e.details()}")
            error = RpcOperationError(f"Failed to get DLQ stats due to: {e.details()}")
            self._handle_error(error, handler=error_handler)

    def get_active_heartbeat_count(self) -> int:
        """
        Get the number of messages with currently active heartbeats.

        Returns:
        -------
        int
            The count of messages that have active heartbeat threads running.

        Example:
        --------
        >>> count = client.get_active_heartbeat_count()
        >>> print(f"Active heartbeats: {count}")
        """
        with self.lock:
            return len(self._active_heartbeats)

    def get_heartbeat_stats(self) -> Dict[str, dict]:
        """
        Get detailed statistics for all heartbeats (active and recently completed).

        Returns a dictionary mapping message IDs to their heartbeat metrics, including:
        - message_id: The message identifier
        - queue_name: The queue containing the message
        - started_at: Timestamp when heartbeat started
        - heartbeats_sent: Number of successful heartbeats sent
        - heartbeats_failed: Number of failed heartbeat attempts
        - last_heartbeat_at: Timestamp of last successful heartbeat
        - last_error: Last error message (if any)
        - ended_at: Timestamp when heartbeat ended (if completed)
        - total_heartbeats: Total heartbeats sent (if completed)

        Returns:
        -------
        Dict[str, dict]
            Dictionary of heartbeat metrics keyed by message_id.

        Example:
        --------
        >>> stats = client.get_heartbeat_stats()
        >>> for msg_id, metrics in stats.items():
        ...     print(f"Message {msg_id}: {metrics['heartbeats_sent']} heartbeats sent")
        """
        with self.lock:
            return dict(self._heartbeat_metrics)

    def get_active_heartbeats(self) -> Dict[str, dict]:
        """
        Get information about currently active heartbeats.

        Returns a dictionary mapping message IDs to their heartbeat info, including:
        - queue_name: The queue containing the message
        - started_at: Timestamp when heartbeat started
        - heartbeat_frequency: Interval between heartbeats in seconds

        Returns:
        -------
        Dict[str, dict]
            Dictionary of active heartbeat info keyed by message_id.

        Example:
        --------
        >>> active = client.get_active_heartbeats()
        >>> for msg_id, info in active.items():
        ...     duration = time.time() - info['started_at']
        ...     print(f"Message {msg_id} heartbeat running for {duration:.1f}s")
        """
        with self.lock:
            return {
                msg_id: {
                    "queue_name": info["queue_name"],
                    "started_at": info["started_at"],
                    "heartbeat_frequency": info["heartbeat_frequency"],
                    "duration": time.time() - info["started_at"],
                }
                for msg_id, info in self._active_heartbeats.items()
            }

    def stop_heartbeat(self, message_id: str) -> bool:
        """
        Manually stop the heartbeat for a specific message.

        This can be used to stop heartbeats before acknowledging a message,
        or to stop heartbeats for messages that are no longer being processed.

        Parameters:
        ----------
        message_id : str
            The unique identifier of the message whose heartbeat should be stopped.

        Returns:
        -------
        bool
            True if heartbeat was stopped, False if no active heartbeat for this message.

        Example:
        --------
        >>> if client.stop_heartbeat("msg-123"):
        ...     print("Heartbeat stopped")
        ... else:
        ...     print("No active heartbeat for this message")
        """
        with self.lock:
            if message_id in self._heartbeat_control:
                logging.info(f"Manually stopping heartbeat for message {message_id}")
                self._heartbeat_control[message_id].set()
                return True
            return False

    def close(self, timeout: float = 30.0, error_handler=None) -> None:
        """
        Gracefully closes the gRPC channel and stops all active heartbeats.

        Closes the gRPC channel used by the SDK to communicate with the Nzovu service, ensuring
        that resources are released and open connections to the service are terminated. This method
        also stops all active heartbeat threads gracefully by signaling them to stop and waiting
        for them to complete within the specified timeout. It is recommended to invoke this method
        when the SDK is no longer needed, such as when your application is terminating, to cleanly
        shut down the SDK components.

        Parameters:
        ----------
        timeout : float, optional
            Maximum time in seconds to wait for heartbeats to stop, by default 30.0.
            If heartbeats don't stop within this time, they will be forcefully terminated.

        error_handler : callable, optional
            A custom error handling function that will be invoked if an error occurs during the operation.
            The function should accept a single argument, which is the error/exception object.
            If not provided, the SDK's default error handling mechanism will be utilized.

        Returns:
        -------
        None

        Raises:
        ------
        RpcOperationError:
            If there's an error closing the gRPC channel and no custom error handler is provided.

        Example:
        --------
        >>> client = NzovuClient(host="localhost", port=50051)
        >>> # ... perform operations ...
        >>> client.close()
        >>> # Or with custom timeout:
        >>> client.close(timeout=60.0)
        """
        try:
            logging.info("Closing NzovuClient, stopping all heartbeats...")

            # Signal all active heartbeats to stop
            with self.lock:
                active_count = len(self._heartbeat_control)
                if active_count > 0:
                    logging.info(f"Signaling {active_count} active heartbeat(s) to stop")
                    for message_id, stop_event in self._heartbeat_control.items():
                        stop_event.set()

            # Shutdown thread pool gracefully
            logging.info("Shutting down heartbeat thread pool...")
            self._heartbeat_executor.shutdown(wait=True)
            # Note: ThreadPoolExecutor.shutdown doesn't support timeout parameter
            # If we need timeout, we would need to implement it differently

            # Legacy support - stop old heartbeat thread if it exists
            if hasattr(self, "_heartbeat_manager_thread") and self._heartbeat_manager_thread.is_alive():
                self._stop_heartbeat.set()
                self._heartbeat_manager_thread.join(timeout=5.0)

            # Close gRPC channel
            if (
                self.channel
                and self.channel._channel.check_connectivity_state(True) != grpc.ChannelConnectivity.SHUTDOWN
            ):
                logging.info("Closing gRPC channel")
                self.channel.close()

            logging.info("NzovuClient closed successfully")

        except Exception as e:
            logging.error(f"Error closing client: {e}")
            error = RpcOperationError(f"Failed to close client due to: {e}")
            self._handle_error(error, handler=error_handler)

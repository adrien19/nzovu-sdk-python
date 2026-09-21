import logging
from typing import Callable, Dict, Optional

import grpc
from google.protobuf import json_format
from google.protobuf.duration_pb2 import Duration

from .api.queue.v1 import queue_pb2
from .api.queueservice.v1 import request_response_pb2, service_pb2_grpc
from .api.schedule.v1 import schedule_pb2
from .exceptions import RpcOperationError
from .heartbeat import OWNERSHIP_LOST, SyncHeartbeats
from .ownership import Claim
from .transport import RpcOptions, create_channel
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
    >>> client = NzovuClient(host="localhost", port=9000, use_tls=True, tls_config=my_tls_config)

    # Create a new queue
    >>> client.create_queue(CreateQueueParams(name="my_new_queue"))

    # Post a message to a queue
    >>> msg_params = PostMessageParams(message_id="12345", data={"key": "value"})
    >>> client.post_message(msg_params)
    """

    def __init__(
        self,
        host: str,
        port: int = 9000,
        use_tls=True,
        tls_config: Optional[TlsConfig] = None,
        worker_id: str = "",
        heartbeat_max_duration: int = 300,  # 5 minutes
        heartbeat_max_count: int = 1000000000,  # Large number to effectively disable count limit
        heartbeat_thread_pool_size: int = 20,
        heartbeat_error_callback: Optional[Callable] = None,
        *,
        api_key: Optional[str] = None,
        rpc_timeout: Optional[float] = None,
        heartbeat_interval: float = 1.0,
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
            and client key. Optional: system trust roots are used when omitted; client certificates enable mTLS.
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
            If authentication, deadline or TLS credential files are invalid.
        """
        self.host = host
        self.port = port
        self._use_tls = use_tls
        self._tls_config = tls_config
        self._rpc_options = RpcOptions(api_key=api_key, timeout=rpc_timeout)
        self._worker_id = worker_id
        self._heartbeat_max_duration = heartbeat_max_duration
        self._heartbeat_max_count = heartbeat_max_count
        self._heartbeat_error_callback = heartbeat_error_callback

        self._heartbeats = SyncHeartbeats(
            heartbeat_thread_pool_size,
            heartbeat_max_duration,
            heartbeat_max_count,
            heartbeat_interval,
            rpc_timeout,
            heartbeat_error_callback,
        )

        self.channel = create_channel(f"{self.host}:{self.port}", self._use_tls, self._tls_config, self._rpc_options)
        self.stub = service_pb2_grpc.QueueServiceStub(self.channel)

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
            error = RpcOperationError(f"Failed to create queue due to: {e.details()}", cause=e)
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
            error = RpcOperationError(f"Failed to delete queue due to: {e.details()}", cause=e)
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
        validate_page(page_size, page_token)
        try:
            request = request_response_pb2.ListQueuesRequest(prefix=prefix, page_size=page_size, page_token=page_token)
            response = self.stub.ListQueues(request)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            logging.error(f"Error listing queues: {e.details()}")
            error = RpcOperationError(f"Failed to list queues due to: {e.details()}", cause=e)
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
            error = RpcOperationError(f"Failed to post message due to: {e.details()}", cause=e)
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
            request = build_bulk_request(queue_name, messages, transaction_mode)
            response = self.stub.PostMessagesBulk(request)
            return ResponseWrapper(response_protobuf=response)

        except grpc.RpcError as e:
            logging.error(f"Error posting messages in bulk: {e.details()}")
            error = RpcOperationError(f"Failed to post messages in bulk due to: {e.details()}", cause=e)
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
        enable_heartbeat: bool = False,
        error_handler=None,
        *,
        worker_id: Optional[str] = None,
        attempt_id: Optional[str] = None,
    ) -> ResponseWrapper:
        """Claim a message; response.claim retains ownership even without automatic heartbeats.

        Automatic heartbeats reserve capacity before claiming. Saturation raises
        HeartbeatCapacityError without fetching an unprotected message.
        """
        self._heartbeats.ensure_open()
        reserved = False
        try:
            request = request_response_pb2.GetNextMessageRequest(
                queue_name=queue_name,
                lease_duration=string_to_duration(lease_duration),
                exclusivity_key=exclusivity_key,
                worker_id=self._worker_id if worker_id is None else worker_id,
                attempt_id=attempt_id,
            )
            if enable_heartbeat:
                self._heartbeats.reserve()
                reserved = True
            response = self.stub.GetNextMessage(request)
            wrapped = ResponseWrapper(response_protobuf=response)
            wrapped.claim = Claim.from_response(queue_name, response)
            if enable_heartbeat and wrapped.claim is not None:
                self._heartbeats.start(wrapped.claim, self.stub)
                reserved = False
            return wrapped
        except grpc.RpcError as e:
            self._handle_error(RpcOperationError(f"Failed to get next message: {e.details()}", cause=e), error_handler)
        finally:
            if reserved:
                self._heartbeats.release()

    def acknowledge_message(self, params: AcknowledgeMessageParams, error_handler=None) -> ResponseWrapper:
        """Acknowledge an explicit claim; stop its heartbeat only after confirmed success."""
        claim = Claim(params.queue_name, params.message_id, params.worker_id, params.attempt_id)
        try:
            request = request_response_pb2.AcknowledgeMessageRequest(
                **claim.to_dict(),
                state=params.state.name if isinstance(params.state, MessageState) else params.state,
            )
            response = self.stub.AcknowledgeMessage(request)
            if response.success:
                self._heartbeats.stop(claim)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            if e.code() in OWNERSHIP_LOST:
                self._heartbeats.stop(claim)
            self._handle_error(
                RpcOperationError(f"Failed to acknowledge message: {e.details()}", cause=e), error_handler
            )

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
            error = RpcOperationError(f"Failed to cancel message due to: {e.details()}", cause=e)
            self._handle_error(error, handler=error_handler)

    def renew_message_lease(
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
        claim = Claim(queue_name, message_id, worker_id or "", attempt_id or "")
        try:
            pb_release_duration: Duration = string_to_duration(new_lease_duration)

            request = request_response_pb2.RenewMessageLeaseRequest(
                queue_name=queue_name,
                message_id=message_id,
                lease_duration=pb_release_duration,
                worker_id=worker_id,
                attempt_id=attempt_id,
            )
            response = self.stub.RenewMessageLease(request)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            # FAILED_PRECONDITION can mean an extension limit, while ownership remains valid.
            if e.code() in {grpc.StatusCode.NOT_FOUND, grpc.StatusCode.PERMISSION_DENIED}:
                self._heartbeats.stop(claim)
            logging.error(f"Error renewing message lease: {e.details()}")
            error = RpcOperationError(f"Failed to renew message lease due to: {e.details()}", cause=e)
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
        >>> from nzovu.utils import PeekQueueMessagesParams
        >>> params = PeekQueueMessagesParams(queue_name="my_queue", page_size=10, priority=5)
        >>> client.peek_queue_messages(params)

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
            response = self.stub.PeekQueueMessages(request)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            logging.error(f"Error peeking queue messages: {e.details()}")
            error = RpcOperationError(f"Failed to peek queue due to: {e.details()}", cause=e)
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
            error = RpcOperationError(f"Failed to get queue state due to: {e.details()}", cause=e)
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
        claim = Claim(queue_name, message_id, worker_id or "", attempt_id or "")
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
            if e.code() in OWNERSHIP_LOST:
                self._heartbeats.stop(claim)
            logging.error(f"Error sending heartbeat for message {message_id}: {e.details()}")
            error = RpcOperationError(f"Failed to send heartbeat due to: {e.details()}", cause=e)
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
            request = build_schedule_request(schedule_id, options)
            response = self.stub.CreateSchedule(request)
            return ResponseWrapper(response_protobuf=response)

        except grpc.RpcError as e:
            logging.error(f"Error creating schedule {schedule_id}: {e.details()}")
            error = RpcOperationError(f"Failed to create schedule due to: {e.details()}", cause=e)
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
            error = RpcOperationError(f"Failed to delete schedule due to: {e.details()}", cause=e)
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
            error = RpcOperationError(f"Failed to get schedule due to: {e.details()}", cause=e)
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
        validate_page(page_size, page_token)
        try:
            request = request_response_pb2.ListSchedulesRequest(
                prefix=prefix, page_size=page_size, page_token=page_token
            )
            response = self.stub.ListSchedules(request)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            logging.error(f"Error listing schedules: {e.details()}")
            error = RpcOperationError(f"Failed to list schedules due to: {e.details()}", cause=e)
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
        validate_page(page_size, page_token)
        try:
            request = request_response_pb2.GetScheduleHistoryRequest(
                schedule_id=schedule_id, page_size=page_size, page_token=page_token
            )
            response = self.stub.GetScheduleHistory(request)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            logging.error(f"Error getting schedule history for {schedule_id}: {e.details()}")
            error = RpcOperationError(f"Failed to get schedule history due to: {e.details()}", cause=e)
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
            error = RpcOperationError(f"Failed to pause schedule due to: {e.details()}", cause=e)
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
            error = RpcOperationError(f"Failed to resume schedule due to: {e.details()}", cause=e)
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
            error = RpcOperationError(f"Failed to validate calendar schedule due to: {e.details()}", cause=e)
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
        validate_integer(count, "count", 0, 100)
        try:
            calendar_schedule_pb = json_format.ParseDict(calendar_schedule, schedule_pb2.CalendarSchedule())
            request = request_response_pb2.PreviewCalendarScheduleRequest(
                calendar_schedule=calendar_schedule_pb, count=count
            )
            response = self.stub.PreviewCalendarSchedule(request)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            logging.error(f"Error previewing calendar schedule: {e.details()}")
            error = RpcOperationError(f"Failed to preview calendar schedule due to: {e.details()}", cause=e)
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
            response = self.stub.RegisterSchema(request)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            logging.error(f"Error registering schema {schema_id}: {e.details()}")
            error = RpcOperationError(f"Failed to register schema due to: {e.details()}", cause=e)
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
        validate_integer(version, "version", 0, 2**31 - 1)
        try:
            request = request_response_pb2.GetSchemaRequest(schema_id=schema_id, version=version)
            response = self.stub.GetSchema(request)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            logging.error(f"Error getting schema {schema_id}: {e.details()}")
            error = RpcOperationError(f"Failed to get schema due to: {e.details()}", cause=e)
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
        validate_page(page_size, page_token)
        try:
            request = request_response_pb2.ListSchemasRequest(
                prefix=prefix, page_size=page_size, page_token=page_token, active_only=active_only
            )
            response = self.stub.ListSchemas(request)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            logging.error(f"Error listing schemas: {e.details()}")
            error = RpcOperationError(f"Failed to list schemas due to: {e.details()}", cause=e)
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
        validate_integer(version, "version", 0, 2**31 - 1)
        try:
            request = request_response_pb2.DeleteSchemaRequest(schema_id=schema_id, version=version)
            response = self.stub.DeleteSchema(request)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            logging.error(f"Error deleting schema {schema_id}: {e.details()}")
            error = RpcOperationError(f"Failed to delete schema due to: {e.details()}", cause=e)
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
        validate_integer(version, "version", 0, 2**31 - 1)
        try:
            request = request_response_pb2.ValidatePayloadRequest(schema_id=schema_id, version=version, payload=payload)
            response = self.stub.ValidatePayload(request)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            logging.error(f"Error validating payload against schema {schema_id}: {e.details()}")
            error = RpcOperationError(f"Failed to validate payload due to: {e.details()}", cause=e)
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
        validate_page(page_size, page_token)
        try:
            request = request_response_pb2.GetDLQMessagesRequest(
                dlq_name=dlq_name, page_size=page_size, page_token=page_token
            )
            response = self.stub.GetDLQMessages(request)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            logging.error(f"Error getting DLQ messages from {dlq_name}: {e.details()}")
            error = RpcOperationError(f"Failed to get DLQ messages due to: {e.details()}", cause=e)
            self._handle_error(error, handler=error_handler)

    def requeue_from_dlq(
        self, dlq_name: str, message_id: str, target_queue: str, error_handler=None
    ) -> ResponseWrapper:
        """
        Move a message from DLQ to an explicit existing target queue.

        This allows recovery of failed messages by requeuing them for another processing attempt.
        The server requires an explicit existing target queue.

        Parameters:
        ----------
        dlq_name : str
            Name of the Dead Letter Queue containing the message.

        message_id : str
            Unique identifier of the message to requeue.

        target_queue : str
            Required existing target queue name.

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
        >>> response = client.requeue_from_dlq("orders_queue_dlq", "msg-123", "orders_queue")
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
        require_name(target_queue, "target_queue")
        try:
            request = request_response_pb2.RequeueFromDLQRequest(
                dlq_name=dlq_name, message_id=message_id, target_queue=target_queue
            )
            response = self.stub.RequeueFromDLQ(request)
            return ResponseWrapper(response_protobuf=response)
        except grpc.RpcError as e:
            logging.error(f"Error requeuing message {message_id} from DLQ {dlq_name}: {e.details()}")
            error = RpcOperationError(f"Failed to requeue from DLQ due to: {e.details()}", cause=e)
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
            error = RpcOperationError(f"Failed to delete from DLQ due to: {e.details()}", cause=e)
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
            error = RpcOperationError(f"Failed to purge DLQ due to: {e.details()}", cause=e)
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
            error = RpcOperationError(f"Failed to get DLQ stats due to: {e.details()}", cause=e)
            self._handle_error(error, handler=error_handler)

    def iter_pages(self, method: str, *args, max_pages=None, **kwargs):
        """Yield one response per request; stop on exhaustion or max_pages."""
        validate_page_iterator(method, max_pages)
        params = (args[0] if args else kwargs.get("params")) if method == "peek_queue_messages" else None
        token = params.page_token if params is not None else kwargs.get("page_token", "")
        seen = {token}
        count = 0
        while max_pages is None or count < max_pages:
            call_args, call_kwargs = page_call_arguments(method, args, kwargs, token)
            response = getattr(self, method)(*call_args, **call_kwargs)
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
        """Return a snapshot of active claim-scoped heartbeat workers."""
        return len(self._heartbeats.snapshot())

    def get_heartbeat_stats(self) -> Dict[Claim, dict]:
        """Return a snapshot of active claim-scoped heartbeat workers."""
        return self._heartbeats.snapshot()

    def get_active_heartbeats(self) -> Dict[Claim, dict]:
        """Return a snapshot of active claim-scoped heartbeat workers."""
        return self._heartbeats.snapshot()

    def stop_heartbeat(self, claim: Claim) -> bool:
        """Stop this exact claim; never infer an attempt from a message ID."""
        if not isinstance(claim, Claim):
            raise TypeError("stop_heartbeat requires a Claim")
        return self._heartbeats.stop(claim)

    def close(self, timeout: float = 30.0, error_handler=None):
        """Stop admission, cancel channel RPCs, and join all managed heartbeat work."""
        futures = self._heartbeats.begin_close()
        try:
            try:
                if self.channel is not None:
                    self.channel.close()
            finally:
                self._heartbeats.join(futures, timeout)
        except Exception as e:
            self._handle_error(RpcOperationError(f"Failed to close client: {e}"), error_handler)

class NzovuError(Exception):
    """Base exception for Nzovu SDK."""

    pass


class ConnectionError(NzovuError):
    """Raised when there's a connection issue."""

    pass


class InitializationError(NzovuError):
    """Raised when there's an error during the SDK's initialization."""

    pass


class RpcOperationError(NzovuError):
    """Raised when there's an error performing an RPC operation."""

    pass

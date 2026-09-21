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
    """RPC failure retaining its original gRPC status and metadata."""

    def __init__(self, message, *, cause=None):
        super().__init__(message)
        self.__cause__ = cause
        self.rpc_error = cause

    def code(self):
        return self.rpc_error.code() if self.rpc_error is not None else None

    def details(self):
        return self.rpc_error.details() if self.rpc_error is not None else str(self)

    def trailing_metadata(self):
        return self.rpc_error.trailing_metadata() if self.rpc_error is not None else None

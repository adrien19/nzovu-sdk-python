"""Immutable ownership returned by a successful message claim."""

from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class Claim:
    queue_name: str
    message_id: str
    worker_id: str
    attempt_id: str

    def __post_init__(self):
        for name, value in asdict(self).items():
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} is required for message ownership")

    @classmethod
    def from_response(cls, queue_name, response):
        if not response.HasField("message") or not response.message.message_id:
            return None
        return cls(queue_name, response.message.message_id, response.worker_id, response.attempt_id)

    def to_dict(self):
        return asdict(self)

    def acknowledge(self, state):
        from .utils import AcknowledgeMessageParams

        return AcknowledgeMessageParams(state=state, **self.to_dict())

from abc import ABC, abstractmethod
from typing import Optional

from charybdisk.messages import FileMessage


class SendResult:
    def __init__(
        self,
        success: bool,
        error: Optional[Exception] = None,
        assumed_success: bool = False,
        transient: bool = False,
    ):
        self.success = success
        self.error = error
        # True when success was assumed after a ReadTimeout (delivery unconfirmed)
        self.assumed_success = assumed_success
        # True when failure is transient (5xx, connection error) and the file should be retried
        self.transient = transient


class Transport(ABC):
    @abstractmethod
    def max_transfer_size(self) -> Optional[int]:
        """Return max allowed bytes for this transport, or None if unlimited."""

    @abstractmethod
    def send(self, destination: str, message: FileMessage) -> SendResult:
        """Send a file message to the destination (topic/url/etc.)."""

    @abstractmethod
    def stop(self) -> None:
        """Cleanup resources if needed."""


class Receiver(ABC):
    @abstractmethod
    def start(self) -> None:
        """Begin receiving messages."""

    @abstractmethod
    def stop(self) -> None:
        """Stop receiving messages."""

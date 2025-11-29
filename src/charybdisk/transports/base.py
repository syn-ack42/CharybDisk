from abc import ABC, abstractmethod
from typing import Optional

from charybdisk.messages import FileMessage


class SendResult:
    def __init__(self, success: bool, error: Optional[Exception] = None):
        self.success = success
        self.error = error


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

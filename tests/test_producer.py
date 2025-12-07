import os
import time
from requests.exceptions import ReadTimeout
from charybdisk.messages import FileMessage
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from kafka.errors import NoBrokersAvailable

from charybdisk.producer import FileProducer
from charybdisk.transports.base import SendResult, Transport
from charybdisk.transports.http_transport import HttpTransport


class DummyTransport(Transport):
    def __init__(self, succeed: bool = True):
        self.succeed = succeed
        self.sent = []
        self.stopped = False

    def max_transfer_size(self):
        return None

    def send(self, destination: str, message) -> SendResult:
        self.sent.append((destination, message))
        if self.succeed:
            return SendResult(True)
        return SendResult(False, Exception("fail"))

    def stop(self) -> None:
        self.stopped = True


def make_producer(tempdir: str) -> FileProducer:
    producer_cfg = {
        "enabled": True,
        "directories": [
            {
                "id": "test",
                "path": tempdir,
                "transport": "http",
                "destination": "http://example.com/upload",
                "file_pattern": "*",
            }
        ],
        "http": {},
        "scan_interval": 1,
    }
    kafka_cfg = {}
    return FileProducer(producer_cfg, kafka_cfg)


def test_process_file_deletes_when_no_backup():
    with TemporaryDirectory() as tmpdir:
        producer = make_producer(tmpdir)
        dummy_transport = DummyTransport()
        file_path = Path(tmpdir) / "test.txt"
        file_path.write_text("hello world")

        producer.process_file(str(file_path), "http://example.com/upload", "cfg", None, dummy_transport, None)

        assert not file_path.exists(), "File should be removed when no backup directory is configured"
        assert dummy_transport.sent, "Transport should have been called"


def test_process_file_moves_to_backup():
    with TemporaryDirectory() as tmpdir:
        producer = make_producer(tmpdir)
        dummy_transport = DummyTransport()
        src_file = Path(tmpdir) / "test2.txt"
        src_file.write_text("content")

        backup_dir = Path(tmpdir) / "backup"
        producer.process_file(str(src_file), "http://example.com/upload", "cfg", str(backup_dir), dummy_transport, None)

        assert not src_file.exists(), "Source file should be moved out of original directory"
        moved_files = list(backup_dir.glob("*"))
        assert len(moved_files) == 1, "One file should be in backup directory"
        assert moved_files[0].read_text() == "content"


def test_process_file_respects_failed_transport():
    with TemporaryDirectory() as tmpdir:
        producer = make_producer(tmpdir)
        dummy_transport = DummyTransport(succeed=False)
        src_file = Path(tmpdir) / "test3.txt"
        src_file.write_text("content")

        producer.process_file(str(src_file), "http://example.com/upload", "cfg", None, dummy_transport, None)

        # On failure, original file should remain and no deletion should occur
        assert src_file.exists()
        assert dummy_transport.sent


def test_kafka_unavailable_does_not_block_http(monkeypatch, tmp_path):
    class FailingKafkaTransport:
        def __init__(self, kafka_config):  # noqa: ARG002
            raise NoBrokersAvailable()

    monkeypatch.setattr("charybdisk.producer.KafkaTransport", FailingKafkaTransport)

    producer_cfg = {
        "enabled": True,
        "directories": [
            {
                "id": "http",
                "path": tmp_path.as_posix(),
                "transport": "http",
                "destination": "http://example.com/upload",
                "file_pattern": "*",
            },
            {
                "id": "kafka",
                "path": (tmp_path / "kafka_dir").as_posix(),
                "transport": "kafka",
                "topic": "topic",
            },
        ],
        "http": {},
        "scan_interval": 1,
    }
    kafka_cfg = {"broker": "localhost:9092"}

    producer = FileProducer(producer_cfg, kafka_cfg)

    assert producer.kafka_transport is None
    assert producer._kafka_retry_at > time.time()  # type: ignore[attr-defined]

    http_dir = next(d for d in producer.directories if d.get("transport") == "http")
    transport = producer._get_transport_for_directory(http_dir)
    assert isinstance(transport, HttpTransport)


def test_http_read_timeout_treated_as_success(monkeypatch):
    transport = HttpTransport({"timeout": 1})

    def fake_post(*args, **kwargs):  # noqa: ANN001, D401
        raise ReadTimeout("simulated read timeout")

    transport.session.post = fake_post  # type: ignore[assignment]
    msg = FileMessage(
        file_name="x.bin",
        create_timestamp="now",
        content=b"abc",
        file_id="id",
        chunk_index=0,
        total_chunks=1,
        original_size=3,
    )
    result = transport.send("http://example.com/upload", msg)
    assert result.success

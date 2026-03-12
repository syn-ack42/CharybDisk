"""
Simulation tests for HTTP resilience behaviour introduced to handle:
  - Reverse-proxy / backend returning 5xx  → transient, file stays in pending
  - Permanent client errors (4xx)          → file moves to error (unchanged)
  - ConnectTimeout                         → transient, file stays in pending
  - ReadTimeout                            → assumed success, file moves to done_timeout
  - Backend recovery                       → pending file is picked up on next scan
  - HttpPoller 500                         → response closed, retries after poll_interval
"""

import threading
import time
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock, patch

import pytest
from requests.exceptions import ConnectTimeout, ReadTimeout

from charybdisk.messages import FileMessage
from charybdisk.producer import FileProducer
from charybdisk.transports.base import SendResult
from charybdisk.transports.http_transport import HttpPoller, HttpTransport


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_response(status_code: int, text: str = "", content: bytes = b"") -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.ok = status_code < 400
    resp.text = text
    resp.content = content or text.encode()
    return resp


def _make_file_message(name: str = "file.txt", content: bytes = b"data") -> FileMessage:
    return FileMessage(
        file_name=name,
        create_timestamp="2024-01-01T00:00:00",
        content=content,
        file_id=name,
        chunk_index=0,
        total_chunks=1,
        original_size=len(content),
    )


def _make_producer(tempdir: str) -> FileProducer:
    cfg = {
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
        "scan_interval": 60,
    }
    return FileProducer(cfg, {})


# ---------------------------------------------------------------------------
# HttpTransport.send() — SendResult flag assertions
# ---------------------------------------------------------------------------

class TestHttpTransportSendResult:
    def test_200_ok_is_clean_success(self):
        transport = HttpTransport({"timeout": 5})
        transport.session.post = MagicMock(return_value=_make_response(200))
        result = transport.send("http://example.com", _make_file_message())
        assert result.success
        assert not result.assumed_success
        assert not result.transient

    def test_500_is_transient_failure(self):
        transport = HttpTransport({"timeout": 5})
        transport.session.post = MagicMock(return_value=_make_response(500, "Internal Server Error"))
        result = transport.send("http://example.com", _make_file_message())
        assert not result.success
        assert result.transient
        assert not result.assumed_success

    def test_502_is_transient_failure(self):
        transport = HttpTransport({"timeout": 5})
        transport.session.post = MagicMock(return_value=_make_response(502, "Bad Gateway"))
        result = transport.send("http://example.com", _make_file_message())
        assert not result.success
        assert result.transient

    def test_503_is_transient_failure(self):
        transport = HttpTransport({"timeout": 5})
        transport.session.post = MagicMock(return_value=_make_response(503, "Service Unavailable"))
        result = transport.send("http://example.com", _make_file_message())
        assert not result.success
        assert result.transient

    def test_504_is_transient_failure(self):
        transport = HttpTransport({"timeout": 5})
        transport.session.post = MagicMock(return_value=_make_response(504, "Gateway Timeout"))
        result = transport.send("http://example.com", _make_file_message())
        assert not result.success
        assert result.transient

    def test_404_is_permanent_failure(self):
        transport = HttpTransport({"timeout": 5})
        transport.session.post = MagicMock(return_value=_make_response(404, "Not Found"))
        result = transport.send("http://example.com", _make_file_message())
        assert not result.success
        assert not result.transient

    def test_400_is_permanent_failure(self):
        transport = HttpTransport({"timeout": 5})
        transport.session.post = MagicMock(return_value=_make_response(400, "Bad Request"))
        result = transport.send("http://example.com", _make_file_message())
        assert not result.success
        assert not result.transient

    def test_read_timeout_is_assumed_success(self):
        transport = HttpTransport({"timeout": 5})
        transport.session.post = MagicMock(side_effect=ReadTimeout("timed out"))
        result = transport.send("http://example.com", _make_file_message())
        assert result.success
        assert result.assumed_success
        assert not result.transient

    def test_connect_timeout_is_transient_failure(self):
        transport = HttpTransport({"timeout": 5})
        transport.session.post = MagicMock(side_effect=ConnectTimeout("connect timed out"))
        result = transport.send("http://example.com", _make_file_message())
        assert not result.success
        assert result.transient

    def test_connection_error_is_transient_failure(self):
        from requests.exceptions import ConnectionError as ReqConnectionError
        transport = HttpTransport({"timeout": 5})
        transport.session.post = MagicMock(side_effect=ReqConnectionError("connection refused"))
        result = transport.send("http://example.com", _make_file_message())
        assert not result.success
        assert result.transient


# ---------------------------------------------------------------------------
# FileProducer.process_file() — file routing
# ---------------------------------------------------------------------------

class TestProducerFileRouting:

    def _make_http_transport(self, result: SendResult) -> HttpTransport:
        transport = HttpTransport({"timeout": 5})
        transport.send = MagicMock(return_value=result)
        transport.max_transfer_size = MagicMock(return_value=None)
        return transport

    def test_5xx_leaves_file_in_pending(self):
        """Transient failure: file must stay in pending/ for retry — not go to error/."""
        with TemporaryDirectory() as tmpdir:
            producer = _make_producer(tmpdir)
            src = Path(tmpdir) / "pending" / "job.txt"
            src.parent.mkdir(parents=True)
            src.write_bytes(b"payload")

            transport = self._make_http_transport(
                SendResult(False, Exception("503"), transient=True)
            )
            error_dir = Path(tmpdir) / "error"
            done_dir = Path(tmpdir) / "done"

            producer.process_file(
                str(src), "http://host/upload", "cfg",
                str(done_dir), transport, str(error_dir),
            )

            assert src.exists(), "File must remain in pending/ after transient failure"
            assert not list(error_dir.glob("*")) if error_dir.exists() else True, "Error dir must be empty"
            assert not list(done_dir.glob("*")) if done_dir.exists() else True, "Done dir must be empty"

    def test_4xx_moves_file_to_error(self):
        """Permanent failure: file must move to error/ immediately."""
        with TemporaryDirectory() as tmpdir:
            producer = _make_producer(tmpdir)
            src = Path(tmpdir) / "pending" / "bad.txt"
            src.parent.mkdir(parents=True)
            src.write_bytes(b"payload")

            transport = self._make_http_transport(
                SendResult(False, Exception("404 Not Found"), transient=False)
            )
            error_dir = Path(tmpdir) / "error"
            done_dir = Path(tmpdir) / "done"

            producer.process_file(
                str(src), "http://host/upload", "cfg",
                str(done_dir), transport, str(error_dir),
            )

            assert not src.exists(), "File must be moved out of pending/ after permanent failure"
            assert (error_dir / "bad.txt").exists(), "File must land in error/"
            assert not list(done_dir.glob("*")) if done_dir.exists() else True

    def test_read_timeout_moves_file_to_done_timeout(self):
        """assumed_success: file must go to done_timeout/, not done/."""
        with TemporaryDirectory() as tmpdir:
            producer = _make_producer(tmpdir)
            src = Path(tmpdir) / "pending" / "uncertain.txt"
            src.parent.mkdir(parents=True)
            src.write_bytes(b"payload")

            transport = self._make_http_transport(
                SendResult(True, assumed_success=True)
            )
            done_dir = Path(tmpdir) / "done"
            timeout_dir = Path(tmpdir) / "done_timeout"
            error_dir = Path(tmpdir) / "error"

            producer.process_file(
                str(src), "http://host/upload", "cfg",
                str(done_dir), transport, str(error_dir),
                timeout_directory=str(timeout_dir),
            )

            assert not src.exists()
            assert not list(done_dir.glob("*")) if done_dir.exists() else True, "File must NOT be in done/"
            assert list(timeout_dir.glob("*")), "File must be in done_timeout/"
            assert not list(error_dir.glob("*")) if error_dir.exists() else True

    def test_read_timeout_without_explicit_timeout_dir_falls_back_to_done(self):
        """If timeout_directory is not configured, assumed_success falls back to backup_directory."""
        with TemporaryDirectory() as tmpdir:
            producer = _make_producer(tmpdir)
            src = Path(tmpdir) / "pending" / "fallback.txt"
            src.parent.mkdir(parents=True)
            src.write_bytes(b"payload")

            transport = self._make_http_transport(
                SendResult(True, assumed_success=True)
            )
            done_dir = Path(tmpdir) / "done"

            producer.process_file(
                str(src), "http://host/upload", "cfg",
                str(done_dir), transport, None,
                timeout_directory=None,
            )

            assert not src.exists()
            assert list(done_dir.glob("*")), "Should fall back to done/ when timeout_dir is None"

    def test_clean_success_uses_done_not_timeout_dir(self):
        """Normal success must not touch done_timeout/."""
        with TemporaryDirectory() as tmpdir:
            producer = _make_producer(tmpdir)
            src = Path(tmpdir) / "pending" / "good.txt"
            src.parent.mkdir(parents=True)
            src.write_bytes(b"payload")

            transport = self._make_http_transport(SendResult(True))
            done_dir = Path(tmpdir) / "done"
            timeout_dir = Path(tmpdir) / "done_timeout"

            producer.process_file(
                str(src), "http://host/upload", "cfg",
                str(done_dir), transport, None,
                timeout_directory=str(timeout_dir),
            )

            assert not src.exists()
            assert list(done_dir.glob("*")), "File must be in done/"
            assert not timeout_dir.exists() or not list(timeout_dir.glob("*")), "done_timeout/ must be empty"

    def test_custom_timeout_directory_from_config(self):
        """timeout_directory config key is resolved by process_directory."""
        with TemporaryDirectory() as tmpdir:
            cfg = {
                "enabled": True,
                "directories": [
                    {
                        "id": "test",
                        "path": tmpdir,
                        "transport": "http",
                        "destination": "http://example.com/upload",
                        "file_pattern": "*",
                        "timeout_directory": "my_timeout_dir",
                    }
                ],
                "http": {},
                "scan_interval": 60,
            }
            producer = FileProducer(cfg, {})

            transport = self._make_http_transport(SendResult(True, assumed_success=True))
            producer.http_transports["test"] = transport

            src = Path(tmpdir) / "file.txt"
            src.write_bytes(b"data")

            producer.process_directory(cfg["directories"][0])

            timeout_dir = Path(tmpdir) / "my_timeout_dir"
            assert list(timeout_dir.glob("*")), "File must be in configured timeout dir"


# ---------------------------------------------------------------------------
# Simulation: backend recovers after 5xx
# ---------------------------------------------------------------------------

class TestBackendRecovery:

    def test_file_retried_after_backend_recovers(self):
        """
        Simulate: first scan → 503 → file stays in pending.
        Second scan → 200 → file moves to done.
        """
        with TemporaryDirectory() as tmpdir:
            producer = _make_producer(tmpdir)
            pending = Path(tmpdir) / "pending"
            done = Path(tmpdir) / "done"
            pending.mkdir()

            src = Path(tmpdir) / "data.txt"
            src.write_bytes(b"important data")

            call_count = [0]

            def failing_then_succeeding(destination, message):
                call_count[0] += 1
                if call_count[0] == 1:
                    return SendResult(False, Exception("503"), transient=True)
                return SendResult(True)

            transport = HttpTransport({"timeout": 5})
            transport.send = failing_then_succeeding
            transport.max_transfer_size = MagicMock(return_value=None)
            producer.http_transports["test"] = transport

            dir_cfg = producer.directories[0]

            # First scan: 503 → file should be in pending
            producer.process_directory(dir_cfg)
            pending_files = list(pending.glob("*"))
            assert len(pending_files) == 1, "File must be in pending/ after 503"
            assert not list(done.glob("*")) if done.exists() else True

            # Second scan: 200 → file should be in done
            producer.process_directory(dir_cfg)
            assert not list(pending.glob("*")), "Pending must be empty after successful retry"
            assert list(done.glob("*")), "File must be in done/ after successful retry"
            assert call_count[0] == 2

    def test_multiple_files_each_retried_independently(self):
        """All files in pending/ are retried on next scan; each tracked independently."""
        with TemporaryDirectory() as tmpdir:
            producer = _make_producer(tmpdir)
            pending = Path(tmpdir) / "pending"
            done = Path(tmpdir) / "done"
            pending.mkdir()

            (Path(tmpdir) / "a.txt").write_bytes(b"aaa")
            (Path(tmpdir) / "b.txt").write_bytes(b"bbb")

            calls = []

            def first_fail_then_ok(destination, message):
                calls.append(message.file_name)
                if len(calls) <= 2:
                    return SendResult(False, Exception("503"), transient=True)
                return SendResult(True)

            transport = HttpTransport({"timeout": 5})
            transport.send = first_fail_then_ok
            transport.max_transfer_size = MagicMock(return_value=None)
            producer.http_transports["test"] = transport

            dir_cfg = producer.directories[0]

            producer.process_directory(dir_cfg)  # scan 1: both fail
            assert len(list(pending.glob("*"))) == 2

            producer.process_directory(dir_cfg)  # scan 2: both succeed
            assert len(list(pending.glob("*"))) == 0
            assert len(list(done.glob("*"))) == 2


# ---------------------------------------------------------------------------
# HttpPoller — response is closed and retry behaviour on errors
# ---------------------------------------------------------------------------

class TestHttpPollerResilience:

    def test_500_response_is_closed(self):
        """On a 500 response the poller must close the response object."""
        closed = []

        def fake_get(*args, **kwargs):
            resp = _make_response(500, "error", b"error body")
            resp.close = MagicMock(side_effect=lambda: closed.append(True))
            return resp

        received = []
        poller = HttpPoller({"poll_interval": 0, "timeout": 1}, "http://x", received.append)
        poller.session.get = fake_get

        poller._stopped.set()  # stop after first outer loop iteration
        # Run one outer iteration manually
        poller._stopped.clear()

        t = threading.Thread(target=poller.run, daemon=True)
        t.start()
        time.sleep(0.1)
        poller.stop()
        t.join(timeout=2)

        assert closed, "resp.close() must be called on 500 response"
        assert not received, "No messages must be delivered on 500"

    def test_200_no_content_response_is_closed(self):
        """204 / empty body response must also close the response."""
        closed = []

        def fake_get(*args, **kwargs):
            resp = _make_response(204, "", b"")
            resp.close = MagicMock(side_effect=lambda: closed.append(True))
            return resp

        received = []
        poller = HttpPoller({"poll_interval": 0, "timeout": 1}, "http://x", received.append)
        poller.session.get = fake_get

        t = threading.Thread(target=poller.run, daemon=True)
        t.start()
        time.sleep(0.1)
        poller.stop()
        t.join(timeout=2)

        assert closed, "resp.close() must be called on 204"

    def test_poller_recovers_after_repeated_500s(self):
        """After N × 500, a 200 with content must eventually deliver a message."""
        call_count = [0]
        delivered = []

        def fake_get(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] < 4:
                resp = _make_response(500, "err", b"err")
                resp.close = MagicMock()
                return resp
            # Return a valid file on the 4th call, then 204 to stop the inner loop
            if call_count[0] == 4:
                resp = MagicMock()
                resp.status_code = 200
                resp.ok = True
                resp.content = b"file content"
                resp.headers = {
                    "X-File-Name": "recovered.txt",
                    "X-Create-Timestamp": "ts",
                }
                resp.close = MagicMock()
                return resp
            resp = _make_response(204, "", b"")
            resp.close = MagicMock()
            return resp

        poller = HttpPoller({"poll_interval": 0, "timeout": 1}, "http://x", delivered.append)
        poller.session.get = fake_get

        t = threading.Thread(target=poller.run, daemon=True)
        t.start()
        # Give it time to go through 500×3 and then a 200
        deadline = time.time() + 2
        while not delivered and time.time() < deadline:
            time.sleep(0.05)
        poller.stop()
        t.join(timeout=2)

        assert delivered, "Poller must deliver message after backend recovers"
        assert delivered[0].file_name == "recovered.txt"


# ---------------------------------------------------------------------------
# HttpPoller — exponential backoff on consecutive errors
# ---------------------------------------------------------------------------

class TestHttpPollerBackoff:

    def _make_poller(self, poll_interval=0, max_poll_interval=300):
        received = []
        poller = HttpPoller(
            {"poll_interval": poll_interval, "max_poll_interval": max_poll_interval, "timeout": 1},
            "http://x",
            received.append,
        )
        return poller, received

    def test_wait_doubles_on_consecutive_errors(self):
        """Each consecutive 500 must double the wait up to max_poll_interval."""
        poller, _ = self._make_poller(poll_interval=1, max_poll_interval=60)
        waits = []
        original_wait = poller._stopped.wait

        def capturing_wait(timeout=None):
            if timeout and timeout > 0:
                waits.append(timeout)
            return True  # act as if stopped so the loop exits after each wait

        poller._stopped.wait = capturing_wait

        call_count = [0]

        def always_500(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] > 4:
                poller._stopped.set()
            resp = _make_response(500, "err", b"err")
            resp.close = MagicMock()
            return resp

        poller.session.get = always_500
        poller.run()

        # waits should be 1, 2, 4, 8 (poll_interval * 2^n)
        assert len(waits) >= 3
        assert waits[0] == 1
        assert waits[1] == 2
        assert waits[2] == 4

    def test_backoff_capped_at_max_poll_interval(self):
        """Backoff must not exceed max_poll_interval."""
        poller, _ = self._make_poller(poll_interval=10, max_poll_interval=25)
        waits = []

        def capturing_wait(timeout=None):
            if timeout and timeout > 0:
                waits.append(timeout)
            return True  # exit after each wait

        poller._stopped.wait = capturing_wait

        call_count = [0]

        def always_500(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] > 5:
                poller._stopped.set()
            resp = _make_response(500, "err", b"err")
            resp.close = MagicMock()
            return resp

        poller.session.get = always_500
        poller.run()

        assert all(w <= 25 for w in waits), f"All waits must be <= max_poll_interval, got {waits}"
        assert 25 in waits, "Must reach the cap"

    def test_consecutive_errors_reset_after_success(self):
        """After a successful response the backoff counter must reset to zero.

        Flow: 3×500 (backoff grows) → 200 (fetched_any=True, no wait, loop again)
              → 204 (fetched_any=True still, no wait) → 204 (fetched_any=False,
              wait=poll_interval proves backoff was reset).
        """
        poll_interval = 5
        poller, received = self._make_poller(poll_interval=poll_interval, max_poll_interval=300)
        waits = []
        phase = [0]  # 0=error phase, 1=post-success empty polls

        def capturing_wait(timeout=None):
            if timeout and timeout > 0:
                waits.append((phase[0], timeout))
            # Stop once we have collected the post-recovery wait
            if len(waits) >= 4:
                poller._stopped.set()
            return poller._stopped.is_set()

        poller._stopped.wait = capturing_wait

        call_count = [0]

        def errors_then_success_then_empty(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] <= 3:
                resp = _make_response(500, "err", b"err")
                resp.close = MagicMock()
                return resp
            if call_count[0] == 4:
                # One 200 → fetched_any=True; inner loop continues, next call is 204
                resp = MagicMock()
                resp.status_code = 200
                resp.ok = True
                resp.content = b"hello"
                resp.headers = {"X-File-Name": "f.txt", "X-Create-Timestamp": ""}
                resp.close = MagicMock()
                return resp
            # 204: inner loop breaks; first time fetched_any=True (no wait),
            # second time fetched_any=False (wait=poll_interval → recorded as phase 1)
            phase[0] = 1
            resp = _make_response(204, "", b"")
            resp.close = MagicMock()
            return resp

        poller.session.get = errors_then_success_then_empty
        poller.run()

        error_waits = [w for p, w in waits if p == 0]
        post_recovery_waits = [w for p, w in waits if p == 1]

        assert len(error_waits) >= 2, f"Expected growing error waits, got {error_waits}"
        assert error_waits[1] > error_waits[0], "Backoff must grow during error phase"
        assert post_recovery_waits, "Must wait after recovery"
        assert post_recovery_waits[0] == poll_interval, (
            f"First wait after recovery must be poll_interval={poll_interval}, got {post_recovery_waits[0]}"
        )

    def test_exception_also_triggers_backoff(self):
        """Network exceptions (not just 5xx) must increment the backoff counter."""
        poller, _ = self._make_poller(poll_interval=1, max_poll_interval=60)
        waits = []

        def capturing_wait(timeout=None):
            if timeout and timeout > 0:
                waits.append(timeout)
            return True

        poller._stopped.wait = capturing_wait

        call_count = [0]

        def always_raises(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] > 3:
                poller._stopped.set()
            raise ConnectionError("connection refused")

        poller.session.get = always_raises
        poller.run()

        assert len(waits) >= 2
        assert waits[1] > waits[0], "Backoff must grow for repeated exceptions"

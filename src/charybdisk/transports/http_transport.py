import base64
import logging
import threading
from typing import Any, Dict, Optional

import requests
from requests import Session

from charybdisk.messages import FileMessage
from charybdisk.transports.base import Receiver, SendResult, Transport

logger = logging.getLogger('charybdisk.transport.http')


def _build_headers(message: FileMessage, extra_headers: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    safe_name, changed = sanitize_header_filename(message.file_name)
    if changed:
        logger.warning(f"Non-ASCII characters in file name '{message.file_name}' were replaced for HTTP headers as '{safe_name}'")

    headers = {
        'X-File-Name': safe_name,
        'X-Create-Timestamp': message.create_timestamp,
        'X-Original-File-Name-B64': base64.b64encode(message.file_name.encode('utf-8')).decode('ascii'),
    }
    if extra_headers:
        headers.update(extra_headers)
    return headers


def sanitize_header_filename(name: str) -> (str, bool):
    """
    HTTP headers must be ASCII. Replace any non-ASCII characters with U+XXXX notation.
    """
    changed = False
    out_chars = []
    for ch in name:
        if ord(ch) < 128:
            out_chars.append(ch)
        else:
            out_chars.append(f"U+{ord(ch):04X}")
            changed = True
    return "".join(out_chars), changed


class HttpTransport(Transport):
    def __init__(self, http_config: Dict[str, Any]) -> None:
        self.http_config = http_config
        self.session: Session = requests.Session()
        self.extra_headers = http_config.get('default_headers', {})
        self._max_size = http_config.get('max_transfer_file_size')  # optional per transport

    def max_transfer_size(self) -> Optional[int]:
        return self._max_size

    def send(self, destination: str, message: FileMessage) -> SendResult:
        url = destination
        headers = _build_headers(message, self.extra_headers)
        try:
            resp = self.session.post(url, headers=headers, data=message.content, timeout=self.http_config.get('timeout', 30))
            if resp.ok:
                return SendResult(True)
            return SendResult(False, Exception(f"HTTP {resp.status_code}: {resp.text}"))
        except Exception as e:
            return SendResult(False, e)

    def stop(self) -> None:
        self.session.close()


class HttpPoller(Receiver, threading.Thread):
    def __init__(
        self,
        http_config: Dict[str, Any],
        url: str,
        on_message,
        headers: Optional[Dict[str, str]] = None,
    ) -> None:
        threading.Thread.__init__(self, daemon=False)
        self.http_config = http_config
        self.url = url
        self.on_message = on_message
        self.session: Session = requests.Session()
        base_headers = http_config.get('default_headers', {})
        headers = headers or {}
        merged = dict(base_headers)
        merged.update(headers)
        self.extra_headers = merged
        self._stopped = threading.Event()

    def run(self) -> None:
        poll_interval = self.http_config.get('poll_interval', 5)
        timeout = self.http_config.get('timeout', 30)
        while not self._stopped.is_set():
            fetched_any = False
            try:
                while not self._stopped.is_set():
                    resp = self.session.get(self.url, headers=self.extra_headers, timeout=timeout)
                    if resp.status_code == 204 or not resp.content:
                        break
                    if resp.ok:
                        fetched_any = True
                        original_b64 = resp.headers.get('X-Original-File-Name-B64')
                        if original_b64:
                            try:
                                file_name = base64.b64decode(original_b64).decode('utf-8')
                            except Exception:
                                file_name = resp.headers.get('X-File-Name', 'file.bin')
                        else:
                            file_name = resp.headers.get('X-File-Name', 'file.bin')
                        create_timestamp = resp.headers.get('X-Create-Timestamp', '')
                        content = resp.content
                        self.on_message(
                            FileMessage(
                                file_name=file_name,
                                create_timestamp=create_timestamp,
                                content=content,
                                file_id=file_name,
                                chunk_index=0,
                                total_chunks=1,
                                original_size=len(content),
                            )
                        )
                        continue

                    logger.error(f"HTTP poll failed {resp.status_code}: {resp.text}")
                    break
            except Exception as e:
                logger.error(f"HTTP polling error for {self.url}: {e}")
            finally:
                # Only sleep when there is nothing left to pull
                if self._stopped.is_set():
                    break
                if not fetched_any:
                    self._stopped.wait(poll_interval)

    def start(self) -> None:  # type: ignore[override]
        threading.Thread.start(self)

    def stop(self) -> None:
        self._stopped.set()
        self.session.close()

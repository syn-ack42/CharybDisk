import glob
import logging
import os
import threading
import time
from typing import Any, Dict, List, Optional

from kafka.errors import KafkaError, NoBrokersAvailable

from charybdisk.file_preparer import DEFAULT_MAX_TRANSFER_FILE_SIZE, FilePreparer, backup_file
from charybdisk.messages import FileMessage
from charybdisk.transports.base import SendResult, Transport
from charybdisk.transports.http_transport import HttpTransport
from charybdisk.transports.kafka_transport import KafkaTransport
import uuid

KAFKA_CONN_RETRY_INTERVAL_SECONDS = 5
DEFAULT_MAX_MESSAGE_BYTES = 1_000_000
DEFAULT_CHUNK_FRACTION = 0.6  # leave headroom for base64+metadata
DEFAULT_CHUNK_SIZE = int(DEFAULT_MAX_MESSAGE_BYTES * DEFAULT_CHUNK_FRACTION)

logger = logging.getLogger('charybdisk.producer')


class FileProducer(threading.Thread):
    """
    Watches configured directories and publishes files using the configured transport (Kafka/HTTP) per directory.
    Runs in its own thread so it can coexist with consumers in a single process.
    """

    def __init__(self, producer_config: Dict[str, Any], kafka_config: Dict[str, Any]) -> None:
        super().__init__(daemon=True)
        self.producer_config = producer_config
        self.kafka_config = kafka_config
        self.stop_event = threading.Event()
        self.file_preparer = FilePreparer(max_transfer_file_size=None)  # per-transport limit applied at call time
        self.directories: List[Dict[str, Any]] = producer_config['directories']
        self.scan_interval: int = producer_config.get('scan_interval', 60)
        self.default_http_config = producer_config.get('http', {})
        self.kafka_transport: Optional[KafkaTransport] = None
        self.http_transports: Dict[str, HttpTransport] = {}
        self.max_message_bytes = producer_config.get('max_message_bytes') or kafka_config.get('max_message_bytes') or DEFAULT_MAX_MESSAGE_BYTES
        self.chunk_size = producer_config.get('chunk_size_bytes', int(self.max_message_bytes * DEFAULT_CHUNK_FRACTION))

        self._init_kafka_transport_if_needed()

    def _init_kafka_transport_if_needed(self) -> None:
        kafka_dirs = [d for d in self.directories if d.get('transport') == 'kafka']
        if not kafka_dirs:
            return
        self.kafka_transport = KafkaTransport(self.kafka_config)
        topics = {d['topic']: d for d in kafka_dirs if d.get('topic')}
        self.kafka_transport.ensure_topics(topics)

    def _get_transport_for_directory(self, directory: Dict[str, Any]) -> Optional[Transport]:
        mode = directory.get('transport')
        if mode == 'kafka':
            if self.kafka_transport is None:
                logger.error("Kafka transport requested but not initialized.")
            return self.kafka_transport
        if mode == 'http':
            dir_id = directory['id']
            if dir_id not in self.http_transports:
                self.http_transports[dir_id] = HttpTransport(self._merged_http_config(directory))
            return self.http_transports[dir_id]
        logger.error(f"Unknown transport '{mode}' for directory '{directory.get('id')}'.")
        return None

    def _merged_http_config(self, directory: Dict[str, Any]) -> Dict[str, Any]:
        cfg = dict(self.default_http_config)
        dir_http_cfg = directory.get('http', {})
        cfg.update({k: v for k, v in dir_http_cfg.items() if k != 'headers'})

        default_headers = dict(self.default_http_config.get('default_headers', {}))
        dir_headers = directory.get('headers') or dir_http_cfg.get('headers') or {}
        default_headers.update(dir_headers)
        cfg['default_headers'] = default_headers
        return cfg

    def run(self) -> None:
        while not self.stop_event.is_set():
            try:
                self.produce_messages()
            except Exception as e:
                logger.error(
                    f"File producer encountered an error. Retrying in {KAFKA_CONN_RETRY_INTERVAL_SECONDS} seconds...; error: {e}"
                )
                self._wait_before_retry()
            finally:
                self._stop_transports()

    def produce_messages(self) -> None:
        logger.info("Starting to watch directories.")
        while not self.stop_event.is_set():
            for directory in self.directories:
                if self.stop_event.is_set():
                    break
                try:
                    self.process_directory(directory)
                except Exception as e:
                    self.handle_directory_processing_error(directory, e)

            if self.stop_event.wait(self.scan_interval):
                break

    def process_directory(self, directory: Dict[str, Any]) -> None:
        directory_path = directory['path']
        destination = directory.get('destination') or directory.get('topic') or directory.get('url')
        config_id = directory['id']
        file_pattern = directory.get('file_pattern', '*')
        backup_directory = directory.get('backup_directory')
        transport = self._get_transport_for_directory(directory)
        error_directory = directory.get('error_directory') or os.path.join(directory_path, 'error')

        if not destination:
            logger.error(f"Directory config '{config_id}' missing destination (topic/url). Skipping.")
            return
        if transport is None:
            logger.error(f"Directory config '{config_id}' has no usable transport. Skipping.")
            return

        if not os.path.exists(directory_path):
            logger.error(f"Directory '{directory_path}' does not exist.")
            return

        if not os.access(directory_path, os.R_OK):
            logger.error(f"No read permission for directory '{directory_path}'.")
            return

        files = glob.glob(os.path.join(directory_path, file_pattern))
        files = [f for f in files if os.path.isfile(f)]
        for file_path in files:
            if self.stop_event.is_set():
                break
            try:
                self.process_file(file_path, destination, config_id, backup_directory, transport, error_directory)
            except Exception as e:
                self.handle_file_processing_error(file_path, e)

    def process_file(
        self,
        file_path: str,
        destination: str,
        config_id: str,
        backup_directory: Optional[str],
        transport: Transport,
        error_directory: Optional[str],
    ) -> None:
        try:
            prepared = self.file_preparer.prepare(file_path, transport.max_transfer_size())
            if prepared is None:
                time.sleep(1)
                return

            file_bytes = prepared.message.content
            max_transport_size = transport.max_transfer_size() or self.chunk_size
            base_chunk_size = min(self.chunk_size, max_transport_size, int(self.max_message_bytes * DEFAULT_CHUNK_FRACTION))
            chunk_size = max(1, base_chunk_size)
            total_chunks = max(1, (len(file_bytes) + chunk_size - 1) // chunk_size)
            file_id = f"{prepared.message.file_name}-{uuid.uuid4().hex}"
            all_sent = True

            for idx in range(total_chunks):
                start = idx * chunk_size
                end = start + chunk_size
                chunk = file_bytes[start:end]
                chunk_msg = FileMessage(
                    file_name=prepared.message.file_name,
                    create_timestamp=prepared.message.create_timestamp,
                    content=chunk,
                    file_id=file_id,
                    chunk_index=idx,
                    total_chunks=total_chunks,
                    original_size=len(file_bytes),
                )
                result: SendResult = transport.send(destination, chunk_msg)
                if not result.success:
                    all_sent = False
                    raise result.error or Exception("Unknown transport send error")

            if all_sent:
                logger.info(
                    f"Sent file '{prepared.message.file_name}' (create timestamp: {prepared.message.create_timestamp}) to destination '{destination}' (Configuration ID: {config_id}). Backup file name: '{prepared.backup_name}'."
                )
                if backup_directory and prepared.backup_name:
                    backup_file(prepared.original_path, backup_directory, prepared.backup_name)
                elif backup_directory is None:
                    try:
                        os.remove(prepared.original_path)
                        logger.info(f"Removed source file '{prepared.original_path}' after successful send (no backup configured).")
                    except Exception as e:
                        logger.error(f"Failed to remove source file '{prepared.original_path}' after send: {e}")
        except Exception as e:
            logger.error(f"Transport send failed for file '{file_path}': {e}")
            if error_directory:
                try:
                    os.makedirs(error_directory, exist_ok=True)
                    dest = os.path.join(error_directory, os.path.basename(file_path))
                    os.replace(file_path, dest)
                    logger.info(f"Moved file '{file_path}' to error directory '{error_directory}' after send failure.")
                except Exception as move_err:
                    logger.error(f"Failed to move file '{file_path}' to error directory '{error_directory}': {move_err}")

    def handle_file_processing_error(self, file_path: str, error: Exception) -> None:
        if isinstance(error, (NoBrokersAvailable, KafkaError)):
            logger.error(f"Transport error occurred while processing file '{file_path}': {error}")
            raise
        else:
            logger.error(f"Error processing file '{file_path}': {error}, continuing")

    def handle_directory_processing_error(self, directory: Dict[str, Any], error: Exception) -> None:
        if isinstance(error, (NoBrokersAvailable, KafkaError)):
            logger.error(f"Transport error occurred while processing directory '{directory['path']}': {error}")
            raise
        else:
            logger.error(f"Error processing directory '{directory['path']}': {error}, continuing")

    def _wait_before_retry(self) -> None:
        self.stop_event.wait(KAFKA_CONN_RETRY_INTERVAL_SECONDS)

    def stop(self) -> None:
        self.stop_event.set()
        self._stop_transports()

    def _stop_transports(self) -> None:
        if self.kafka_transport:
            try:
                self.kafka_transport.stop()
            except Exception as e:
                logger.error(f"Error stopping Kafka transport: {e}")
        for transport in self.http_transports.values():
            try:
                transport.stop()
            except Exception as e:
                logger.error(f"Error stopping HTTP transport: {e}")

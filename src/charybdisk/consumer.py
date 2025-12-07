import logging
import threading
from typing import Any, Dict, List, Optional
import os
import json
from pathlib import Path
import time

from kafka.errors import KafkaError, NoBrokersAvailable

from charybdisk.file_writer import write_file_safe
from charybdisk.messages import FileMessage
from charybdisk.transports.http_transport import HttpPoller
from charybdisk.transports.kafka_transport import KafkaReceiver

logger = logging.getLogger('charybdisk.consumer')
KAFKA_RETRY_INTERVAL_SECONDS = 60


class FileConsumerGroup(threading.Thread):
    """
    Starts one receiver per consumer config entry (Kafka or HTTP) and writes files safely to disk.
    """

    def __init__(self, consumer_config: Dict[str, Any], kafka_config: Dict[str, Any]) -> None:
        super().__init__(daemon=True)
        self.consumer_config = consumer_config
        self.kafka_config = kafka_config
        self.receivers: List[threading.Thread] = []
        self.stop_event = threading.Event()
        self.work_dir = consumer_config.get('working_directory', '/tmp/charybdisk_parts')
        self.kafka_topics: List[Dict[str, Any]] = []
        self.kafka_retry_at: float = 0
        self.default_group_id = kafka_config.get('default_group_id')
        self.start_from_end = self.consumer_config.get('start_from_end', False)

    def run(self) -> None:
        default_http_cfg = self.consumer_config.get('http', {})

        for topic_cfg in self.consumer_config.get('topics', []):
            transport = topic_cfg.get('transport')
            output_directory = topic_cfg['output_directory']
            output_suffix = topic_cfg.get('output_suffix')
            on_message = self._build_handler(output_directory, output_suffix)

            if transport == 'http':
                url = topic_cfg.get('url') or topic_cfg.get('endpoint')
                if not url:
                    logger.error("HTTP consumer entry missing 'url'/'endpoint'")
                    continue
                headers = topic_cfg.get('headers') or {}
                http_cfg = dict(default_http_cfg)
                http_cfg.update({k: v for k, v in (topic_cfg.get('http') or {}).items() if k != 'headers'})
                receiver = HttpPoller(http_cfg, url, on_message, headers=headers)
                receiver.start()
                self.receivers.append(receiver)
            else:
                self.kafka_topics.append(topic_cfg)

        self._maybe_start_kafka_receivers()

        # Keep thread alive while receivers run
        while not self.stop_event.is_set():
            self._maybe_start_kafka_receivers()
            self.stop_event.wait(1)

    def _maybe_start_kafka_receivers(self) -> None:
        if not self.kafka_topics:
            return
        if time.time() < self.kafka_retry_at:
            return
        remaining_topics: List[Dict[str, Any]] = []
        for topic_cfg in self.kafka_topics:
            topic = topic_cfg.get('topic')
            if not topic:
                logger.error("Kafka consumer entry missing 'topic'")
                continue
            try:
                on_message = self._build_handler(topic_cfg['output_directory'], topic_cfg.get('output_suffix'))
                group_id = topic_cfg.get('group_id', self.default_group_id)
                receiver = KafkaReceiver(self.kafka_config, topic, group_id, self.start_from_end, on_message)
                receiver.start()
                self.receivers.append(receiver)
            except (NoBrokersAvailable, KafkaError) as e:
                logger.warning(
                    "Kafka unavailable for topic '%s'; retrying in %s seconds: %s",
                    topic,
                    KAFKA_RETRY_INTERVAL_SECONDS,
                    e,
                )
                remaining_topics.append(topic_cfg)
            except Exception as e:
                logger.error(f"Failed to start Kafka receiver for topic '{topic}': {e}")
                remaining_topics.append(topic_cfg)
        self.kafka_topics = remaining_topics
        if self.kafka_topics:
            self.kafka_retry_at = time.time() + KAFKA_RETRY_INTERVAL_SECONDS

    def _build_handler(self, output_directory: str, output_suffix: Optional[str]):
        assembler = ChunkAssembler(self.work_dir, output_directory, output_suffix)

        def handle(file_message: FileMessage) -> None:
            final_path = assembler.handle_chunk(file_message)
            if final_path:
                logger.info(
                    f"Wrote file '{final_path.name}' (original: '{file_message.file_name}') to directory '{output_directory}'"
                )

        return handle

    def stop(self) -> None:
        self.stop_event.set()
        for receiver in self.receivers:
            stop_fn = getattr(receiver, 'stop', None)
            if callable(stop_fn):
                stop_fn()
        for receiver in self.receivers:
            receiver.join(timeout=2)


class ChunkAssembler:
    """
    Persists chunks to disk and assembles when all parts are present.
    """

    def __init__(self, work_dir: str, output_directory: str, output_suffix: Optional[str]) -> None:
        self.work_dir = work_dir
        self.output_directory = output_directory
        self.output_suffix = output_suffix
        os.makedirs(self.work_dir, exist_ok=True)

    def handle_chunk(self, file_message: FileMessage) -> Optional[Path]:
        file_dir = Path(self.work_dir) / file_message.file_id
        os.makedirs(file_dir, exist_ok=True)

        meta_path = file_dir / "meta.json"
        chunk_path = file_dir / f"chunk_{file_message.chunk_index}"

        # Persist metadata
        meta = {
            "file_name": file_message.file_name,
            "total_chunks": file_message.total_chunks,
            "original_size": file_message.original_size,
        }
        with open(meta_path, "w") as f:
            json.dump(meta, f)

        # Write chunk
        with open(chunk_path, "wb") as f:
            f.write(file_message.content)

        # Check completion
        chunks = list(file_dir.glob("chunk_*"))
        if len(chunks) < file_message.total_chunks:
            return None

        # Assemble
        chunks_sorted = sorted(chunks, key=lambda p: int(p.name.split("_")[1]))
        content = b""
        for ch in chunks_sorted:
            content += ch.read_bytes()

        final_path = write_file_safe(self.output_directory, file_message.file_name, content, self.output_suffix)
        # Cleanup
        for ch in chunks_sorted:
            ch.unlink(missing_ok=True)
        meta_path.unlink(missing_ok=True)
        try:
            file_dir.rmdir()
        except OSError:
            pass
        return final_path

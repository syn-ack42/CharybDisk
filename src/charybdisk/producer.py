import glob
import logging
import os
import threading
import time
from typing import Any, Dict, List, Optional

from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import KafkaError, NoBrokersAvailable

from charybdisk.file_preparer import FilePreparer, backup_file, MAX_TRANSFER_FILE_SIZE
from charybdisk.kafka_helpers import build_kafka_client_config
from charybdisk.messages import encode_message

KAFKA_CONN_RETRY_INTERVAL_SECONDS = 5

logger = logging.getLogger('charybdisk.producer')


class KafkaFileProducer(threading.Thread):
    """
    Watches configured directories and publishes files to Kafka topics.
    Runs in its own thread so it can coexist with consumers in a single process.
    """

    def __init__(self, producer_config: Dict[str, Any], kafka_config: Dict[str, Any]) -> None:
        super().__init__(daemon=True)
        self.producer_config = producer_config
        self.kafka_config = kafka_config
        self.stop_event = threading.Event()
        self.kafka_producer: Optional[KafkaProducer] = None
        self.admin_client: Optional[KafkaAdminClient] = None
        self.file_preparer = FilePreparer(max_transfer_file_size=MAX_TRANSFER_FILE_SIZE)
        self.directories: List[Dict[str, Any]] = producer_config['directories']
        self.scan_interval: int = producer_config.get('scan_interval', 60)

    def run(self) -> None:
        while not self.stop_event.is_set():
            try:
                self.setup_kafka()
                logger.info("Kafka file producer connected successfully.")
                self.produce_messages()
            except (NoBrokersAvailable, KafkaError) as e:
                logger.error(
                    f"No brokers available or Kafka error. Retrying in {KAFKA_CONN_RETRY_INTERVAL_SECONDS} seconds...; error: {e}"
                )
                self._wait_before_retry()
            except Exception as e:
                logger.error(
                    f"Failed to start Kafka file producer. Retrying in {KAFKA_CONN_RETRY_INTERVAL_SECONDS} seconds...; error: {e}"
                )
                self._wait_before_retry()
            finally:
                self.close_producer()

    def setup_kafka(self) -> None:
        kafka_client_config = build_kafka_client_config(self.kafka_config)
        self.admin_client = KafkaAdminClient(**kafka_client_config)
        self.kafka_producer = KafkaProducer(**kafka_client_config)
        logger.info(f'Created KafkaProducer with id {self.kafka_config.get("client_id")}')
        self.create_topics()

    def create_topics(self) -> None:
        if self.admin_client is None:
            return

        try:
            existing_topics = self.admin_client.list_topics()
            for directory in self.directories:
                topic = directory['topic']
                if topic not in existing_topics:
                    new_topic = NewTopic(name=topic, num_partitions=1, replication_factor=3)
                    self.admin_client.create_topics([new_topic])
                    logger.debug(f'Created new Kafka topic "{topic}" for directory watcher with ID "{directory["id"]}"')
        except KafkaError as e:
            logger.error(f"Failed to create topics: {e}")
            raise

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
        topic = directory['topic']
        config_id = directory['id']
        file_pattern = directory.get('file_pattern', '*')
        backup_directory = directory.get('backup_directory')

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
                self.process_file(file_path, topic, config_id, backup_directory)
            except Exception as e:
                self.handle_file_processing_error(file_path, e)

    def process_file(self, file_path: str, topic: str, config_id: str, backup_directory: Optional[str]) -> None:
        if self.kafka_producer is None:
            raise KafkaError("Kafka producer not initialized")

        prepared = self.file_preparer.prepare(file_path)
        if prepared is None:
            time.sleep(1)
            return

        try:
            self.kafka_producer.send(topic, value=encode_message(prepared.message))
            logger.info(
                f"Sent file '{prepared.message.file_name}' (create timestamp: {prepared.message.create_timestamp}) to Kafka topic '{topic}' (Configuration ID: {config_id}). Backup file name: '{prepared.backup_name}'."
            )

            if backup_directory and prepared.backup_name:
                backup_file(prepared.original_path, backup_directory, prepared.backup_name)
        except (NoBrokersAvailable, KafkaError):
            raise
        except Exception as e:
            logger.error(f"Unexpected error occurred while processing file '{file_path}': {e}, continuing")

    def handle_file_processing_error(self, file_path: str, error: Exception) -> None:
        if isinstance(error, (NoBrokersAvailable, KafkaError)):
            logger.error(f"Kafka error occurred while processing file '{file_path}': {error}")
            raise
        else:
            logger.error(f"Error processing file '{file_path}': {error}, continuing")

    def handle_directory_processing_error(self, directory: Dict[str, Any], error: Exception) -> None:
        if isinstance(error, (NoBrokersAvailable, KafkaError)):
            logger.error(f"Kafka error occurred while processing directory '{directory['path']}': {error}")
            raise
        else:
            logger.error(f"Error processing directory '{directory['path']}': {error}, continuing")

    def _wait_before_retry(self) -> None:
        self.stop_event.wait(KAFKA_CONN_RETRY_INTERVAL_SECONDS)

    def close_producer(self) -> None:
        if self.kafka_producer is not None:
            self.kafka_producer.close()
        if self.admin_client is not None:
            self.admin_client.close()

    def stop(self) -> None:
        self.stop_event.set()

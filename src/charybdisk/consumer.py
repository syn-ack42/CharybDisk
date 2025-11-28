import base64
import json
import logging
import os
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable

from charybdisk.kafka_helpers import build_kafka_client_config

logger = logging.getLogger('charybdisk.consumer')


class KafkaFileConsumer(threading.Thread):
    """
    Consumes files from Kafka topics and writes them to the configured output directories.
    Each consumer runs on its own thread.
    """

    def __init__(
        self,
        kafka_config: Dict[str, Any],
        topic_config: Dict[str, Any],
        start_from_end: bool,
        default_group_id: Optional[str] = None,
    ) -> None:
        super().__init__(daemon=True)
        self.kafka_config = kafka_config
        self.topic_config = topic_config
        self.group_id = topic_config.get('group_id', default_group_id or kafka_config.get('default_group_id', 'default_group'))
        self.output_directory = topic_config['output_directory']
        self.output_suffix = topic_config.get('output_suffix')
        self.stop_event = threading.Event()

        if self.output_suffix and not self.output_suffix.startswith('.'):
            self.output_suffix = f'.{self.output_suffix}'

        kafka_consumer_config = build_kafka_client_config(kafka_config)
        kafka_consumer_config.update({
            'group_id': self.group_id,
            'auto_offset_reset': 'latest' if start_from_end else kafka_config.get('auto_offset_reset', 'earliest'),
            'enable_auto_commit': kafka_config.get('enable_auto_commit', True),
            'value_deserializer': lambda x: x.decode('utf-8'),
        })

        self.consumer = KafkaConsumer(
            topic_config['topic'],
            **kafka_consumer_config,
        )
        logger.info(
            f'Created a new consumer for topic {topic_config["topic"]}, group id: {self.group_id}, client id: {kafka_config.get("client_id")}'
        )

    def run(self) -> None:
        try:
            self.consume_messages()
        except (NoBrokersAvailable, KafkaError) as e:
            logger.error(f"Kafka error in consumer for topic '{self.topic_config['topic']}': {e}")
        except Exception as e:
            logger.error(f"Unexpected error in consumer for topic '{self.topic_config['topic']}': {e}")
        finally:
            self.consumer.close()

    def consume_messages(self) -> None:
        for message in self.consumer:
            if self.stop_event.is_set():
                break
            self.write_file(json.loads(message.value), message)

    def write_file(self, message: Dict[str, Any], kafka_message: Any) -> None:
        file_name = message['file_name']
        file_content = base64.b64decode(message['content'].encode('utf-8'))
        filepath = os.path.join(self.output_directory, file_name)
        if self.output_suffix:
            new_filepath = Path(filepath).with_suffix(self.output_suffix)
            logger.debug(f'write_file() changing file suffix; old file {filepath}, new file {new_filepath}')
            filepath = new_filepath

        if not os.path.exists(self.output_directory):
            try:
                os.makedirs(self.output_directory)
                logger.warning(f"Output directory '{self.output_directory}' did not exist, created it.")
            except Exception as e:
                logger.error(f"Error creating output directory '{self.output_directory}': {e}")
                return

        if os.path.exists(filepath):
            try:
                with open(filepath, 'rb') as existing_file:
                    existing_content = existing_file.read()
                if existing_content == file_content:
                    logger.info(f"File '{filepath}' already exists with identical content. Skipping write.")
                    return
                else:
                    timestamp = datetime.now().strftime('%Y%m%d%H%M%S%f')
                    new_filepath = Path(filepath).with_name(f"{Path(filepath).stem}_{timestamp}{Path(filepath).suffix}")
                    filepath = new_filepath
                    logger.info(f"File '{filepath}' already exists with different content. Writing new file with timestamp.")
            except Exception as e:
                logger.error(f"Error reading existing file '{filepath}': {e}")
                return

        try:
            with open(filepath, 'wb') as file:
                file.write(file_content)
            logger.info(
                f"Received message from Kafka topic '{kafka_message.topic}': Wrote file '{Path(filepath).name}' (original: '{file_name}') to directory '{self.output_directory}'"
            )
        except Exception as e:
            logger.error(
                f"Error writing file '{filepath}' (original: '{file_name}') to directory '{self.output_directory}': {e}"
            )

    def stop(self) -> None:
        self.stop_event.set()
        self.consumer.close()


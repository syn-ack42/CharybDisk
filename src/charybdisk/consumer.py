import logging
import threading
from typing import Any, Dict, Optional

from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable

from charybdisk.file_writer import write_file_safe
from charybdisk.kafka_helpers import build_kafka_client_config
from charybdisk.messages import decode_message

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
            self.handle_message(message.value, message)

    def handle_message(self, raw_message: Any, kafka_message: Any) -> None:
        try:
            file_message = decode_message(raw_message)
        except Exception as e:
            logger.error(f"Failed to decode message from Kafka topic '{kafka_message.topic}': {e}")
            return

        final_path = write_file_safe(self.output_directory, file_message.file_name, file_message.content, self.output_suffix)
        if final_path:
            logger.info(
                f"Received message from Kafka topic '{kafka_message.topic}': Wrote file '{final_path.name}' (original: '{file_message.file_name}') to directory '{self.output_directory}'"
            )

    def stop(self) -> None:
        self.stop_event.set()
        self.consumer.close()

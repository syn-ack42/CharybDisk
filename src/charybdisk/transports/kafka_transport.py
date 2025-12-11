import logging
import threading
from typing import Any, Callable, Dict, Optional

from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import KafkaError, NoBrokersAvailable, TopicAlreadyExistsError

from charybdisk.kafka_helpers import build_kafka_client_config
from charybdisk.messages import FileMessage, decode_message, encode_message
from charybdisk.transports.base import Receiver, SendResult, Transport

logger = logging.getLogger('charybdisk.transport.kafka')


class KafkaTransport(Transport):
    def __init__(self, kafka_config: Dict[str, Any]) -> None:
        self.kafka_config = kafka_config
        self.kafka_producer: Optional[KafkaProducer] = None
        self.admin_client: Optional[KafkaAdminClient] = None
        self.replication_factor: int = kafka_config.get('replication_factor', 1)
        self._setup()

    def _setup(self) -> None:
        client_config = build_kafka_client_config(self.kafka_config)
        self.admin_client = KafkaAdminClient(**client_config)
        self.kafka_producer = KafkaProducer(**client_config)
        logger.info(f"Kafka transport ready with client id {self.kafka_config.get('client_id')}")

    def ensure_topics(self, topics: Dict[str, Dict[str, Any]]) -> None:
        if self.admin_client is None:
            return
        try:
            existing_topics = self.admin_client.list_topics()
            for topic in topics.keys():
                if topic not in existing_topics:
                    new_topic = NewTopic(name=topic, num_partitions=1, replication_factor=self.replication_factor)
                    self.admin_client.create_topics([new_topic])
                    logger.debug(f'Created new Kafka topic "{topic}"')
        except KafkaError as e:
            # Ignore "already exists" errors; re-raise others
            if isinstance(e, TopicAlreadyExistsError) or "already exists" in str(e):
                logger.info(f"Topic creation skipped (already exists): {e}")
            else:
                logger.error(f"Failed to create topics: {e}")
                raise

    def send(self, destination: str, message: FileMessage) -> SendResult:
        if self.kafka_producer is None:
            return SendResult(False, KafkaError("Kafka producer not initialized"))
        try:
            future = self.kafka_producer.send(destination, value=encode_message(message))
            # Block briefly to surface broker-side errors
            future.get(timeout=10)
            return SendResult(True)
        except Exception as e:
            logger.error(f"Kafka send failed for topic '{destination}': {e}")
            return SendResult(False, e)

    def max_transfer_size(self) -> Optional[int]:
        return self.kafka_config.get('max_transfer_file_size')

    def stop(self) -> None:
        if self.kafka_producer is not None:
            self.kafka_producer.close()
        if self.admin_client is not None:
            self.admin_client.close()


class KafkaReceiver(Receiver, threading.Thread):
    def __init__(
        self,
        kafka_config: Dict[str, Any],
        topic: str,
        group_id: Optional[str],
        start_from_end: bool,
        on_message: Callable[[FileMessage], None],
    ) -> None:
        threading.Thread.__init__(self, daemon=False)
        self.kafka_config = kafka_config
        self.topic = topic
        self.group_id = group_id or kafka_config.get('default_group_id', 'default_group')
        self.start_from_end = start_from_end
        self.on_message = on_message
        self.stop_event = threading.Event()

        consumer_config = build_kafka_client_config(kafka_config)
        consumer_config.update({
            'group_id': self.group_id,
            'auto_offset_reset': 'latest' if start_from_end else kafka_config.get('auto_offset_reset', 'earliest'),
            'enable_auto_commit': kafka_config.get('enable_auto_commit', True),
            'value_deserializer': lambda x: x.decode('utf-8'),
        })

        self.consumer = KafkaConsumer(self.topic, **consumer_config)
        logger.info(f'Kafka receiver created for topic {self.topic}, group {self.group_id}')

    def run(self) -> None:
        try:
            while not self.stop_event.is_set():
                records = self.consumer.poll(timeout_ms=1000)
                if not records:
                    continue
                for topic_partition, batch in records.items():
                    for message in batch:
                        try:
                            file_message = decode_message(message.value)
                            self.on_message(file_message)
                        except Exception as e:
                            logger.error(f"Failed to handle Kafka message from topic {self.topic}: {e}")
        except Exception as e:
            # When stopping, the consumer.poll loop can be interrupted; only warn if unexpected
            if not self.stop_event.is_set():
                logger.error(f"Kafka receiver loop failed for topic {self.topic}: {e}")
        finally:
            try:
                self.consumer.close()
            except Exception as e:
                logger.debug(f"Kafka consumer close raised: {e}")

    def start(self) -> None:  # type: ignore[override]
        threading.Thread.start(self)

    def stop(self) -> None:
        self.stop_event.set()
        try:
            # Wake up the poll loop so it can exit promptly
            self.consumer.wakeup()
        except Exception:
            pass

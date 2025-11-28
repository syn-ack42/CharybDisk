import logging
import signal
import threading
from typing import Any, Dict, List

from charybdisk.consumer import KafkaFileConsumer
from charybdisk.logging_setup import configure_logging
from charybdisk.producer import KafkaFileProducer

logger = logging.getLogger('charybdisk.app')


def start_services(config: Dict[str, Any]) -> None:
    kafka_config = config['kafka']
    logging_config = config.get('logging', {})
    configure_logging(logging_config)

    threads: List[threading.Thread] = []
    stop_lock = threading.Lock()
    stopped = False

    def shutdown(signum=None, frame=None) -> None:  # noqa: ARG001
        nonlocal stopped
        with stop_lock:
            if stopped:
                return
            stopped = True

        logger.info("Shutting down CharybDisk services...")
        for thread in threads:
            stop_fn = getattr(thread, 'stop', None)
            if callable(stop_fn):
                stop_fn()

    consumer_cfg = config.get('consumer', {})
    if consumer_cfg.get('enabled'):
        start_from_end = consumer_cfg.get('start_from_end', False)
        default_group_id = kafka_config.get('default_group_id')
        for topic_cfg in consumer_cfg.get('topics', []):
            consumer = KafkaFileConsumer(kafka_config, topic_cfg, start_from_end, default_group_id)
            consumer.start()
            threads.append(consumer)

    producer_cfg = config.get('producer', {})
    if producer_cfg.get('enabled'):
        producer = KafkaFileProducer(producer_cfg, kafka_config)
        producer.start()
        threads.append(producer)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    if not threads:
        logger.warning("No producer or consumer threads started; exiting.")
        return

    logger.info("CharybDisk services started.")
    try:
        for thread in threads:
            thread.join()
    except KeyboardInterrupt:
        shutdown()
        for thread in threads:
            thread.join()

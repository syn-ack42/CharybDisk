import logging
import signal
import threading
from typing import Any, Dict, List, Optional

from charybdisk.consumer import FileConsumerGroup
from charybdisk.logging_setup import configure_logging
from charybdisk.producer import FileProducer

logger = logging.getLogger('charybdisk.app')


def start_services(config: Dict[str, Any], stop_event: Optional[threading.Event] = None) -> None:
    kafka_config = config.get('kafka', {})
    logging_config = config.get('logging', {})
    configure_logging(logging_config)

    threads: List[threading.Thread] = []
    stop_lock = threading.Lock()
    shutdown_event = threading.Event()
    external_stop_event = stop_event

    def _signal_name(signum: Optional[int]) -> str:
        if signum is None:
            return ""
        try:
            return signal.Signals(signum).name
        except Exception:
            return str(signum)

    def shutdown(signum=None, frame=None) -> None:  # noqa: ARG001
        with stop_lock:
            if shutdown_event.is_set():
                return
            shutdown_event.set()

        sig_name = _signal_name(signum)
        if sig_name:
            logger.info("Shutting down CharybDisk services (signal: %s)...", sig_name)
        else:
            logger.info("Shutting down CharybDisk services...")
        for thread in threads:
            stop_fn = getattr(thread, 'stop', None)
            if callable(stop_fn):
                try:
                    stop_fn()
                except Exception as e:
                    logger.error("Error while stopping thread %s: %s", thread.name, e)

    def wait_for_threads() -> None:
        while True:
            if external_stop_event and external_stop_event.is_set():
                shutdown()
            alive = False
            for thread in threads:
                if thread.is_alive():
                    alive = True
                    # Short timeout keeps Python's signal handling responsive on Windows.
                    thread.join(timeout=0.5)
            if not alive:
                break

    # Register signal handlers (including SIGBREAK for Windows consoles/WinSW).
    handled_signals = [signal.SIGINT, signal.SIGTERM]
    if hasattr(signal, 'SIGBREAK'):
        handled_signals.append(signal.SIGBREAK)  # type: ignore[attr-defined]
    for sig in handled_signals:
        try:
            signal.signal(sig, shutdown)
        except ValueError:
            logger.warning("Could not register signal handler for %s (not in main thread)", _signal_name(sig))

    try:
        consumer_cfg = config.get('consumer', {})
        if consumer_cfg.get('enabled'):
            consumer_group = FileConsumerGroup(consumer_cfg, kafka_config)
            consumer_group.start()
            threads.append(consumer_group)

        producer_cfg = config.get('producer', {})
        if producer_cfg.get('enabled'):
            producer = FileProducer(producer_cfg, kafka_config)
            producer.start()
            threads.append(producer)

        if not threads:
            logger.warning("No producer or consumer threads started; exiting.")
            return

        logger.info("CharybDisk services started.")
        wait_for_threads()
    except KeyboardInterrupt:
        shutdown(signal.SIGINT)
        wait_for_threads()

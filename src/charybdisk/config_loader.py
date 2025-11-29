import os
from typing import Any, Dict

import yaml


class ConfigError(Exception):
    """Raised when configuration is missing required data."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ConfigError(message)


def _needs_kafka(config: Dict[str, Any]) -> bool:
    producer_cfg = config.get('producer', {})
    consumer_cfg = config.get('consumer', {})
    if producer_cfg.get('enabled'):
        for directory in producer_cfg.get('directories', []):
            if directory.get('transport') == 'kafka':
                return True
    if consumer_cfg.get('enabled'):
        for topic_cfg in consumer_cfg.get('topics', []):
            if topic_cfg.get('transport', 'kafka') == 'kafka':
                return True
    return False


def _require_destination(entry: Dict[str, Any], message: str) -> None:
    dest = entry.get('destination') or entry.get('topic') or entry.get('url') or entry.get('endpoint')
    _require(bool(dest), message)


def _require_transport(entry: Dict[str, Any]) -> None:
    transport = entry.get('transport')
    _require(transport in ('kafka', 'http'), "Each entry requires transport: 'kafka' or 'http'")


def validate_config(config: Dict[str, Any]) -> None:
    _require('logging' in config, "Configuration must include 'logging'")

    if _needs_kafka(config):
        kafka_cfg = config.get('kafka', {})
        _require('broker' in kafka_cfg, "Kafka configuration is missing 'broker'")

    producer_cfg = config.get('producer', {})
    consumer_cfg = config.get('consumer', {})
    _require(
        producer_cfg.get('enabled') or consumer_cfg.get('enabled'),
        "Enable at least one of producer or consumer",
    )

    if producer_cfg.get('enabled'):
        directories = producer_cfg.get('directories', [])
        _require(isinstance(directories, list) and directories, "Producer requires non-empty 'directories' list")
        for directory in directories:
            _require('path' in directory, "Each producer directory requires 'path'")
            _require('id' in directory, "Each producer directory requires 'id'")
            _require_transport(directory)
            _require_destination(directory, "Each producer directory requires 'topic' (Kafka) or 'url' (HTTP)")
            if directory.get('transport') == 'kafka':
                _require('topic' in directory, "Kafka producer directory requires 'topic'")
            if directory.get('transport') == 'http':
                _require(any(k in directory for k in ('destination', 'url', 'endpoint')), "HTTP producer directory requires 'destination'/'url'")

    if consumer_cfg.get('enabled'):
        topics = consumer_cfg.get('topics', [])
        _require(isinstance(topics, list) and topics, "Consumer requires non-empty 'topics' list")
        for topic_cfg in topics:
            _require('output_directory' in topic_cfg, "Each consumer topic requires 'output_directory'")
            _require_transport(topic_cfg)
            _require_destination(topic_cfg, "Each consumer topic requires 'topic' (Kafka) or 'url' (HTTP)")
            if topic_cfg.get('transport') == 'kafka':
                _require('topic' in topic_cfg, "Kafka consumer entry requires 'topic'")
            if topic_cfg.get('transport') == 'http':
                _require(any(k in topic_cfg for k in ('url', 'endpoint')), "HTTP consumer entry requires 'url'")


def load_config(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Configuration file '{path}' not found")

    with open(path, 'r') as f:
        data = yaml.safe_load(f) or {}

    if not isinstance(data, dict):
        raise ConfigError("Configuration root must be a mapping")

    validate_config(data)
    return data

import os
from typing import Any, Dict

import yaml


class ConfigError(Exception):
    """Raised when configuration is missing required data."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ConfigError(message)


def validate_config(config: Dict[str, Any]) -> None:
    _require('kafka' in config, "Configuration must include 'kafka'")
    _require('logging' in config, "Configuration must include 'logging'")

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
            _require('topic' in directory, "Each producer directory requires 'topic'")
            _require('id' in directory, "Each producer directory requires 'id'")

    if consumer_cfg.get('enabled'):
        topics = consumer_cfg.get('topics', [])
        _require(isinstance(topics, list) and topics, "Consumer requires non-empty 'topics' list")
        for topic_cfg in topics:
            _require('topic' in topic_cfg, "Each consumer topic requires 'topic'")
            _require('output_directory' in topic_cfg, "Each consumer topic requires 'output_directory'")


def load_config(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Configuration file '{path}' not found")

    with open(path, 'r') as f:
        data = yaml.safe_load(f) or {}

    if not isinstance(data, dict):
        raise ConfigError("Configuration root must be a mapping")

    validate_config(data)
    return data


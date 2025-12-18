from typing import Any, Dict


def build_kafka_client_config(kafka_conf: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build base Kafka client configuration including optional SASL settings.
    """
    config: Dict[str, Any] = {
        'bootstrap_servers': kafka_conf['broker'],
        'client_id': kafka_conf.get('client_id'),
        'api_version_auto_timeout_ms': 10000,
    }

    if kafka_conf.get('sasl_plain_username') is not None:
        config.update({
            'security_protocol': 'SASL_SSL',
            'sasl_mechanism': 'PLAIN',
            'sasl_plain_username': kafka_conf.get('sasl_plain_username'),
            'sasl_plain_password': kafka_conf.get('sasl_plain_password'),
        })

    return config


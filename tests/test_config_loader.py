import pytest

from charybdisk.config_loader import ConfigError, load_config, validate_config


def test_validate_requires_transport():
    config = {
        "logging": {},
        "producer": {
            "enabled": True,
            "directories": [
                {"id": "x", "path": "/tmp", "topic": "t1"}  # missing transport
            ],
        },
        "consumer": {"enabled": False},
    }
    with pytest.raises(ConfigError):
        validate_config(config)


def test_http_only_config_does_not_require_kafka(tmp_path):
    cfg_path = tmp_path / "cfg.yaml"
    cfg_path.write_text(
        """
logging: {}
producer:
  enabled: true
  http: {}
  directories:
    - id: httpdir
      path: /tmp
      transport: http
      destination: http://example.com/upload
consumer:
  enabled: false
"""
    )
    loaded = load_config(str(cfg_path))
    assert loaded["producer"]["directories"][0]["transport"] == "http"

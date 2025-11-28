# CharybDisk
Gobble files from disk, whirl them around, spit them out.

Unified producer/consumer that watches directories, sends files to Kafka topics, and can simultaneously receive from Kafka and write to disk.

## Setup
- Create a virtualenv (recommended) and install deps: `pip install -r requirements.txt`
- Copy `config.example.yaml` to your own config and adjust paths/topics/logging.

## Running
```
python main.py --config path/to/your_config.yaml
```
- Toggle roles via config: set `producer.enabled` and/or `consumer.enabled`.
- Logging goes to console and optional file (no Kafka log sink).

## Configuration Highlights
- `kafka`: `broker`, optional `client_id`, SASL credentials.
- `producer`: `enabled`, `scan_interval`, and `directories` list (`id`, `path`, `topic`, optional `backup_directory`, `file_pattern`).
- `consumer`: `enabled`, `start_from_end`, and `topics` list (`topic`, `output_directory`, optional `group_id`, `output_suffix`).
- `logging`: `console_log_level`, `file_log_level`, optional `log_file`.

See `config.example.yaml` for a complete template using the legacy directories/topics.

# CharybDisk
Gobble files from disk, whirl them around, spit them out.

Unified producer/consumer that watches directories, sends files via transports (Kafka, HTTP), and can simultaneously receive and write to disk.

## Setup
- Create a virtualenv (recommended) and install deps: `pip install -r requirements.txt`
- Copy `config.example.yaml` to your own config and adjust paths/topics/logging.

## Running
```
python main.py --config path/to/your_config.yaml
```
- Toggle roles via config: set `producer.enabled` and/or `consumer.enabled`. Each producer directory and consumer topic explicitly declares `transport: kafka` or `transport: http`.
- Logging goes to console and optional file (no Kafka log sink).

## Configuration Highlights
- `kafka`: `broker`, optional `client_id`, SASL credentials (only needed if any transport is Kafka), optional `max_transfer_file_size`.
- `producer`: `enabled`, optional `scan_interval`, per-directory config: `id`, `path`, `transport` (`kafka` uses `topic`; `http` uses `destination`/`url`), optional `backup_directory`, `file_pattern`, `headers` (merged with `producer.http.default_headers` for HTTP sends).
- `consumer`: `enabled`, per-topic config: `transport` (`kafka` uses `topic`/`group_id`; `http` uses `url`), `output_directory`, optional `output_suffix`, `headers` (merged with `consumer.http.default_headers` for HTTP polls); `start_from_end` applies to Kafka.
- `logging`: `console_log_level`, `file_log_level`, optional `log_file`.
- HTTP extras: under `producer.http`/`consumer.http` you can set `default_headers`, `timeout`, `poll_interval` (consumer), and optional `max_transfer_file_size` for HTTP sends (unset means no cap).

See `config.example.yaml` for a complete template using the legacy directories/topics plus sample HTTP entries.

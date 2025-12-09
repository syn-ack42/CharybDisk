# CharybDisk
Gobble files from disk, whirl them around, spit them out.

Unified producer/consumer that watches directories, sends files via transports (Kafka, HTTP), and can simultaneously receive and write to disk.

## Setup
- Create a virtualenv (recommended) and install deps: `pip install -r requirements.txt`
- Copy `config.example.yaml` to your own config and adjust paths/topics/logging.

## Running
- Install deps (or use a venv): `pip install -r requirements.txt`
- Run via module (preferred): `python -m charybdisk --config path/to/your_config.yaml`
- Or use the thin wrapper: `python main.py --config path/to/your_config.yaml`
- Toggle roles via config: set `producer.enabled` and/or `consumer.enabled`. Each producer directory and consumer topic explicitly declares `transport: kafka` or `transport: http`.
- Logging goes to console and optional file (no Kafka log sink).

## Configuration Highlights
- `kafka`: `broker`, optional `client_id`, SASL credentials (only needed if any transport is Kafka), optional `max_transfer_file_size`.
- `producer`: `enabled`, optional `scan_interval`, per-directory config: `id`, `path`, `transport` (`kafka` uses `topic`; `http` uses `destination`/`url`), optional `file_pattern`, `headers` (merged with `producer.http.default_headers` for HTTP sends). File handling: files are first moved into a pending directory (default `pending` under `path`, configurable via `pending_directory`; relative paths are relative to `path`). Successful sends always move to backup (default `done` under `path`, configurable via `backup_directory`, relative allowed). Failures move to error (default `error` under `path`, configurable via `error_directory`, relative allowed). Pending files from a previous crash are retried on the next scan.
- `consumer`: `enabled`, per-topic config: `transport` (`kafka` uses `topic`/`group_id`; `http` uses `url`), `output_directory`, optional `output_suffix`, `headers` (merged with `consumer.http.default_headers` for HTTP polls); `start_from_end` applies to Kafka. File handling: incoming files are staged in an incoming directory (default `incoming` under `output_directory`, configurable via `incoming_directory`; relative paths are relative to `output_directory`) with a temp suffix (default `.tmp`, configurable via `temp_suffix`), then atomically renamed to the final name. Kafka chunk assembly directory is configurable via `working_directory`; if unset and Kafka is used, it defaults to `charybdisk_parts` under the `output_directory`.
- `logging`: `console_log_level`, `file_log_level`, optional `log_file`.
- HTTP extras: under `producer.http`/`consumer.http` you can set `default_headers`, `timeout`, `poll_interval` (consumer), and optional `max_transfer_file_size` for HTTP sends (unset means no cap).

See `config.example.yaml` for a complete template using the legacy directories/topics plus sample HTTP entries.

## File Handling Safety
- Outbound (producer): files are moved into a per-directory pending area before sending to avoid reading in-progress writes; after success they are moved to a backup directory; after failure they go to an error directory. Relative paths for pending/backup/error are resolved under the source `path`; defaults are `pending`, `done`, and `error`.
- Inbound (consumer): received content is written as `incoming/<name>.tmp` (paths relative to `output_directory` by default) and then atomically renamed to the final file. If a file with the same name exists and differs, a timestamped variant is written; identical content is skipped. Kafka assemblies use a working directory (default `output_directory/charybdisk_parts` when Kafka is enabled) to build chunks before the final write.

## Testing
- Unit tests: `pytest`
- Integration tests (require Docker, http/kafka test stacks): `RUN_INTEGRATION=1 pytest tests/integration/test_end_to_end.py -v`
  - Defaults: runtime 60s, restart every 20s; override via `INTEGRATION_RUNTIME` and `INTEGRATION_RESTART_EVERY`.

## Windows Service + Installer (WiX + WinSW)
- Build exe on Windows with PyInstaller: `pyinstaller --name charybdisk --onefile -m charybdisk` (output in `dist\charybdisk.exe`).
- Download WinSW (x64) and rename to `charybdisk-service.exe`. Copy `tools/windows/winsw-example.xml` next to it, rename to `charybdisk-service.xml`, and edit `<executable>` (point to `charybdisk.exe`) and `<arguments>` (point to your external `config.yaml`, e.g., `C:\ProgramData\CharybDisk\config.yaml`).
- Gather build payloads into a folder (e.g., `C:\charybdisk-build`): `charybdisk.exe`, `charybdisk-service.exe`, `charybdisk-service.xml`, `config.example.yaml`.
- Build MSI with WiX v4 installed/on PATH: `wix build -dBuildOutput="C:\charybdisk-build" tools/windows/charybdisk.wxs`
- Install MSI (elevated). It copies binaries to `Program Files\CharybDisk`, sample config to `ProgramData\CharybDisk`, and runs `charybdisk-service.exe install`. Put your real `config.yaml` at the path referenced in the WinSW XML.

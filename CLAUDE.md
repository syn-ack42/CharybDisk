# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Run the application
python -m charybdisk --config path/to/config.yaml

# Install dependencies
pip install -r requirements.txt

# Run unit tests
pytest

# Run a single test file
pytest tests/test_messages.py -v

# Run integration tests (requires Docker with Kafka and HTTP test server running)
RUN_INTEGRATION=1 pytest tests/integration/test_end_to_end.py -v

# Build Windows executable
pyinstaller --name charybdisk -F main.py

# Build Windows service executable
pyinstaller charybdisk_win_service.spec
```

## Architecture

CharybDisk transfers files between systems via Kafka or HTTP. A single process can run as producer, consumer, or both simultaneously.

**Data flow (producer):** Source directory → pending dir → transport (Kafka/HTTP) → backup dir (success) or error dir (failure)

**Data flow (consumer):** Transport → `.tmp` file → atomic rename → output directory (duplicate handling with timestamp suffix)

### Key modules

- **`app.py`** — Orchestrates producer/consumer threads, signal handling, graceful shutdown
- **`config_loader.py`** — Loads and validates YAML config; validates per-transport requirements
- **`producer.py`** — Watches directories, moves files to pending, sends via transport, handles retries
- **`consumer.py`** — Polls transport, assembles chunked files, writes to disk
- **`messages.py`** — `FileMessage` dataclass: JSON envelope with base64 content, chunk index/total
- **`file_preparer.py`** — Validates files before sending (lock check, size limits, empty file rejection)
- **`file_writer.py`** — Atomic writes (temp suffix → rename), duplicate detection
- **`transports/base.py`** — `Transport` and `Receiver` ABCs that Kafka/HTTP implementations extend

### Transport implementations

- **`transports/kafka_transport.py`** — Chunked send/receive; SASL auth; auto topic creation; configurable max message bytes and chunk size (default 60% of max)
- **`transports/http_transport.py`** — POST for upload, GET polling for download; per-entry header overrides

### Windows service

`charybdisk_win_service.py` wraps the app for Windows Service Control Manager. Install with `charybdisk_win_service.exe install`. Config path read from registry or `-c` CLI flag. Built via `charybdisk_win_service.spec` (PyInstaller).

## Configuration

See `config.example.yaml` for full reference and `config.test.yaml` for test setup. Key sections: `kafka`, `producer` (list of directories), `consumer` (list of topics), `http`, `logging`.

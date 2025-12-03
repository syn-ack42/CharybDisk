import base64
import os
import random
import shutil
import string
import subprocess
import tempfile
import threading
import time
import uuid
from pathlib import Path

from charybdisk.transports.http_transport import sanitize_header_filename

import pytest
import requests
from kafka import KafkaAdminClient


RUN_INTEGRATION = os.environ.get("RUN_INTEGRATION") == "1"
HTTP_COMPOSE_DIR = Path("tools/http-test-server")
KAFKA_COMPOSE_DIR = Path("tools/kafka-test")


pytestmark = pytest.mark.skipif(
    not RUN_INTEGRATION, reason="Set RUN_INTEGRATION=1 to run integration tests (requires Docker)."
)


def _run(cmd, cwd=None):
    print(f"[integration] run: {' '.join(cmd)} cwd={cwd}")
    subprocess.run(cmd, cwd=cwd, check=True)


def _compose_up(path: Path):
    _run(["docker", "compose", "up", "-d", "--remove-orphans"], cwd=path)


def _compose_down(path: Path):
    _run(["docker", "compose", "down", "--remove-orphans"], cwd=path)


def _dump_tree(path: Path) -> str:
    out = []
    if not path.exists():
        return f"<dir does not exist: {path}>"

    for root, dirs, files in os.walk(path):
        root_path = Path(root)
        rel = root_path.relative_to(path)
        indent = "  " * len(rel.parts)
        out.append(f"{indent}{root}:")
        for d in dirs:
            out.append(f"{indent}  [D] {d}")
        for f in files:
            out.append(f"{indent}  [F] {f}")
    return "\n".join(out)


def _dump_threads() -> str:
    lines = []
    for t in threading.enumerate():
        lines.append(
            f"Thread(name={t.name!r}, alive={t.is_alive()}, daemon={t.daemon})"
        )
    return "\n".join(lines)


def _rand_name(prefix="file", ext="bin"):
    chars = string.ascii_letters + string.digits + "._-äöüß"
    stem = "".join(random.choice(chars) for _ in range(8))
    return f"{prefix}_{stem}.{ext}"


def _write_random_file(path: Path, size: int):
    path.write_bytes(os.urandom(size))


def _wait_for_file(path: Path, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if path.exists():
            return True
        time.sleep(0.5)
    return False


def _wait_for_http_health(url: str, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.get(url, timeout=3)
            if r.ok:
                return True
        except Exception:
            time.sleep(1)
            continue
    return False


def _wait_for_kafka(broker: str, timeout=30):
    deadline = time.time() + timeout
    last_err = None
    while time.time() < deadline:
        try:
            admin = KafkaAdminClient(bootstrap_servers=broker)
            admin.list_topics()
            admin.close()
            return True, None
        except Exception as e:
            last_err = str(e)
            time.sleep(1)
    print(f"[integration] kafka not ready: {last_err}")
    return False, last_err


def _make_config(tmpdir: Path) -> Path:
    upload_http = tmpdir / "http_upload"
    download_http = tmpdir / "http_download"
    upload_kafka = tmpdir / "kafka_upload"
    download_kafka = tmpdir / "kafka_download"
    error_http = tmpdir / "http_error"
    error_kafka = tmpdir / "kafka_error"
    parts_dir = tmpdir / "parts"

    for p in [upload_http, download_http, upload_kafka, download_kafka, error_http, error_kafka, parts_dir]:
        p.mkdir(parents=True, exist_ok=True)

    basic = base64.b64encode(b"user:pass").decode()
    config = f"""
kafka:
  broker: 127.0.0.1:29092
  client_id: kafka_test_node
  default_group_id: kafka_test_group
  max_message_bytes: 1000000

logging:
  console_log_level: info
  file_log_level: debug
  log_file: {tmpdir}/integration.log

producer:
  enabled: true
  chunk_size_bytes: 600000
  directories:
    - id: HTTP_OK
      transport: http
      path: "{upload_http}"
      backup_directory: "{upload_http}/done"
      file_pattern: "*"
      destination: "http://localhost:8080/upload"
      headers:
        Authorization: "Basic {basic}"
    - id: HTTP_BAD
      transport: http
      path: "{error_http}"
      error_directory: "{error_http}/error"
      destination: "http://localhost:8081/bad"
    - id: KAFKA_OK
      transport: kafka
      path: "{upload_kafka}"
      backup_directory: "{upload_kafka}/done"
      error_directory: "{error_kafka}"
      file_pattern: "*"
      topic: "charybdisk.test.files"

consumer:
  enabled: true
  http:
    poll_interval: 1
    timeout: 10
    default_headers:
      Authorization: "Bearer secret-token"
  working_directory: "{parts_dir}"
  start_from_end: false
  topics:
    - transport: http
      url: "http://localhost:8080/download"
      output_directory: "{download_http}"
    - transport: kafka
      topic: "charybdisk.test.files"
      output_directory: "{download_kafka}"
"""
    cfg_path = tmpdir / "config.yaml"
    cfg_path.write_text(config)
    return cfg_path, upload_http, download_http, upload_kafka, download_kafka, error_http, error_kafka


@pytest.fixture()
def app_process(tmp_path_factory):
    tmpdir = tmp_path_factory.mktemp("integration")
    cfg, upload_http, download_http, upload_kafka, download_kafka, error_http, error_kafka = _make_config(tmpdir)

    ok, reason = _wait_for_kafka("127.0.0.1:29092", timeout=60)
    if not ok:
        pytest.skip(f"Kafka test broker not reachable before app start: {reason}")

    logs_dir = Path("integration_logs")
    logs_dir.mkdir(exist_ok=True)
    log_file = logs_dir / f"app_stdout_{uuid.uuid4().hex}.log"
    log_fh = open(log_file, "wb")

    env = os.environ.copy()
    src_path = str(Path(__file__).resolve().parents[2] / "src")
    env["PYTHONPATH"] = src_path + os.pathsep + env.get("PYTHONPATH", "")
    env["PYTHONUNBUFFERED"] = "1"

    print("START CHARYBDISK")
    proc = subprocess.Popen(
        ["python", "-u", "-m", "charybdisk", "--config", str(cfg)],
        stdout=log_fh,
        stderr=subprocess.STDOUT,
        bufsize=1,
        env=env,
    )
    print(f"STARTED {proc} log={log_file}")

    time.sleep(3)

    # Early check for crashed watcher threads
    thread_dump = _dump_threads()
    print("EARLY THREAD STATE:\n" + thread_dump)

    integ_log = Path(cfg).with_name("integration.log")

    yield {
        "proc": proc,
        "cfg": cfg,
        "dirs": {
            "upload_http": upload_http,
            "download_http": download_http,
            "upload_kafka": upload_kafka,
            "download_kafka": download_kafka,
            "error_http": error_http,
            "error_kafka": error_kafka,
        },
        "log_file": log_file,
        "integration_log": integ_log,
    }

    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()

    log_fh.close()

    if integ_log.exists():
        shutil.copy(integ_log, log_file.parent / f"integration_{log_file.name}")

    shutil.rmtree(tmpdir, ignore_errors=True)


def test_end_to_end_http_and_kafka(app_process):
    dirs = app_process["dirs"]
    proc = app_process["proc"]

    if proc.poll() is not None:
        log_tail = Path(app_process["log_file"]).read_text().splitlines()[-200:]
        pytest.fail("App exited before test start:\n" + "\n".join(log_tail))

    small_http = dirs["upload_http"] / _rand_name("http_small", "txt")
    _write_random_file(small_http, 1024)

    large_kafka = dirs["upload_kafka"] / _rand_name("kafka_large", "bin")
    _write_random_file(large_kafka, 800_000)

    bad_http = dirs["error_http"] / _rand_name("http_bad", "txt")
    _write_random_file(bad_http, 2048)

    sanitized, _ = sanitize_header_filename(small_http.name)

    found_http = (
        _wait_for_file(dirs["download_http"] / small_http.name, timeout=40)
        or _wait_for_file(dirs["download_http"] / sanitized, timeout=5)
    )

    if not found_http:
        log_file = app_process["log_file"]
        integ_log = app_process.get("integration_log")

        try:
            log_tail = Path(log_file).read_text().splitlines()[-200:]
        except Exception:
            log_tail = ["<unable to read log>"]

        integ_tail = []
        if integ_log and integ_log.exists():
            try:
                integ_tail = integ_log.read_text().splitlines()[-200:]
            except Exception:
                integ_tail = ["<unable to read integration log>"]

        http_up = _dump_tree(dirs["upload_http"])
        http_dl = _dump_tree(dirs["download_http"])
        http_err = _dump_tree(dirs["error_http"])
        threads = _dump_threads()

        pytest.fail(
            "HTTP file not received.\n\n"
            "==== THREADS ====\n" + threads + "\n\n"
            "==== UPLOAD_HTTP TREE ====\n" + http_up + "\n\n"
            "==== DOWNLOAD_HTTP TREE ====\n" + http_dl + "\n\n"
            "==== ERROR_HTTP TREE ====\n" + http_err + "\n\n"
            "==== STDOUT LOG (tail) ====\n" + "\n".join(log_tail) + "\n\n"
            "==== INTEGRATION LOG (tail) ====\n" + "\n".join(integ_tail)
        )

    assert _wait_for_file(dirs["download_kafka"] / large_kafka.name, timeout=40)
    assert _wait_for_file(dirs["error_http"] / "error" / bad_http.name, timeout=40)


def test_under_load_with_restarts(app_process):
    dirs = app_process["dirs"]
    runtime = int(os.environ.get("INTEGRATION_RUNTIME", "60"))
    restart_every = int(os.environ.get("INTEGRATION_RESTART_EVERY", "20"))
    end_time = time.time() + runtime

    def restarter(proc):
        while time.time() < end_time and proc.poll() is None:
            time.sleep(restart_every)
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()

            app_process["proc"] = subprocess.Popen(
                ["python", "-u", "-m", "charybdisk", "--config", str(app_process["proc"].args[-1])],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                bufsize=1,
            )

    t = threading.Thread(target=restarter, args=(app_process["proc"],), daemon=True)
    t.start()

    while time.time() < end_time:
        fname = _rand_name("mix", random.choice(["txt", "bin", "json"]))
        size = random.choice([256, 2048, 50_000, 300_000, 700_000])
        target_dir = random.choice([dirs["upload_http"], dirs["upload_kafka"]])
        _write_random_file(target_dir / fname, size)
        time.sleep(0.5)

    t.join()
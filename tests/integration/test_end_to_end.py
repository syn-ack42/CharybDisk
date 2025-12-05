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
import pytest
import requests
from kafka import KafkaAdminClient

from charybdisk.transports.http_transport import sanitize_header_filename

RUN_INTEGRATION = os.environ.get("RUN_INTEGRATION") == "1"
HTTP_COMPOSE_DIR = Path("tools/http-test-server")
KAFKA_COMPOSE_DIR = Path("tools/kafka-test")



pytestmark = pytest.mark.skipif(
    not RUN_INTEGRATION, reason="Set RUN_INTEGRATION=1 to run integration tests (requires Docker)."
)


# ----------------------------------------------------------------------
# EXTENDED DEBUG HELPERS
# ----------------------------------------------------------------------

def _dump_tree(path: Path) -> str:
    if not path.exists():
        return f"<directory {path} does not exist>"
    out = []
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


# ----------------------------------------------------------------------
# CORE HELPERS
# ----------------------------------------------------------------------

def _run(cmd, cwd=None):
    print(f"[integration] run: {' '.join(cmd)} cwd={cwd}")
    subprocess.run(cmd, cwd=cwd, check=True)


def _compose_up(path: Path):
    _run(["docker", "compose", "up", "-d", "--remove-orphans"], cwd=path)


def _compose_down(path: Path):
    _run(["docker", "compose", "down", "--remove-orphans"], cwd=path)


@pytest.fixture(scope="session", autouse=True)
def services():
    _compose_up(HTTP_COMPOSE_DIR)
    _compose_up(KAFKA_COMPOSE_DIR)
    print("[integration] waiting for HTTP test server health...")
    if not _wait_for_http_health("http://localhost:8080/health", timeout=60):
        pytest.fail("HTTP test server not reachable")
    print("[integration] waiting for Kafka broker...")
    ok, reason = _wait_for_kafka("127.0.0.1:29092", timeout=60)
    if not ok:
        pytest.fail(f"Kafka test broker not reachable: {reason}")
    yield
    print("------ ASKED FOR SHUTDOWN------")
    _compose_down(HTTP_COMPOSE_DIR)
    _compose_down(KAFKA_COMPOSE_DIR)


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
    log_file = tmpdir / "integration.log"

    for p in [upload_http, download_http, upload_kafka, download_kafka, error_http, error_kafka, parts_dir]:
        p.mkdir(parents=True, exist_ok=True)

    basic = base64.b64encode(b"user:pass").decode()
    upload_http_s = upload_http.as_posix()
    download_http_s = download_http.as_posix()
    upload_kafka_s = upload_kafka.as_posix()
    download_kafka_s = download_kafka.as_posix()
    error_http_s = error_http.as_posix()
    error_kafka_s = error_kafka.as_posix()
    parts_dir_s = parts_dir.as_posix()
    log_file_s = log_file.as_posix()

    config = f"""
kafka:
  broker: 127.0.0.1:29092
  client_id: kafka_test_node
  default_group_id: kafka_test_group
  max_message_bytes: 1000000

logging:
  console_log_level: info
  file_log_level: debug
  log_file: "{log_file_s}"

producer:
  enabled: true
  scan_interval: 1
  chunk_size_bytes: 600000
  directories:
    - id: HTTP_OK
      transport: http
      path: "{upload_http_s}"
      backup_directory: "{upload_http_s}/done"
      file_pattern: "*"
      destination: "http://localhost:8080/upload"
      headers:
        Authorization: "Basic {basic}"
    - id: HTTP_BAD
      transport: http
      path: "{error_http_s}"
      error_directory: "{error_http_s}/error"
      destination: "http://localhost:8081/bad"
    - id: KAFKA_OK
      transport: kafka
      path: "{upload_kafka_s}"
      backup_directory: "{upload_kafka_s}/done"
      error_directory: "{error_kafka_s}"
      file_pattern: "*"
      topic: "charybdisk.test.files"

consumer:
  enabled: true
  http:
    poll_interval: 1
    timeout: 10
    default_headers:
      Authorization: "Bearer secret-token"
  working_directory: "{parts_dir_s}"
  start_from_end: false
  topics:
    - transport: http
      url: "http://localhost:8080/download"
      output_directory: "{download_http_s}"
    - transport: kafka
      topic: "charybdisk.test.files"
      output_directory: "{download_kafka_s}"
"""
    cfg_path = tmpdir / "config.yaml"
    cfg_path.write_text(config)
    return cfg_path, upload_http, download_http, upload_kafka, download_kafka, error_http, error_kafka


# ----------------------------------------------------------------------
# APP PROCESS FIXTURE
# ----------------------------------------------------------------------

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
    log_fh = open(log_file, "w", buffering=1)

    env = os.environ.copy()
    src_path = str(Path(__file__).resolve().parents[2] / "src")
    env["PYTHONPATH"] = src_path + os.pathsep + env.get("PYTHONPATH", "")
    env["PYTHONUNBUFFERED"] = "1"
    env["PYTHONFAULTHANDLER"] = "1"
    env["PYTHONDEVMODE"] = "1"


    # wait until Kafka is really ready
    ok, reason = _wait_for_kafka("127.0.0.1:29092", timeout=60)
    if not ok:
        pytest.fail("Kafka broker not ready")

    print("START CHARYBDISK")
    proc = subprocess.Popen(
        ["python", "-X", "faulthandler", "-u", "-m", "charybdisk", "--config", str(cfg)],
        stdout=log_fh,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        env=env,
    )
    print(f"STARTED {proc} log={log_file}")

    time.sleep(3)

    # -----------------------------------------------------------
    # EXTENDED DEBUG: CONFIG + DIRECTORY STRUCTURES + THREADS
    # -----------------------------------------------------------
    # print("\n=== DEBUG: CONFIG CONTENT ===")
    # try:
    #     print(Path(cfg).read_text())
    # except Exception as e:
    #     print("FAILED TO READ CONFIG:", e)

    print("\n=== DEBUG: WATCHED DIRECTORIES ===")
    dirs = {
        "upload_http": upload_http,
        "download_http": download_http,
        "upload_kafka": upload_kafka,
        "download_kafka": download_kafka,
        "error_http": error_http,
        "error_kafka": error_kafka,
    }
    for name, p in dirs.items():
        print(f"{name}: exists={p.exists()} path={p}")
        if p.exists():
            try:
                print(" contents:", list(p.iterdir()))
            except Exception as e:
                print(" error listing:", e)

    print("\n=== DEBUG: EARLY THREAD DUMP ===")
    print(_dump_threads())
    print("=== END DEBUG ===\n")

    integ_log = Path(cfg).with_name("integration.log")

    yield {
        "proc": proc,
        "cfg": cfg,
        "dirs": dirs,
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


# ----------------------------------------------------------------------
# MAIN TEST
# ----------------------------------------------------------------------

def test_end_to_end_http_and_kafka(app_process):
    dirs = app_process["dirs"]
    proc = app_process["proc"]

    if proc.poll() is not None:
        log_tail = Path(app_process["log_file"]).read_text().splitlines()[-200:]
        pytest.fail("App exited before test start:\n" + "\n".join(log_tail))

    # HTTP success
    small_http = dirs["upload_http"] / _rand_name("http_small", "txt")
    _write_random_file(small_http, 1024)

    # Kafka success
    large_kafka = dirs["upload_kafka"] / _rand_name("kafka_large", "bin")
    _write_random_file(large_kafka, 800_000)

    # HTTP failure
    bad_http = dirs["error_http"] / _rand_name("http_bad", "txt")
    _write_random_file(bad_http, 2048)

    sanitized, _ = sanitize_header_filename(small_http.name)

    found_http = (
        _wait_for_file(dirs["download_http"] / small_http.name, timeout=40)
        or _wait_for_file(dirs["download_http"] / sanitized, timeout=50)
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

        threads = _dump_threads()
        up_tree = _dump_tree(dirs["upload_http"])
        dl_tree = _dump_tree(dirs["download_http"])
        err_tree = _dump_tree(dirs["error_http"])

        pytest.fail(
            "HTTP file not received.\n\n"
            "==== THREADS ====\n" + threads + "\n\n"
            "==== UPLOAD_HTTP TREE ====\n" + up_tree + "\n\n"
            "==== DOWNLOAD_HTTP TREE ====\n" + dl_tree + "\n\n"
            "==== ERROR_HTTP TREE ====\n" + err_tree + "\n\n"
            "==== STDOUT LOG (tail) ====\n" + "\n".join(log_tail) + "\n\n"
            "==== INTEGRATION LOG (tail) ====\n" + "\n".join(integ_tail)
        )

    # Kafka path
    assert _wait_for_file(dirs["download_kafka"] / large_kafka.name, timeout=120)

    # HTTP failure path
    assert _wait_for_file(dirs["error_http"] / "error" / bad_http.name, timeout=40)


# ----------------------------------------------------------------------
# LOAD TEST WITH RESTARTS
# ----------------------------------------------------------------------

def test_under_load_with_restarts(app_process):
    dirs = app_process["dirs"]
    runtime = int(os.environ.get("INTEGRATION_RUNTIME", "300"))
    restart_every = int(os.environ.get("INTEGRATION_RESTART_EVERY", "30"))
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
                bufsize=1, text=True,
            )

    t = threading.Thread(target=restarter, args=(app_process["proc"],), daemon=False)
    t.start()

    while time.time() < end_time:
        fname = _rand_name("mix", random.choice(["txt", "bin", "json"]))
        size = random.choice([256, 2048, 50_000, 300_000, 700_000])
        target_dir = random.choice([dirs["upload_http"], dirs["upload_kafka"]])
        _write_random_file(target_dir / fname, size)
        time.sleep(0.5)

    t.join()

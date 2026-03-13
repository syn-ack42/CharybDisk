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
HTTP_ONLY = os.environ.get("HTTP_ONLY") == "1"
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
    if not HTTP_ONLY:
        _compose_up(KAFKA_COMPOSE_DIR)
    print("[integration] waiting for HTTP test server health...")
    if not _wait_for_http_health("http://localhost:8080/health", timeout=60):
        pytest.fail("HTTP test server not reachable")
    if not HTTP_ONLY:
        print("[integration] waiting for Kafka broker (admin)...")
        ok, reason = _wait_for_kafka("127.0.0.1:29092", timeout=60)
        if not ok:
            pytest.fail(f"Kafka test broker not reachable: {reason}")
        print("[integration] waiting for Kafka broker (produce-ready)...")
        ok, reason = _wait_for_kafka_produce("127.0.0.1:29092", timeout=60)
        if not ok:
            pytest.fail(f"Kafka test broker not ready for produce: {reason}")
    yield
    print("------ ASKED FOR SHUTDOWN------")
    _compose_down(HTTP_COMPOSE_DIR)
    if not HTTP_ONLY:
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
    """Wait until Kafka accepts admin API calls (broker is up)."""
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


def _wait_for_kafka_produce(broker: str, timeout=30):
    """Wait until Kafka is ready to actually produce messages.

    `list_topics()` can succeed while the broker is still initialising and
    not yet accepting produce requests.  This sends a real message to a
    sentinel topic to confirm the broker is fully operational.
    """
    from kafka import KafkaProducer
    deadline = time.time() + timeout
    last_err = None
    while time.time() < deadline:
        try:
            producer = KafkaProducer(
                bootstrap_servers=broker,
                request_timeout_ms=5000,
                api_version_auto_timeout_ms=3000,
            )
            future = producer.send("__charybdisk_readiness__", b"ping")
            future.get(timeout=5)
            producer.close()
            return True, None
        except Exception as e:
            last_err = str(e)
            time.sleep(1)
    print(f"[integration] kafka produce not ready: {last_err}")
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

    dirs_to_create = [upload_http, download_http, error_http, parts_dir]
    if not HTTP_ONLY:
        dirs_to_create += [upload_kafka, download_kafka, error_kafka]
    for p in dirs_to_create:
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

    kafka_section = "" if HTTP_ONLY else f"""
kafka:
  broker: 127.0.0.1:29092
  client_id: kafka_test_node
  default_group_id: kafka_test_group
  max_message_bytes: 1000000
"""

    kafka_producer_entry = "" if HTTP_ONLY else f"""
    - id: KAFKA_OK
      transport: kafka
      path: "{upload_kafka_s}"
      backup_directory: "{upload_kafka_s}/done"
      error_directory: "{error_kafka_s}"
      file_pattern: "*"
      topic: "charybdisk.test.files"
"""

    kafka_consumer_entry = "" if HTTP_ONLY else f"""
    - transport: kafka
      topic: "charybdisk.test.files"
      output_directory: "{download_kafka_s}"
"""

    config = f"""{kafka_section}
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
{kafka_producer_entry}
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
{kafka_consumer_entry}
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

    if not HTTP_ONLY:
        ok, reason = _wait_for_kafka("127.0.0.1:29092", timeout=60)
        if not ok:
            pytest.skip(f"Kafka test broker not reachable before app start: {reason}")

    # Clear HTTP server state from any previous test run so leftover files
    # in the download queue don't confuse this test's file-count accounting.
    try:
        r = requests.delete("http://localhost:8080/admin/clear", timeout=10)
        print(f"[integration] HTTP server cleared: {r.json()}")
    except Exception as e:
        print(f"[integration] WARNING: could not clear HTTP server state: {e}")

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

    if not HTTP_ONLY:
        # Confirm Kafka is still ready for produce (not just admin) before starting the app.
        ok, reason = _wait_for_kafka_produce("127.0.0.1:29092", timeout=30)
        if not ok:
            pytest.fail(f"Kafka broker not produce-ready before app start: {reason}")

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
        "error_http": error_http,
    }
    if not HTTP_ONLY:
        dirs.update({
            "upload_kafka": upload_kafka,
            "download_kafka": download_kafka,
            "error_kafka": error_kafka,
        })
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
        "logs_dir": logs_dir,
        "env": env,
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

    if not HTTP_ONLY:
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
        pytest.fail("HTTP file not received.")

    if not HTTP_ONLY:
        # Kafka path
        assert _wait_for_file(dirs["download_kafka"] / large_kafka.name, timeout=120)

    # HTTP failure path: connection refused is a transient error, so CharybDisk must
    # keep the file in pending/ for retry rather than permanently discarding it to error/.
    pending_path = dirs["error_http"] / "pending" / bad_http.name
    assert _wait_for_file(pending_path, timeout=15), (
        "File for an unreachable endpoint must be moved to pending/ so it is retried"
    )
    error_path = dirs["error_http"] / "error" / bad_http.name
    assert not error_path.exists(), (
        "A transient connection error must NOT move the file to error/"
    )


# ----------------------------------------------------------------------
# LOAD TEST WITH RESTARTS
# ----------------------------------------------------------------------

def test_under_load_with_restarts(app_process):
    dirs = app_process["dirs"]
    runtime = int(os.environ.get("INTEGRATION_RUNTIME", "300"))
    restart_every = int(os.environ.get("INTEGRATION_RESTART_EVERY", "40"))
    end_time = time.time() + runtime
    sent_files = []
    sent_http = 0
    sent_kafka = 0

    logs_dir = app_process["logs_dir"]
    env = app_process["env"]
    restart_count = [0]

    def restarter():
        # Always restart the *current* process, not the original one.
        # Use a dedicated log file per restart so stdout never goes to a PIPE,
        # which can deadlock once the OS pipe buffer fills up.
        while time.time() < end_time:
            time.sleep(restart_every)
            if time.time() >= end_time:
                break
            current = app_process["proc"]
            if current.poll() is not None:
                print("[integration] CharybDisk exited unexpectedly; stopping restarter")
                break
            config_path = current.args[-1]
            current.terminate()
            try:
                current.wait(timeout=5)
            except subprocess.TimeoutExpired:
                current.kill()
                current.wait()
            restart_count[0] += 1
            restart_log = logs_dir / f"restart_{restart_count[0]}_{uuid.uuid4().hex}.log"
            restart_fh = open(restart_log, "w", buffering=1)
            app_process["proc"] = subprocess.Popen(
                ["python", "-u", "-m", "charybdisk", "--config", config_path],
                stdout=restart_fh,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                env=env,
            )
            print(f"[integration] CharybDisk restarted (#{restart_count[0]}), log={restart_log}")

    t = threading.Thread(target=restarter, daemon=False)
    t.start()

    upload_choices = [dirs["upload_http"]]
    if not HTTP_ONLY:
        upload_choices.append(dirs["upload_kafka"])

    while time.time() < end_time:
        fname = _rand_name("mix", random.choice(["txt", "bin", "json"]))
        size = random.choice([256, 2048, 50_000, 300_000, 700_000])
        target_dir = random.choice(upload_choices)
        dest_dir = dirs["download_http"] if target_dir == dirs["upload_http"] else dirs["download_kafka"]
        if target_dir == dirs["upload_http"]:
            sent_http += 1
        else:
            sent_kafka += 1
        file_path = target_dir / fname
        _write_random_file(file_path, size)
        sent_files.append((file_path.name, size, dest_dir, target_dir))
        total_sent = sent_http + sent_kafka
        if total_sent % 10 == 0:
            print(f"[integration] sent so far: http={sent_http}, kafka={sent_kafka}")
        time.sleep(0.5)

    t.join()

    # Allow up to 2 minutes after sending stops for all files to arrive
    deadline = time.time() + 120
    pending = list(sent_files)
    while pending and time.time() < deadline:
        pending = [
            (name, size, dest, source)
            for (name, size, dest, source) in pending
            if not (dest / name).exists()
        ]
        received_http = sum(1 for (name, _, dest, _) in sent_files if dest == dirs["download_http"] and (dest / name).exists())
        received_kafka = sum(1 for (name, _, dest, _) in sent_files if dest == dirs["download_kafka"] and (dest / name).exists())
        total_received = received_http + received_kafka
        if total_received > 0 and total_received % 10 == 0:
            print(
                f"[integration] received so far: http={received_http}/{sent_http}, "
                f"kafka={received_kafka}/{sent_kafka}"
            )
        if pending:
            time.sleep(1)

    missing = [(name, dest) for (name, _, dest, _) in pending]
    if missing:
        http_missing = sum(1 for (_, dest) in missing if dest == dirs["download_http"])
        kafka_missing = sum(1 for (_, dest) in missing if dest == dirs["download_kafka"])
        sample = ", ".join(f"{n}->{dest}" for n, dest in missing[:10])
        pytest.fail(
            f"{len(missing)} files not received back within timeout "
            f"(http missing={http_missing}, kafka missing={kafka_missing}). "
            f"Sent http={sent_http}, kafka={sent_kafka}. Sample: {sample}"
        )

    received_http = sum(1 for (name, _, dest, _) in sent_files if dest == dirs["download_http"] and (dest / name).exists())
    received_kafka = sum(1 for (name, _, dest, _) in sent_files if dest == dirs["download_kafka"] and (dest / name).exists())

    error_http_dir = dirs["error_http"] / "error"
    errors_found = []
    for name, _, dest, source in sent_files:
        if dest == dirs["download_http"]:
            err_path = error_http_dir / name
        elif not HTTP_ONLY:
            err_path = dirs["error_kafka"] / name
        else:
            continue
        if err_path.exists():
            errors_found.append(err_path)

    if errors_found:
        pytest.fail(
            f"Files landed in error directories: {len(errors_found)} (e.g., {errors_found[:5]}) "
            f"(sent http={sent_http}, kafka={sent_kafka}, received http={received_http}, kafka={received_kafka})"
        )

    assert received_http == sent_http and received_kafka == sent_kafka, (
        f"Mismatch counts: sent http={sent_http}, kafka={sent_kafka}; "
        f"received http={received_http}, kafka={received_kafka}"
    )

import base64
import json
import logging
import os
from functools import wraps
from pathlib import Path
from typing import Dict, Optional, Tuple

import yaml
from flask import Flask, jsonify, make_response, request, send_file

app = Flask(__name__)

logger = logging.getLogger("http-test-server")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def load_config() -> Dict:
    config_path = os.environ.get("HTTP_TEST_CONFIG", "config.yaml")
    if os.path.isdir(config_path):
        candidate_yaml = os.path.join(config_path, "config.yaml")
        candidate_json = os.path.join(config_path, "config.json")
        if os.path.exists(candidate_yaml):
            config_path = candidate_yaml
        elif os.path.exists(candidate_json):
            config_path = candidate_json
        else:
            raise FileNotFoundError(f"Config directory '{config_path}' does not contain config.yaml or config.json")

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found at {config_path}")

    with open(config_path, "r") as f:
        if config_path.endswith(".json"):
            return json.load(f)
        return yaml.safe_load(f)


CONFIG = load_config()


def ensure_dir(path: str) -> None:
    Path(path).mkdir(parents=True, exist_ok=True)


def log_request(resp_status: int, payload_preview: Optional[str] = None) -> None:
    headers = {k: v for k, v in request.headers.items()}
    preview = payload_preview or ""
    preview = preview[:50]
    logger.info(
        json.dumps(
            {
                "method": request.method,
                "path": request.path,
                "status": resp_status,
                "headers": headers,
                "payload_preview": preview,
            }
        )
    )


def unauthorized_response() -> Tuple:
    response = make_response(jsonify({"error": "unauthorized"}), 401)
    response.headers["WWW-Authenticate"] = 'Basic realm="Auth required"'
    return response


def bearer_token_from_header(auth_header: str) -> Optional[str]:
    if not auth_header or not auth_header.startswith("Bearer "):
        return None
    return auth_header.split(" ", 1)[1].strip()


def check_auth(config_entry: Dict) -> bool:
    auth_cfg = config_entry.get("auth", {})
    if not auth_cfg:
        return True
    if auth_cfg.get("type") == "basic":
        auth = request.authorization
        if not auth:
            return False
        return auth.username == auth_cfg.get("username") and auth.password == auth_cfg.get("password")
    if auth_cfg.get("type") == "bearer":
        token = bearer_token_from_header(request.headers.get("Authorization", ""))
        return token == auth_cfg.get("token")
    return True


def require_auth(config_entry: Dict):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if not check_auth(config_entry):
                log_request(401)
                return unauthorized_response()
            return func(*args, **kwargs)

        return wrapper

    return decorator


def _endpoint_name(prefix: str, path: str) -> str:
    clean = path.strip("/").replace("/", "_") or "root"
    return f"{prefix}_{clean}"


def register_endpoints(app: Flask, config: Dict) -> None:
    # Upload endpoints
    for ep in config.get("upload_endpoints", []):
        path = ep["path"]
        save_dir = ep["save_dir"]
        done_dir = ep.get("done_dir", os.path.join(save_dir, "done"))
        ensure_dir(save_dir)
        ensure_dir(done_dir)

        @app.route(path, methods=["POST"], endpoint=_endpoint_name("upload", path))
        @require_auth(ep)
        def upload(ep=ep, save_dir=save_dir):
            content: Optional[bytes] = None
            file_name: Optional[str] = request.headers.get("X-File-Name")
            original_b64 = request.headers.get("X-Original-File-Name-B64")
            if original_b64:
                try:
                    original_name = base64.b64decode(original_b64).decode("utf-8")
                    file_name = original_name
                except Exception:
                    pass

            if request.files:
                # Accept first file from multipart form data
                first_file = next(iter(request.files.values()))
                content = first_file.read()
                if not file_name:
                    file_name = first_file.filename
            else:
                # Raw body
                content = request.data

            if not content:
                log_request(400)
                return jsonify({"error": "empty body"}), 400

            if not file_name:
                log_request(400)
                return jsonify({"error": "missing X-File-Name header or multipart filename"}), 400

            ensure_dir(save_dir)
            dest = os.path.join(save_dir, file_name)
            with open(dest, "wb") as f:
                f.write(content)
            preview = content[:50].decode(errors="replace")
            log_request(200, preview)
            return jsonify({"status": "ok", "saved_to": dest}), 200

    # Download endpoints
    for ep in config.get("download_endpoints", []):
        path = ep["path"]
        input_dir = ep["input_dir"]
        done_dir = ep.get("done_dir", os.path.join(input_dir, "done"))
        ensure_dir(input_dir)
        ensure_dir(done_dir)

        @app.route(path, methods=["GET"], endpoint=_endpoint_name("download", path))
        @require_auth(ep)
        def download(ep=ep, input_dir=input_dir, done_dir=done_dir):
            files = sorted(Path(input_dir).glob("*"))
            files = [f for f in files if f.is_file()]
            if not files:
                log_request(204)
                return ("", 204)
            file_path = files[0]
            file_name = file_path.name
            create_ts = ep.get("create_timestamp_header_value", "")

            safe_name = file_name.encode('utf-8', errors='ignore').decode('ascii', errors='ignore')
            if not safe_name:
                safe_name = "file.bin"
            resp = make_response(send_file(str(file_path), as_attachment=True, download_name=file_name))
            resp.headers["X-File-Name"] = safe_name
            resp.headers["X-Original-File-Name-B64"] = base64.b64encode(file_name.encode("utf-8")).decode("ascii")
            resp.headers["X-Create-Timestamp"] = create_ts

            with open(file_path, "rb") as f:
                preview = f.read(50).decode(errors="replace")
            log_request(200, preview)

            # Move after sending
            dest = Path(done_dir) / file_name
            ensure_dir(done_dir)
            try:
                Path(file_path).rename(dest)
            except Exception as e:
                logger.error(f"Failed moving file {file_path} to {dest}: {e}")
            return resp


register_endpoints(app, CONFIG)


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    host = os.environ.get("HTTP_TEST_HOST", "0.0.0.0")
    port = int(os.environ.get("HTTP_TEST_PORT", "8080"))
    app.run(host=host, port=port)

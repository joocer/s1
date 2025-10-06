import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest
from minio import Minio
from minio.select import (JSONOutputSerialization, ParquetInputSerialization,
                          SelectRequest)

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "data"


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_for_server(port: int, process: subprocess.Popen, timeout: float = 5.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if process.poll() is not None:
            raise RuntimeError("S1 server exited before becoming ready.")
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(0.5)
            if sock.connect_ex(("127.0.0.1", port)) == 0:
                return
        time.sleep(0.1)
    raise TimeoutError("Timed out waiting for S1 server to start.")


@pytest.fixture(scope="module")
def minio_client():
    port = _find_free_port()
    env = os.environ.copy()
    env.update(
        {
            "STORAGE_BACKEND": "local",
            "LOCAL_STORAGE_PATH": str(DATA_DIR),
            "STORAGE_CACHE_SIZE": "8",
        }
    )

    src_path = str(REPO_ROOT / "src")
    existing_path = env.get("PYTHONPATH")
    env["PYTHONPATH"] = (
        f"{src_path}{os.pathsep}{existing_path}" if existing_path else src_path
    )

    cmd = [
        sys.executable,
        "-m",
        "uvicorn",
        "main:application",
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
        "--log-level",
        "warning",
    ]

    process = subprocess.Popen(
        cmd,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )

    try:
        _wait_for_server(port, process)
    except Exception as exc:  # noqa: BLE001 - convert to skip or informative failure
        process.terminate()
        try:
            _, stderr_data = process.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            _, stderr_data = process.communicate(timeout=5)
        message = stderr_data.decode() if stderr_data else ""
        if "operation not permitted" in message.lower():
            pytest.skip("Environment forbids binding to localhost sockets.")
        raise RuntimeError(f"Failed to start S1 server: {message}") from exc

    client = Minio(
        f"127.0.0.1:{port}",
        access_key="access_key",
        secret_key="secret_key",
        secure=False,
    )

    yield client

    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)
    finally:
        if process.stderr:
            process.stderr.close()


def test_list_objects_returns_parquet(minio_client):
    objects = list(minio_client.list_objects("astronauts", recursive=True))
    names = {obj.object_name for obj in objects}
    assert "astronauts.parquet" in names


def test_get_object_matches_local_file(minio_client):
    result = minio_client.get_object("astronauts", "astronauts.parquet")
    try:
        remote_bytes = result.read()
    finally:
        result.close()
        result.release_conn()

    local_bytes = (DATA_DIR / "astronauts" / "astronauts.parquet").read_bytes()
    assert remote_bytes == local_bytes


def test_select_object_content_returns_filtered_rows(minio_client):
    request = SelectRequest(
        "SELECT name, space_flights FROM S3Object WHERE space_flights > 2",
        ParquetInputSerialization(),
        JSONOutputSerialization(),
    )

    with minio_client.select_object_content(
        "astronauts", "astronauts.parquet", request
    ) as reader:
        payload = b"".join(chunk for chunk in reader.stream())

    assert payload, "Expected at least one record from SelectObjectContent"
    lines = payload.decode("utf-8").strip().splitlines()
    sample = json.loads(lines[0])
    assert set(sample) == {"name", "space_flights"}
    assert sample["space_flights"] > 2

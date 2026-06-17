"""Integration test: run the fastsqs handler in the REAL AWS Lambda runtime.

Builds a container from `public.ecr.aws/lambda/python:3.13` (which bundles the
Lambda Runtime Interface Emulator), then POSTs the SQS event fixtures in
`tests/events/` to the RIE invoke endpoint — exactly the `{"Records":[...]}`
envelope the SQS event-source mapping delivers in production. Asserts the
partial-batch-failure response.

Docker-only, no cloud / no credentials → runs the same locally and in CI.

    pytest --run-integration tests/integration

Auto-skips when Docker is unavailable.
"""

import json
import shutil
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

REPO = Path(__file__).resolve().parents[2]
EVENTS = REPO / "tests" / "events"
DOCKERFILE = REPO / "tests" / "integration" / "Dockerfile"
IMAGE = "fastsqs-rie:test"
CONTAINER = "fastsqs-rie-test"
PORT = 9000
INVOKE_URL = f"http://localhost:{PORT}/2015-03-31/functions/function/invocations"


def _docker_ok() -> bool:
    if not shutil.which("docker"):
        return False
    return subprocess.run(["docker", "info"], capture_output=True).returncode == 0


def _invoke(event: dict) -> dict:
    data = json.dumps(event).encode()
    req = urllib.request.Request(
        INVOKE_URL, data=data, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode())


def _wait_ready(deadline_s: float = 60.0) -> None:
    """Poll the RIE endpoint until an empty event yields an empty failure list."""
    deadline = time.time() + deadline_s
    last = None
    while time.time() < deadline:
        try:
            if _invoke({"Records": []}) == {"batchItemFailures": []}:
                return
        except (urllib.error.URLError, ConnectionError, OSError, ValueError) as exc:
            last = exc
        time.sleep(0.5)
    raise RuntimeError(f"Lambda RIE did not become ready: {last}")


def _load(name: str) -> dict:
    return json.loads((EVENTS / name).read_text())


@pytest.fixture(scope="module")
def rie():
    if not _docker_ok():
        pytest.skip("docker not available")
    subprocess.run(
        ["docker", "build", "-f", str(DOCKERFILE), "-t", IMAGE, "."],
        cwd=REPO,
        check=True,
        capture_output=True,
    )
    subprocess.run(["docker", "rm", "-f", CONTAINER], capture_output=True)
    subprocess.run(
        ["docker", "run", "-d", "--rm", "-p", f"{PORT}:8080", "--name", CONTAINER, IMAGE],
        check=True,
        capture_output=True,
    )
    try:
        _wait_ready()
        yield
    finally:
        subprocess.run(["docker", "stop", CONTAINER], capture_output=True)


def test_standard_batch_partial_failure(rie):
    """Standard queue: only the failing record is reported; others succeed."""
    result = _invoke(_load("sqs_standard_batch.json"))
    assert result == {
        "batchItemFailures": [
            {"itemIdentifier": "22222222-2222-2222-2222-222222222222"}
        ]
    }


def test_fifo_batch_halts_group(rie):
    """FIFO queue: the failing record AND every later record in its group are
    reported (ordering preserved); the earlier record still succeeds."""
    result = _invoke(_load("sqs_fifo_batch.json"))
    failed = {f["itemIdentifier"] for f in result["batchItemFailures"]}
    assert failed == {
        "aaaaaaaa-0002-0002-0002-000000000002",  # boom
        "aaaaaaaa-0003-0003-0003-000000000003",  # blocked tail
    }


def test_invalid_body_is_a_clean_batch_failure(rie):
    """A non-JSON body becomes an InvalidMessage -> the record fails cleanly."""
    result = _invoke(_load("sqs_invalid_body.json"))
    assert result == {"batchItemFailures": [{"itemIdentifier": "inv-1"}]}


def test_redelivered_event_with_attributes_succeeds(rie):
    """A redelivered message (ApproximateReceiveCount>1) with messageAttributes
    is processed normally."""
    result = _invoke(_load("sqs_retry_with_attributes.json"))
    assert result == {"batchItemFailures": []}

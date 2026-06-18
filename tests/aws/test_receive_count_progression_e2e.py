"""Real-AWS e2e: server-side ApproximateReceiveCount progression and the
maxReceiveCount -> DLQ boundary, observed across real ESM redeliveries
(opt-in: ``pytest --run-aws``).

These assert behavior that only the real SQS + ESM stack produces: SQS bumps
``ApproximateReceiveCount`` on every redelivery, the ESM redrives an always-
failing record until ``receiveCount > maxReceiveCount``, and the dead-lettered
copy carries ``ApproximateReceiveCount == maxReceiveCount + 1``. The deployed
handler echoes a receipt (carrying the per-invocation ``approx_receive_count``)
to a per-test results queue BEFORE it fails, so draining that queue reconstructs
the receive-count sequence the Lambda actually saw. The ``echo-fail-*`` task id
echoes then ALWAYS fails, driving the message all the way to the DLQ.
Harness in conftest.py.
"""

import json
import uuid

import pytest

pytestmark = pytest.mark.aws


def _has_identifier(identifier):
    """Predicate for drain_full: keep only results-queue receipts for ``identifier``."""

    def _pred(m):
        try:
            return json.loads(m["Body"]).get("identifier") == identifier
        except (KeyError, ValueError):
            return False

    return _pred


def _recv_counts_for(drain_full, results_url, identifier, min_count):
    """Drain the results queue and return the list of approx_receive_count ints
    (as the handler saw them, BEFORE its fail decision) for one task id."""
    msgs = drain_full(
        results_url, timeout=180, min_count=min_count, predicate=_has_identifier(identifier)
    )
    counts = []
    for m in msgs:
        body = json.loads(m["Body"])
        counts.append(int(body["approx_receive_count"]))
    return counts


def test_approx_receive_count_increments_to_maxreceive_then_dlq(aws, pipeline, drain_full):
    """Default pipeline (max_receive_count=3, visibility=10): an always-failing
    echo-fail message is redelivered with ApproximateReceiveCount 1,2,3 (monotonic,
    no gaps), dead-letters at receiveCount > maxReceiveCount (strict, not off-by-
    one), and the DLQ copy carries ApproximateReceiveCount == maxReceiveCount + 1."""
    sqs = aws["sqs"]
    max_receive = 3
    main_url, dlq_url, results_url = pipeline(
        fifo=False, max_receive_count=max_receive, visibility=10, results=True
    )

    tid = f"echo-fail-{uuid.uuid4().hex[:8]}"
    sqs.send_message(
        QueueUrl=main_url,
        MessageBody=json.dumps({"type": "task", "task_id": tid}),
        MessageAttributes={"ResultsQueue": {"DataType": "String", "StringValue": results_url}},
    )

    # Results queue: the handler echoes its ApproximateReceiveCount on every
    # invocation. Across maxReceiveCount=3 redeliveries we expect the set {1,2,3}.
    counts = _recv_counts_for(drain_full, results_url, tid, min_count=max_receive)
    distinct = sorted(set(counts))
    # Monotonic 1..maxReceiveCount with no gaps; tolerate an occasional extra
    # receive (at-least-once duplicate / requeue) appended at the top.
    assert distinct[: max_receive] == list(range(1, max_receive + 1)), (
        f"expected receive counts to cover 1..{max_receive}, got {sorted(counts)}"
    )
    assert max(distinct) <= max_receive + 1, (
        f"receive count exceeded maxReceiveCount+1, got {sorted(counts)}"
    )

    # DLQ: the dead-lettered copy was received once more than maxReceiveCount
    # (it is the (maxReceiveCount+1)th would-be receive that triggers redrive).
    moved = drain_full(
        dlq_url, timeout=180, min_count=1, predicate=lambda m: json.loads(m["Body"]).get("task_id") == tid
    )
    assert moved, "the always-failing message must reach the DLQ"
    dlq_count = moved[0]["Attributes"]["ApproximateReceiveCount"]
    assert dlq_count == str(max_receive + 1), (
        f"DLQ copy should carry ApproximateReceiveCount == {max_receive + 1}, got {dlq_count!r}"
    )


@pytest.mark.parametrize("max_receive", [1, 3])
def test_maxreceive_boundary_exactly_N_deliveries(aws, pipeline, drain_full, max_receive):
    """maxReceiveCount=N delivers an always-failing message to the Lambda exactly
    N times (not N-1, not N+1), then dead-letters on the (N+1)th would-be receive.
    maxReceiveCount=1 -> exactly ONE invocation then DLQ. (max_receive=5 is skipped
    to avoid the requeue-to-back reordering flake noted in the harness gotchas.)"""
    sqs = aws["sqs"]
    main_url, dlq_url, results_url = pipeline(
        fifo=False, max_receive_count=max_receive, visibility=10, results=True
    )

    tid = f"echo-fail-boundary-{max_receive}-{uuid.uuid4().hex[:8]}"
    sqs.send_message(
        QueueUrl=main_url,
        MessageBody=json.dumps({"type": "task", "task_id": tid}),
        MessageAttributes={"ResultsQueue": {"DataType": "String", "StringValue": results_url}},
    )

    # DLQ first: arrival of the dead-lettered copy proves all redeliveries are done,
    # so the results queue now holds every receipt the handler ever emitted.
    moved = drain_full(
        dlq_url, timeout=180, min_count=1, predicate=lambda m: json.loads(m["Body"]).get("task_id") == tid
    )
    assert moved, f"max_receive_count={max_receive}: message must reach the DLQ"

    # Exactly N distinct receive counts (1..N) observed by the handler. Count by
    # distinct receive number rather than receipt count to absorb at-least-once
    # duplicate echoes of the same delivery.
    counts = _recv_counts_for(drain_full, results_url, tid, min_count=max_receive)
    distinct = sorted(set(counts))
    assert distinct == list(range(1, max_receive + 1)), (
        f"max_receive_count={max_receive}: handler should be invoked exactly "
        f"{max_receive} time(s) (counts 1..{max_receive}), got {sorted(counts)}"
    )

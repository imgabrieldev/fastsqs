"""Real-AWS e2e: partial-batch redrive isolation + per-message receive-count
growth, proven in a single run (opt-in: ``pytest --run-aws``).

The default pipeline runs ``partial_batch_failure=True`` with
ReportBatchItemFailures on the ESM. When a co-batched pair (one good, one poison)
is delivered, ONLY the poison must be re-received — the succeeded sibling is
deleted by SQS (reported as a success) and is NEVER replayed. Across the poison's
redeliveries its ApproximateReceiveCount grows 1 -> 2 -> 3 while the sibling's
stays at exactly 1, and only the poison dead-letters. This single test proves
per-record isolation AND per-message counter growth.

The deployed handler echoes a receipt to the per-record ResultsQueue
messageAttribute BEFORE its pass/fail decision, so each receive of every record
is observable on the results queue regardless of outcome. Harness in conftest.py.
"""

import json

import pytest

pytestmark = pytest.mark.aws


def test_failed_record_redelivers_succeeded_sibling_not_replayed(aws, pipeline, drain, drain_full):
    """Default pipeline (max_receive_count=3). Co-batch ok-A (echo + succeed) with
    boom-B (echo + always fail). Partial redrive must re-receive ONLY boom-B with
    growing ApproximateReceiveCount {1,2,3} while ok-A is deleted after one receive
    and never replayed (count stays 1). Only boom-B dead-letters; ok-A is absent
    from the DLQ."""
    sqs = aws["sqs"]
    main_url, dlq_url, results_url = pipeline(
        fifo=False, max_receive_count=3, results=True
    )

    def send(task_id):
        sqs.send_message(
            QueueUrl=main_url,
            MessageBody=json.dumps({"type": "task", "task_id": task_id}),
            MessageAttributes={
                "ResultsQueue": {"DataType": "String", "StringValue": results_url}
            },
        )

    # Send close together so the ESM co-batches them (BatchSize=10).
    send("ok-A")     # echoes a receipt, then succeeds -> deleted, never replayed
    send("boom-B")   # echoes a receipt every receive, then ALWAYS fails -> redrives to DLQ

    # Drain the results queue: boom-B should echo 3 times (receives 1,2,3) and
    # ok-A exactly once (receive 1). Wait for at least the 4 expected receipts.
    receipts = drain(results_url, timeout=180, min_count=4)
    by_id: dict[str, list[int]] = {}
    for body in receipts:
        r = json.loads(body)
        try:
            count = int(r["approx_receive_count"])
        except (TypeError, ValueError, KeyError):
            count = 0
        by_id.setdefault(r["identifier"], []).append(count)

    a_counts = sorted(by_id.get("ok-A", []))
    b_counts = sorted(by_id.get("boom-B", []))

    # ok-A: the succeeded sibling is deleted after its single receive and is NEVER
    # replayed under partial-batch redrive -> exactly one receipt with count 1.
    assert a_counts == [1], f"ok-A should be received exactly once at count 1, got {a_counts}"

    # boom-B: the failed record is redriven with a growing per-message counter.
    # max_receive_count=3 means SQS delivers it 3 times (counts 1,2,3) before
    # dead-lettering -> a strictly growing {1,2,3} sequence.
    assert b_counts == [1, 2, 3], f"boom-B should be re-received at counts 1,2,3, got {b_counts}"

    # DLQ: only boom-B dead-letters; ok-A is absent. (drain_full lets us read the
    # ApproximateReceiveCount on the dead-lettered copy too.)
    dlq_msgs = drain_full(dlq_url, timeout=180, min_count=1)
    dlq_ids = [json.loads(m["Body"])["task_id"] for m in dlq_msgs]
    assert "boom-B" in dlq_ids, f"boom-B must dead-letter, DLQ had {dlq_ids}"
    assert "ok-A" not in dlq_ids, f"ok-A must NOT dead-letter, DLQ had {dlq_ids}"

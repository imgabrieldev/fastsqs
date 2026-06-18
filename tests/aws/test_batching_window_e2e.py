"""Real-AWS e2e: MaximumBatchingWindowInSeconds coalesces staggered sends into a
single Lambda invocation (opt-in: ``pytest --run-aws``).

This cannot be proven in-process: the buffering is done by the SQS event-source
mapping, not by fastsqs. With BatchSize=1 / window=0 each record is delivered in
its own invocation, so staggered sends produce one Records array (one
aws_request_id) per record. With BatchSize=10 / window=5 the ESM holds the poll
open for up to the window, accumulating records sent within it into a single
Records array — so at least one invocation sees >=2 records.

We observe this through the handler's results-queue echo: every receipt carries
``aws_request_id`` (the per-invocation id) and ``identifier`` (the task_id). We
group receipts by aws_request_id and assert the record-count distribution.

The 6MB payload / per-record metadata cap can make a real batch smaller than the
configured BatchSize, so we assert that SOME invocation coalesced >=2 records
(not that all 5 landed in one). Harness in conftest.py.
"""

import json
import time
from collections import defaultdict

import pytest

pytestmark = pytest.mark.aws


def _send(sqs, main_url, results_url, task_id):
    sqs.send_message(
        QueueUrl=main_url,
        MessageBody=json.dumps({"type": "task", "task_id": task_id}),
        MessageAttributes={
            "ResultsQueue": {"DataType": "String", "StringValue": results_url}
        },
    )


def _group_by_request_id(receipts):
    """Map aws_request_id -> sorted list of task_id identifiers in that invocation."""
    groups = defaultdict(list)
    for body in receipts:
        r = json.loads(body)
        groups[r["aws_request_id"]].append(r["identifier"])
    return {rid: sorted(ids) for rid, ids in groups.items()}


@pytest.mark.slow
def test_maximum_batching_window_coalesces_records_into_one_invocation(
    aws, pipeline, drain
):
    """A: BatchSize=1/window=0 -> staggered sends each get their own invocation
    (~5 distinct request_ids, each with exactly 1 record). B: BatchSize=10/
    window=5 -> sends within the window coalesce, so at least one invocation's
    Records array carries >=2 records (one request_id with >=2 identifiers)."""
    sqs = aws["sqs"]

    # --- Pipeline A: no batching window, one record per invocation. ---
    a_main, _a_dlq, a_results = pipeline(
        fifo=False, results=True, batch_size=1, batching_window=0
    )
    a_ids = [f"echo-a-{i}" for i in range(5)]
    for tid in a_ids:
        _send(sqs, a_main, a_results, tid)
        time.sleep(0.5)  # space sends out so window=0 cannot coalesce them

    a_receipts = drain(a_results, timeout=180, min_count=5)
    a_groups = _group_by_request_id(a_receipts)

    # Every echoed identifier arrived (no duplicates expected: these never fail).
    a_seen = sorted(i for ids in a_groups.values() for i in ids)
    assert a_seen == sorted(a_ids), f"pipeline A identifiers: {a_seen}"
    # With window=0 + BatchSize=1, each record gets its own invocation: ~5
    # distinct request_ids, each carrying exactly one record.
    assert all(len(ids) == 1 for ids in a_groups.values()), (
        f"pipeline A should be 1 record per invocation, got {a_groups}"
    )
    assert len(a_groups) == len(a_ids), (
        f"pipeline A expected {len(a_ids)} distinct invocations, got {a_groups}"
    )

    # --- Pipeline B: 5s batching window, BatchSize=10, burst the sends. ---
    b_main, _b_dlq, b_results = pipeline(
        fifo=False, results=True, batch_size=10, batching_window=5
    )
    b_ids = [f"echo-b-{i}" for i in range(5)]
    for tid in b_ids:
        _send(sqs, b_main, b_results, tid)
        time.sleep(0.4)  # all five land well within the ~5s window (~2s total)

    b_receipts = drain(b_results, timeout=180, min_count=5)
    b_groups = _group_by_request_id(b_receipts)

    b_seen = sorted(i for ids in b_groups.values() for i in ids)
    assert b_seen == sorted(b_ids), f"pipeline B identifiers: {b_seen}"
    # The batching window must have coalesced staggered sends into at least one
    # multi-record Records array. We assert >=2 in one invocation rather than
    # exactly 5: the 6MB / per-record-metadata cap (and ESM poll timing) can
    # split a configured batch, so a coalesced batch may be smaller than 5.
    max_batch = max(len(ids) for ids in b_groups.values())
    assert max_batch >= 2, (
        f"pipeline B expected at least one invocation with >=2 coalesced records, "
        f"got distribution {b_groups}"
    )

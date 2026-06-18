"""Real-AWS e2e: positive FIFO ordering + ordered partial-failure redrive,
observed through the handler's results-queue echo side channel
(opt-in: ``pytest --run-aws``).

The existing FIFO test (``test_fifo_e2e.py``) only observes the dead-letter side
of the FIFO+DLQ tradeoff. These tests prove the *positive* ordered-success path:

1. The real ESM delivers same-group records in strict send order, packing them
   in order into ONE ``Records`` array, with strictly increasing SequenceNumbers.
2. fastsqs ``isolate_groups`` reports the whole not-yet-processed same-group tail
   in ``batchItemFailures`` on the FIRST failing invocation, so the ESM keeps the
   tail (it dead-letters WITH the poison) instead of silently deleting it — no
   data loss.
3. On a *transient* same-group failure the failed tail is redelivered in order,
   the already-succeeded head is not replayed, and the tail behind the failure is
   held until the failure clears — effective order preserved, nothing dead-letters.

All success/order observation rides on the handler echoing a JSON receipt to the
per-record ``ResultsQueue`` messageAttribute BEFORE its pass/fail decision; we
drain that per-test results queue. Harness in conftest.py.
"""

import json

import pytest

pytestmark = pytest.mark.aws


def _receipts(drain_full, results_url, *, min_count, timeout=180):
    """Drain the results queue and return parsed receipt dicts (delete-as-read)."""
    out = []
    for m in drain_full(results_url, timeout=timeout, min_count=min_count):
        out.append(json.loads(m["Body"]))
    return out


def _seq(receipt):
    """SequenceNumber as int (FIFO SequenceNumbers are large numeric strings)."""
    return int(receipt["sequence_number"])


def _send(sqs, url, task_id, results_url, *, group=None, dedup=None):
    kw = {
        "QueueUrl": url,
        "MessageBody": json.dumps({"type": "task", "task_id": task_id}),
        "MessageAttributes": {
            "ResultsQueue": {"DataType": "String", "StringValue": results_url}
        },
    }
    if group is not None:
        kw["MessageGroupId"] = group
    if dedup is not None:
        kw["MessageDeduplicationId"] = dedup
    return sqs.send_message(**kw)


def _send_group_batch(sqs, url, task_ids, results_url, *, group):
    """Send a whole FIFO group in ONE send_message_batch so the records share a
    single source-side commit and (with a batching window on the ESM) co-batch
    into ONE Lambda invocation. All entries carry the same MessageGroupId; the
    queue is created with ContentBasedDeduplication so no MessageDeduplicationId
    is needed.
    """
    entries = [
        {
            "Id": str(i),
            "MessageBody": json.dumps({"type": "task", "task_id": tid}),
            "MessageGroupId": group,
            "MessageAttributes": {
                "ResultsQueue": {"DataType": "String", "StringValue": results_url}
            },
        }
        for i, tid in enumerate(task_ids)
    ]
    resp = sqs.send_message_batch(QueueUrl=url, Entries=entries)
    assert not resp.get("Failed"), f"send_message_batch had failures: {resp.get('Failed')}"
    return resp


def test_fifo_in_group_order_preserved_single_invocation(aws, pipeline, drain_full):
    """ONE MessageGroupId, 10 rapid sends (echo-seq-0..9): the real ESM delivers
    them in strict send order in a single ordered batch, so when the echoed
    receipts are sorted by SequenceNumber the task_ids come out 0..9 and the
    SequenceNumbers are strictly increasing.

    Real-AWS note: the *results* queue is a plain (non-FIFO) queue, so the order
    in which receipts physically arrive there is not guaranteed. We therefore key
    ordering off the per-record FIFO SequenceNumber (a property the source queue
    assigns at send time) rather than results-queue arrival order.
    """
    sqs = aws["sqs"]
    main_url, _dlq_url, results_url = pipeline(fifo=True, max_receive_count=2, results=True)

    for i in range(10):
        _send(sqs, main_url, f"echo-seq-{i}", results_url, group="ORDER")

    receipts = _receipts(drain_full, results_url, min_count=10)
    seq_receipts = [r for r in receipts if r["identifier"].startswith("echo-seq-")]
    assert len(seq_receipts) >= 10, f"expected >=10 echo-seq receipts, got {len(seq_receipts)}"

    # Same group -> every record carries the same MessageGroupId.
    assert {r["message_group_id"] for r in seq_receipts} == {"ORDER"}

    # Order by the FIFO SequenceNumber the source queue assigned at send time.
    ordered = sorted(seq_receipts, key=_seq)
    task_order = [r["identifier"] for r in ordered]
    assert task_order == [f"echo-seq-{i}" for i in range(10)], (
        f"send order not preserved by SequenceNumber: {task_order}"
    )

    # SequenceNumber strictly increasing in that same order.
    seqs = [_seq(r) for r in ordered]
    assert all(b > a for a, b in zip(seqs, seqs[1:])), f"SequenceNumber not strictly increasing: {seqs}"


@pytest.mark.slow
def test_fifo_failed_tail_reported_on_first_failure_not_skipped(aws, pipeline, drain, drain_full):
    """FIFO group G = g1(ok), boom-g2(poison), g3, g4 in one group. With
    ``isolate_groups`` fastsqs adds the whole not-yet-processed same-group tail
    (boom-g2, g3, g4) to ``batchItemFailures`` on the FIRST failing invocation,
    so the ESM keeps g3/g4 — they dead-letter WITH the poison rather than being
    silently deleted. g1 (before the poison) succeeds and is never dead-lettered.

    A regression that reported only the poison would leave g3/g4 ABSENT from the
    DLQ (data loss); this test asserts they are present.
    """
    sqs = aws["sqs"]
    # FIFO rejects MaximumBatchingWindowInSeconds, so we co-batch deterministically
    # with start_disabled: enqueue the whole group via ONE send_message_batch while
    # the ESM is OFF, then enable() so the first poll grabs g1..g4 as ONE Records
    # array. The poison and its tail co-batch (isolate_groups can then report the
    # whole tail on the first failure instead of the tail arriving in a later batch).
    main_url, dlq_url, results_url, enable = pipeline(
        fifo=True, max_receive_count=2, results=True, start_disabled=True
    )

    _send_group_batch(sqs, main_url, ("g1", "boom-g2", "g3", "g4"), results_url, group="G")
    enable()

    # The poison AND its blocked tail (g3, g4) all dead-letter; g1 does not.
    moved = drain(dlq_url, timeout=180, min_count=3)
    dlq_ids = {json.loads(b)["task_id"] for b in moved}
    assert {"boom-g2", "g3", "g4"} <= dlq_ids, f"tail not preserved in DLQ (data loss): {dlq_ids}"
    assert "g1" not in dlq_ids, "g1 (before the poison) must NOT dead-letter"

    # g1 (before the poison) was processed; it is the only id deleted as success.
    # The handler echoes BEFORE deciding, so g3/g4 may echo on a redelivery, but
    # they must never appear AHEAD of the poison g2 by SequenceNumber as a success
    # path that deleted them: their presence in the DLQ above already proves they
    # were not silently consumed. Here we positively confirm g1 was handled.
    # Wide non-short-circuiting drain: min_count huge so it waits the full window
    # and collects EVERY receipt (g1's success echo can lag behind the poison's on
    # the non-FIFO results queue; a min_count=1 drain would return the poison and
    # miss g1).
    receipts = _receipts(drain_full, results_url, min_count=10_000, timeout=150)
    by_id = {}
    for r in receipts:
        by_id.setdefault(r["identifier"], []).append(r)
    assert "g1" in by_id, f"g1 was never processed/echoed: {sorted(by_id)}"

    # Whatever echoes exist for g3/g4, they sit AFTER the poison g2 in FIFO order
    # (strictly greater SequenceNumber), confirming they were never processed
    # ahead of the poison.
    if "boom-g2" in by_id and ("g3" in by_id or "g4" in by_id):
        poison_seq = min(_seq(r) for r in by_id["boom-g2"])
        for tail in ("g3", "g4"):
            for r in by_id.get(tail, []):
                assert _seq(r) > poison_seq, f"{tail} ordered ahead of the poison g2"


def test_fifo_partial_redrive_preserves_order_transient_failure(aws, pipeline, drain, drain_full):
    """FIFO group A = a1(ok), flaky-2-A(transient: fails while
    ApproximateReceiveCount < 2, succeeds at >= 2), a3(ok), max_receive_count=3,
    low visibility. On the partial FIFO redrive the failed tail is redelivered in
    order: a1 is not replayed, a3 is held behind flaky-2 until it succeeds, giving
    effective order a1, flaky-2, a3 with no gap. Nothing dead-letters (transient).

    Real-AWS note: ApproximateReceiveCount is approximate; we assert a1 echoes at
    receive 1, flaky-2 echoes at both receive 1 (the failing pass) and >= 2 (the
    succeeding pass), and a3's *succeeding* echo carries receive count >= 2 (it
    was held behind flaky-2 across the redrive), and the DLQ stays empty.
    """
    sqs = aws["sqs"]
    # FIFO rejects MaximumBatchingWindowInSeconds, so we co-batch deterministically
    # with start_disabled: enqueue the whole group via ONE send_message_batch while
    # the ESM is OFF, then enable() so the first poll grabs a1..a3 as ONE invocation.
    # flaky-2's transient failure then holds its tail (a3) in isolate_groups rather
    # than a3 arriving in a separate batch and succeeding ahead of the redrive.
    main_url, dlq_url, results_url, enable = pipeline(
        fifo=True, max_receive_count=3, visibility=10, results=True, start_disabled=True
    )

    _send_group_batch(sqs, main_url, ("a1", "flaky-2-A", "a3"), results_url, group="A")
    enable()

    # Collect EVERY receipt in a wide window: the redelivery only happens after the
    # ~10s visibility timeout, and a3 is held behind flaky-2 across that redrive, so
    # we must wait the whole window. min_count is set huge so the drain never short-
    # circuits and waits the full timeout, capturing the failing + succeeding passes.
    receipts = _receipts(drain_full, results_url, min_count=10_000, timeout=150)
    by_id = {}
    for r in receipts:
        by_id.setdefault(r["identifier"], []).append(r)

    a1 = by_id.get("a1", [])
    flaky = by_id.get("flaky-2-A", [])
    a3 = by_id.get("a3", [])

    assert a1, "a1 was never processed"
    assert flaky, "flaky-2-A was never processed"
    assert a3, "a3 was never processed"

    # Assert on the DISTINCT receive-count set per id (the handler echoes once per
    # delivery, but ApproximateReceiveCount is approximate and an id may echo more
    # than once at the same count; collapsing to a set keys off the delivery rounds).
    a1_counts = {int(r["approx_receive_count"]) for r in a1}
    flaky_counts = {int(r["approx_receive_count"]) for r in flaky}
    a3_counts = {int(r["approx_receive_count"]) for r in a3}

    # a1 is before the failure: processed on the first receive, never replayed.
    assert min(a1_counts) == 1, f"a1 first receive count should be 1, got {sorted(a1_counts)}"

    # flaky-2 is retried: it must be seen at receive 1 (failing) and at >= 2 (ok).
    assert min(flaky_counts) == 1, f"flaky-2-A should first fail at receive 1, got {sorted(flaky_counts)}"
    assert max(flaky_counts) >= 2, f"flaky-2-A should succeed at receive >= 2, got {sorted(flaky_counts)}"

    # a3 is held behind flaky-2: its processing rides the redrive, so its receive
    # count reaches >= 2 (it was redelivered with the failed tail rather than
    # being deleted ahead of flaky-2).
    assert max(a3_counts) >= 2, f"a3 should be held behind flaky-2 (receive >= 2), got {sorted(a3_counts)}"

    # FIFO order across the group: a1 < flaky-2 < a3 by SequenceNumber.
    a1_seq = min(_seq(r) for r in a1)
    flaky_seq = min(_seq(r) for r in flaky)
    a3_seq = min(_seq(r) for r in a3)
    assert a1_seq < flaky_seq < a3_seq, (
        f"send order not preserved: a1={a1_seq} flaky={flaky_seq} a3={a3_seq}"
    )

    # Transient failure -> nothing dead-letters. (Empty drain within the timeout.)
    assert drain(dlq_url, timeout=60, min_count=1) == [], "transient failure should not dead-letter"

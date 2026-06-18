"""Real-AWS e2e: idempotency under SQS at-least-once redelivery, observed via the
results-queue echo side channel (opt-in: ``pytest --run-aws``).

fastsqs performs NO in-process retry (project design): the ONLY retry path is SQS
redelivery + DLQ. These tests prove the practical consequence — a record that
fails once is redelivered (with an incremented ApproximateReceiveCount) and may
have its side effect attempted more than once, so handlers must be idempotent
because the wire CAN duplicate.

The deployed handler echoes a receipt to the per-record ResultsQueue
messageAttribute BEFORE its pass/fail decision, so draining that queue
reconstructs every receive the Lambda actually saw (with its system-attribute
ApproximateReceiveCount and the SQS messageId). Harness in conftest.py.
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


@pytest.mark.slow
@pytest.mark.xfail(
    reason="observing the exact receive-count progression (1 then 2) of a "
    "transient-fail-then-succeed record is timing-dependent on a real ESM; the "
    "redelivery-with-incrementing-ApproximateReceiveCount mechanism is proven "
    "deterministically by tests/aws/test_visibility_redelivery.py.",
    strict=False,
)
def test_transient_failure_redelivered_and_eventually_succeeds(aws, pipeline, drain_full):
    """Default pipeline (partial_batch_failure=True, max_receive_count=3, short
    visibility=10) with a results queue. A ``fail-once-*`` record fails on receive
    1 and succeeds on receive >= 2.

    Proves SQS at-least-once redelivery is the ONLY retry path: fastsqs does no
    in-process retry, so the record is genuinely re-received by the ESM with a
    *strictly increasing* ApproximateReceiveCount, and its side effect (the echo)
    is attempted N >= 2 times. A non-idempotent handler would therefore double its
    effect — the duplicate must be absorbed by idempotency. The record eventually
    succeeds and NEVER reaches the DLQ.

    The two receipts (receive 1 fails, receive 2 succeeds) are separated by the
    visibility window, so the drain must WAIT the whole window rather than exit
    early on a min_count of 2 — otherwise only the receive-1 echo is observed and
    the redelivery is missed. The drain therefore uses a huge min_count + a generous
    timeout so it collects both, and the distinct receive-count set starts 1,2.
    """
    sqs = aws["sqs"]
    # Short visibility (10s, the minimum legal value == the function Timeout) so the
    # receive-1 failure goes invisible only briefly and the ESM re-delivers it well
    # within the drain window. max_receive_count=3 leaves headroom so the transient
    # failure (receive 1) never dead-letters before it succeeds (receive 2).
    main_url, dlq_url, results_url = pipeline(
        fifo=False, max_receive_count=3, visibility=10, results=True
    )

    tid = f"fail-once-{uuid.uuid4().hex[:8]}"
    sqs.send_message(
        QueueUrl=main_url,
        MessageBody=json.dumps({"type": "task", "task_id": tid}),
        MessageAttributes={"ResultsQueue": {"DataType": "String", "StringValue": results_url}},
    )

    # The handler echoes a receipt on EVERY receive (before the pass/fail decision),
    # so a fail-once record yields exactly two receipts: receive 1 (fails ->
    # invisible for visibility=10s -> redelivered) and receive 2 (succeeds). The two
    # receipts are separated by the visibility window, so a min_count-based early
    # exit would stop after the receive-1 echo and miss the redelivery. Set
    # min_count huge so the drain WAITS the full window and COLLECTS both receipts.
    receipts = drain_full(
        results_url, timeout=150, min_count=10_000, predicate=_has_identifier(tid)
    )
    assert len(receipts) >= 2, (
        f"fail-once must be received >= 2 times (1 fails, >=2 succeeds), got "
        f"{len(receipts)} receipt(s)"
    )

    # ApproximateReceiveCount is server-assigned and bumped on each redelivery;
    # across receives it must be strictly increasing (no in-process retry could
    # produce this — only a real ESM redelivery does). Take it from the system
    # Attributes on each receipt message (PascalCase, string-valued).
    counts = sorted(int(m["Attributes"]["ApproximateReceiveCount"]) for m in receipts)
    distinct = sorted(set(counts))
    assert distinct[:2] == [1, 2], (
        f"the failed-then-succeeded record must be re-received with strictly "
        f"increasing ApproximateReceiveCount starting 1,2; got {counts}"
    )
    # It succeeded on the second delivery, so it must not exhaust maxReceiveCount.
    assert max(distinct) <= 3, (
        f"a fail-once record should succeed well before maxReceiveCount=3; got {counts}"
    )

    # NEVER reaches the DLQ: it succeeded on redelivery. Drain with a generous window
    # so a (wrong) dead-letter movement would have time to surface; its absence here
    # means the record was deleted as a success, not dead-lettered.
    moved = drain_full(
        dlq_url,
        timeout=60,
        min_count=10_000,
        predicate=lambda m: json.loads(m["Body"]).get("task_id") == tid,
    )
    assert not moved, (
        f"fail-once-{tid} succeeded on redelivery and must NOT dead-letter; "
        f"DLQ had {[json.loads(m['Body']).get('task_id') for m in moved]}"
    )


def test_at_least_once_duplicate_surfaced_not_manufactured(aws, pipeline, drain_full):
    """Default pipeline, low visibility=2 to maximize delete/visibility races, with
    a results queue keyed by SQS messageId. Send a burst of N distinct succeed-only
    echo messages.

    Standard SQS is at-least-once: a message CAN be delivered more than once. This
    test proves the harness would CATCH a wire duplicate (same messageId echoed
    more than once) if AWS produced one, and that fastsqs neither manufactures nor
    swallows duplicates — every sent message is observed exactly via its messageId,
    and any messageId seen more than once is an *observed* AWS wire duplicate, not
    one fabricated by fastsqs.

    Observational / soft: AWS rarely emits a wire duplicate, so the absence of one
    is NOT a hard failure. The hard assertion is completeness (no missing
    messageId); duplicates are logged for visibility only.
    """
    sqs = aws["sqs"]
    # visibility must be >= the function Timeout (10) or AWS rejects the ESM
    # create; the harness clamps nothing, so use the minimum legal value (10) that
    # still keeps the visibility window as short as allowed to surface races.
    main_url, _dlq_url, results_url = pipeline(
        fifo=False, max_receive_count=3, visibility=10, results=True
    )

    n = 8
    sent_ids = set()
    for i in range(n):
        resp = sqs.send_message(
            QueueUrl=main_url,
            MessageBody=json.dumps({"type": "task", "task_id": f"echo-ok-{i}"}),
            MessageAttributes={
                "ResultsQueue": {"DataType": "String", "StringValue": results_url}
            },
        )
        sent_ids.add(resp["MessageId"])

    # Every succeed-only message echoes exactly one receipt per receive. With
    # at-least-once delivery we expect AT LEAST one receipt per messageId; collect
    # them all so a wire duplicate (same messageId twice) would be caught.
    receipts = drain_full(results_url, timeout=180, min_count=n)
    seen_counts: dict[str, int] = {}
    for m in receipts:
        body = json.loads(m["Body"])
        mid = body.get("message_id")
        if mid is None:
            continue
        seen_counts[mid] = seen_counts.get(mid, 0) + 1

    seen_ids = set(seen_counts)

    # HARD: completeness — every sent message must surface on the results queue.
    # fastsqs must not swallow any record (no missing messageId).
    missing = sent_ids - seen_ids
    assert not missing, f"these sent messageIds never echoed (swallowed?): {missing}"

    # HARD: no manufactured ids — every echoed messageId must be one we sent.
    # fastsqs must not fabricate a record with an id we never put on the wire.
    foreign = seen_ids - sent_ids
    assert not foreign, f"results queue had messageIds we never sent (manufactured?): {foreign}"

    # SOFT / observational: any messageId echoed more than once is a genuine SQS
    # at-least-once WIRE duplicate (the handler echoes once per receive). We log it
    # but do NOT fail — AWS only occasionally produces a duplicate, and its absence
    # proves nothing wrong. Its PRESENCE proves the harness catches duplicates and
    # that fastsqs surfaced (did not hide) the redelivery.
    observed_dupes = {mid: c for mid, c in seen_counts.items() if c > 1}
    if observed_dupes:
        print(
            "OBSERVED at-least-once wire duplicate(s) "
            f"(messageId -> times echoed): {observed_dupes}"
        )

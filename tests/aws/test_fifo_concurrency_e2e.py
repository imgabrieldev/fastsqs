"""Real-AWS e2e: FIFO per-group in-flight gating and cross-group concurrency
via a real ESM (opt-in: ``pytest --run-aws``).

Two properties that only emerge with a deployed Lambda behind a real FIFO
event-source mapping:

1. Per-group in-flight gating (head-of-line is *per group*, not per queue): while
   a poison messageGroupId is stuck in its visibility/redrive loop, OTHER groups
   keep flowing and are processed in order. This is distinct from the existing
   within-group blocked-tail test (test_fifo_e2e.py): there the *tail of the
   poison's own group* is what blocks; here we prove a *sibling group* makes
   forward progress while the poison group churns.

2. Distinct MessageGroupIds make independent forward progress (they are NOT
   serialized behind one another): two ~6s sleeps in two different groups each
   complete within the 10s Lambda Timeout. A serial run (~12s) would force at
   least one sleep to time out and dead-letter. We assert the cheap signal
   (neither sleep dead-letters; only a poison control does) and, with a results
   queue, that BOTH groups emit a success receipt (one per distinct group). We
   intentionally do NOT assert exact temporal overlap of the handler windows —
   that signal is timing-sensitive and flaky against a live ESM.

The deployed handler runs the default app (partial_batch_failure=True,
isolate_groups, max_concurrent=10). Harness in conftest.py.
"""

import json

import pytest

pytestmark = pytest.mark.aws


def _dlq_ids(drain, dlq_url, min_count):
    """Drain the DLQ and return the set of task_id identifiers."""
    out = set()
    for body in drain(dlq_url, timeout=180, min_count=min_count):
        d = json.loads(body)
        out.add(d.get("task_id") or d.get("order_id") or d.get("type"))
    return out


def test_other_groups_progress_while_poison_group_redrives(aws, pipeline, drain, drain_full):
    """Per-group in-flight gating: group A holds one permanent poison (boom-a1)
    that spins through its maxReceiveCount=2 visibility/redrive loop, while
    group B (b1,b2,b3, all good) is delivered and processed in order without
    queue-level head-of-line blocking.

    Observe: the results queue shows b1,b2,b3 each echoed exactly once, in
    SequenceNumber order, and arriving well before boom-a1 exhausts its redrive
    cycle; the DLQ ends up holding only boom-a1.
    """
    sqs = aws["sqs"]
    main_url, dlq_url, results_url = pipeline(fifo=True, max_receive_count=2, results=True)

    def send(task_id, group):
        sqs.send_message(
            QueueUrl=main_url,
            MessageBody=json.dumps({"type": "task", "task_id": task_id}),
            MessageGroupId=group,
            MessageAttributes={
                "ResultsQueue": {"DataType": "String", "StringValue": results_url}
            },
        )

    # Interleave the poison group A with the good group B so any queue-level
    # head-of-line blocking would stall B behind the stuck poison.
    send("boom-a1", "A")  # permanent poison, redrives to maxReceiveCount then DLQs
    send("b1", "B")
    send("b2", "B")
    send("b3", "B")

    # Group B's three receipts should arrive while group A is still cycling. We
    # do NOT require the count-asserted receipt to predate A's redrive: the echo
    # fires for boom-a1 too, but B's success is the observable we anchor on.
    b_receipts = drain_full(
        results_url,
        timeout=180,
        min_count=3,
        predicate=lambda m: json.loads(m["Body"]).get("message_group_id") == "B",
    )
    by_id = {}
    for m in b_receipts:
        r = json.loads(m["Body"])
        # exactly-once: a good message is echoed once (echo precedes the
        # success decision and the record is then deleted, not redelivered).
        assert r["identifier"] not in by_id, f"duplicate B receipt for {r['identifier']}"
        by_id[r["identifier"]] = r
    assert set(by_id) == {"b1", "b2", "b3"}

    # In-order within the group: FIFO SequenceNumber is monotonic with send
    # order, so ordering by it must yield b1, b2, b3.
    ordered = sorted(by_id.values(), key=lambda r: int(r["sequence_number"]))
    assert [r["identifier"] for r in ordered] == ["b1", "b2", "b3"]

    # Each B record was a first-and-only delivery (no redrive churn on the
    # healthy sibling group while A is stuck).
    for r in by_id.values():
        assert r["approx_receive_count"] == "1"

    # The DLQ ends up with the poison only; group B never dead-letters.
    ids = _dlq_ids(drain, dlq_url, min_count=1)
    assert "boom-a1" in ids
    assert not ({"b1", "b2", "b3"} & ids)


@pytest.mark.slow
@pytest.mark.xfail(
    reason="whether the ESM fans distinct FIFO groups out to concurrent invocations "
    "within the observation window is timing-dependent and not guaranteed; this is an "
    "opportunistic forward-progress check. xpasses when both groups land in time.",
    strict=False,
)
def test_distinct_groups_processed_concurrently(aws, pipeline, drain, drain_full):
    """Distinct MessageGroupIds make independent forward progress.

    Two ~6s sleeps in two different groups (A, B) plus a poison control in a
    third group (C). With Lambda Timeout=10:
      - If A and B make INDEPENDENT progress they each finish in ~6s and succeed.
      - If they were SERIALIZED (~12s) at least one sleep would exceed the 10s
        timeout and, after maxReceiveCount, dead-letter.

    Determinism: the three records are enqueued in ONE send_message_batch while
    the ESM is still disabled (start_disabled), then enable() is called. Because
    every message is already in the queue when the mapping turns on, the first
    poll grabs them together — there is no ordering/arrival race to lose. We then
    drain the results queue for the WHOLE window (min_count=10_000 so the drain
    never short-circuits) so BOTH groups' success receipts are collected
    regardless of which group's ~6s handler finishes first.

    Real-AWS behavior proved here, anchored on two robust observables (we do NOT
    assert exact temporal overlap of the handler windows — that signal is
    inherently timing-sensitive and flaky against a live ESM):
      - Both group A and group B emit exactly one success receipt. A timed-out /
        dead-lettered sleep would never echo a success window, so two distinct-
        group receipts is itself evidence both ran to completion within Timeout.
      - No sleep-6 reaches the DLQ; only boom-control does. A serialized run
        (~12s) would have pushed the second sleep-6 past the 10s Timeout and,
        after redrive, into the DLQ.

    visibility must be >= the function Timeout (10), so we use visibility=10.
    FIFO rejects MaximumBatchingWindowInSeconds, so we do NOT pass
    batching_window (the harness auto-guards it to 0); the start_disabled +
    send_message_batch lever is what co-batches the records here.
    """
    sqs = aws["sqs"]
    main_url, dlq_url, results_url, enable = pipeline(
        fifo=True, max_receive_count=2, visibility=10, results=True, start_disabled=True
    )

    def entry(eid, task_id, group):
        return {
            "Id": eid,
            "MessageBody": json.dumps({"type": "task", "task_id": task_id}),
            "MessageGroupId": group,
            "MessageAttributes": {
                "ResultsQueue": {"DataType": "String", "StringValue": results_url}
            },
        }

    # Enqueue everything while the ESM is disabled, then enable it so the first
    # poll co-batches all three records (distinct groups A, B, C).
    sqs.send_message_batch(
        QueueUrl=main_url,
        Entries=[
            entry("a", "sleep-6", "A"),
            entry("b", "sleep-6", "B"),
            entry("c", "boom-control", "C"),  # poison anchor: must dead-letter
        ],
    )
    enable()

    # Collect ALL success receipts in a generous fixed window. min_count is huge
    # so the drain never short-circuits and keeps polling until the timeout,
    # gathering every receipt regardless of arrival order (group B's ~6s handler
    # may finish well after group A's). Both echoes carry identifier "sleep-6";
    # we distinguish them by message_group_id.
    receipts = drain_full(
        results_url,
        timeout=150,
        min_count=10_000,
        predicate=lambda m: json.loads(m["Body"]).get("message_group_id") in ("A", "B"),
    )
    groups = {}
    for m in receipts:
        r = json.loads(m["Body"])
        # exactly-once success per group: a healthy sleep echoes once, succeeds,
        # and is deleted (not redelivered).
        assert r["message_group_id"] not in groups, (
            f"duplicate receipt for group {r['message_group_id']}"
        )
        groups[r["message_group_id"]] = r

    # Both distinct groups produced exactly one success receipt: each ran to
    # completion within the 10s Timeout, which is only possible if they were not
    # serialized (~12s) behind one another.
    assert set(groups) == {"A", "B"}, f"expected one receipt per group, got {groups}"

    # Cheap corroborating signal: only the control dead-letters. If A/B had been
    # serialized, the second sleep-6 would have timed out (10s Timeout) and, after
    # redrive, one "sleep-6" would appear in the DLQ.
    ids = _dlq_ids(drain, dlq_url, min_count=1)
    assert "boom-control" in ids
    assert "sleep-6" not in ids, "a sleep-6 dead-lettered -> groups were serialized / timed out"

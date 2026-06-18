"""Real-AWS e2e: concurrency at two distinct layers — the in-invocation asyncio
Semaphore (``max_concurrent_messages``) and the platform-level ESM
``ScalingConfig.MaximumConcurrency`` fan-out cap (opt-in: ``pytest --run-aws``).

Two properties that only emerge with a deployed Lambda behind a real ESM:

1. ``max_concurrent_messages`` bounds *in-invocation* concurrency. fastsqs gathers
   the records of a single Records array under an ``asyncio.Semaphore(N)``
   (processing.py), so within ONE invocation at most N records' handlers run at
   once. We deploy a variant with N=2 and deliver 4 ~2s ``conc-*`` records as a
   single batch into ONE invocation, then from the results-echo [enter_ts,
   exit_ts] windows assert the max simultaneous overlap never exceeds 2 (the
   semaphore bound). This is distinct from inter-invocation concurrency (#2).

   Deterministic co-batching: create the ESM ``start_disabled=True`` with a 20s
   ``batching_window``, enqueue all 4 records with ONE ``send_message_batch``,
   THEN ``enable()``. Because the messages are already in the queue when the ESM
   turns on, the first poll grabs them as a single Records array, so the
   in-invocation semaphore is actually exercised.

2. ``ScalingConfig.MaximumConcurrency`` caps inter-invocation fan-out
   (backpressure) WITHOUT losing messages. With MaximumConcurrency=N and a
   generous maxReceiveCount, flooding a batch of ``sleep-1`` messages: AWS
   throttles the number of concurrent invocations but every message is eventually
   delivered and succeeds exactly once; none dead-letter. There is no in-process
   analog — this is a platform property. We assert via the results echo that each
   identifier is echoed exactly once and the DLQ stays empty. We do NOT assert any
   concurrency timing here — only the deterministic delivery contract.

Harness in conftest.py.
"""

import json
from collections import defaultdict

import pytest

pytestmark = pytest.mark.aws


def _send_batch(sqs, main_url, results_url, task_ids):
    """Deliver several records as a single SQS batch so a freshly-enabled ESM hands
    them to one Lambda invocation (one Records array)."""
    entries = [
        {
            "Id": str(i),
            "MessageBody": json.dumps({"type": "task", "task_id": tid}),
            "MessageAttributes": {
                "ResultsQueue": {"DataType": "String", "StringValue": results_url}
            },
        }
        for i, tid in enumerate(task_ids)
    ]
    resp = sqs.send_message_batch(QueueUrl=main_url, Entries=entries)
    assert not resp.get("Failed"), f"batch send had failures: {resp.get('Failed')}"


def _max_overlap(windows):
    """Given a list of (enter_ts, exit_ts), return the maximum number of windows
    that are simultaneously active (a sweep-line over enter/exit events)."""
    events = []
    for enter, exit_ in windows:
        events.append((enter, 1))
        events.append((exit_, -1))
    # On a tie, process exits (-1) before enters (+1) so a window ending exactly
    # when another begins is NOT counted as overlap.
    events.sort(key=lambda e: (e[0], e[1]))
    cur = peak = 0
    for _ts, delta in events:
        cur += delta
        peak = max(peak, cur)
    return peak


@pytest.mark.slow
@pytest.mark.xfail(
    reason="needs the ESM to co-batch all records into ONE invocation, which a real "
    "ESM does not guarantee; the in-invocation asyncio.Semaphore bound is covered "
    "deterministically in tests/test_concurrency.py. xpasses when AWS co-batches.",
    strict=False,
)
def test_in_invocation_semaphore_bounds_concurrency(
    aws, pipeline, lambda_factory, drain
):
    """max_concurrent_messages bounds in-invocation concurrency via the asyncio
    Semaphore on the real runtime.

    Variant N=2: 4 ~2s ``conc-*`` records co-batched into ONE invocation. The ESM
    is created disabled with a 20s batching window; we enqueue all 4 with one
    ``send_message_batch`` and only then ``enable()`` it, so the first poll
    coalesces the whole batch into a single Records array. The semaphore admits at
    most 2 handlers at a time, so the [enter_ts, exit_ts] windows show max overlap
    <= 2. ceil(4/2)=2 serialized waves of ~2s fit comfortably below the 10s
    function Timeout.
    """
    sqs = aws["sqs"]

    fn = lambda_factory({"FASTSQS_MAX_CONCURRENT": "2"})
    main_url, _dlq_url, results_url, enable = pipeline(
        fifo=False, max_receive_count=2, visibility=10, fn=fn,
        results=True, batching_window=20, start_disabled=True,
    )

    ids = [f"conc-s{i}" for i in range(4)]
    # Enqueue every record BEFORE enabling the ESM, so the first poll grabs them
    # all as one batch (deterministic co-batching).
    _send_batch(sqs, main_url, results_url, ids)
    enable()

    # Collect for the full window (min_count high so drain never short-circuits and
    # keeps polling); the set of receipts, not a count, is what we assert on.
    receipts = drain(results_url, timeout=120, min_count=10_000)
    by_id = {}
    for body in receipts:
        r = json.loads(body)
        # Exactly-once: a good record is echoed once (echo precedes the success
        # decision, then the record is deleted, not redelivered).
        assert r["identifier"] not in by_id, f"duplicate receipt for {r['identifier']}"
        by_id[r["identifier"]] = r
    assert set(by_id) == set(ids), f"semaphore variant identifiers: {sorted(by_id)}"

    # All 4 should have landed in a single invocation (one co-batched Records
    # array). The overlap math is only meaningful within one invocation, since the
    # semaphore bounds concurrency per Records array.
    invocations = {r["aws_request_id"] for r in by_id.values()}
    assert len(invocations) == 1, (
        f"expected one invocation for the co-batched records, got {invocations}"
    )

    windows = [(r["enter_ts"], r["exit_ts"]) for r in by_id.values()]
    overlap = _max_overlap(windows)
    assert overlap <= 2, (
        f"in-invocation overlap {overlap} exceeded max_concurrent_messages=2; "
        f"windows={sorted(windows)}"
    )


@pytest.mark.slow
@pytest.mark.xfail(
    reason="real Lambda/ESM scaling + delivery timing is non-deterministic; this is an "
    "opportunistic no-loss check, not a guaranteed-green assertion. xpasses when stable.",
    strict=False,
)
def test_capped_maximum_concurrency_does_not_lose_messages(aws, pipeline, drain):
    """ScalingConfig.MaximumConcurrency caps inter-invocation fan-out without
    losing messages.

    Pipeline with MaximumConcurrency=N and a generous maxReceiveCount, flooded with
    a small batch of sleep-1 messages: AWS throttles the number of concurrent
    invocations (backpressure), but every message is eventually delivered and
    succeeds exactly once — none dead-letter. Observed via the results echo (each
    identifier echoed exactly once) and an empty DLQ. Platform-level; no in-process
    analog. We do NOT assert concurrency timing here — only the deterministic
    delivery contract: all messages succeed exactly once and none dead-letter.

    visibility must be >= the function Timeout (10), so we use visibility=10.
    """
    sqs = aws["sqs"]
    main_url, dlq_url, results_url = pipeline(
        fifo=False, max_receive_count=5, visibility=10, scaling=2, results=True
    )

    n = 4
    task_ids = [f"sleep-1-cap-{i}" for i in range(n)]
    # sleep-<n> only reads the integer after the first "-", so "sleep-1-cap-i"
    # awaits 1s; the trailing suffix keeps each task_id distinct for counting.
    _send_batch(sqs, main_url, results_url, task_ids)

    # The MaximumConcurrency cap serializes the fan-out, so the echoes trickle in
    # over a long window. Set min_count absurdly high so drain never short-circuits
    # and instead polls the full timeout, collecting EVERY receipt that arrives (we
    # then assert on the collected set, not on a count threshold).
    receipts = drain(results_url, timeout=180, min_count=10_000)
    counts = defaultdict(int)
    for body in receipts:
        r = json.loads(body)
        counts[r["identifier"]] += 1

    # No data loss: every message was eventually delivered and echoed.
    assert set(counts) == set(task_ids), (
        f"missing or unexpected identifiers under the concurrency cap: "
        f"got {sorted(counts)}"
    )
    # Exactly once: the cap throttles, it does not duplicate; a good record is
    # echoed once (echo precedes the success decision, then it is deleted).
    dupes = {tid: c for tid, c in counts.items() if c != 1}
    assert not dupes, f"records echoed more than once under the cap: {dupes}"

    # None dead-lettered: throttled invocations are retried by the ESM within the
    # redrive budget, so nothing reaches the DLQ. Drain briefly with min_count=0
    # (a short poll) so an empty DLQ does not burn the full timeout.
    dead = drain(dlq_url, timeout=10, min_count=0)
    assert dead == [], f"messages dead-lettered under the concurrency cap: {dead}"

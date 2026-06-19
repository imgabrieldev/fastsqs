"""Real-AWS e2e: the VALUE of ReportBatchItemFailures, proven by contrast
(opt-in: ``pytest --run-aws``).

These tests isolate what partial-batch reporting buys you by running the SAME
co-batched batch through two functions and comparing what dead-letters:

- STRICT (``partial_batch_failure=False``): fastsqs raises ``BatchFailedError``
  on any record failure -> the invocation throws -> the real ESM has no
  ``batchItemFailures`` to act on, so it redelivers the WHOLE batch and every
  record (including the good siblings) eventually dead-letters.
- DEFAULT (``partial_batch_failure=True``): only the poison is reported, so only
  the poison dead-letters; the good siblings are deleted.

A function timeout is the same class of batch-level event: no return value means
no ``batchItemFailures``, so every co-batched record dead-letters. And under FIFO
``halt_batch`` the poison plus its entire arrival-tail (across groups) redrive,
vs ``isolate_groups`` which only blocks the poison's own group.

To force co-batching DETERMINISTICALLY into ONE invocation we use a FIFO queue
created with ``start_disabled=True``: enqueue every record with a single
``SendMessageBatch`` while the ESM is OFF, then call ``enable()`` so the first
poll grabs the whole group as one batch. (A standard queue scales out pollers and
splits the records, so it cannot guarantee co-batching — partial_batch_failure /
timeout / halt_batch are queue-agnostic, so FIFO is a valid, deterministic
substrate.) Counts stay tiny. Harness in conftest.py.
"""

import json

import pytest

pytestmark = pytest.mark.aws


def _entries(records):
    """Build SendMessageBatch entries from (task_id[, group]) tuples."""
    out = []
    for i, rec in enumerate(records):
        task_id, group = (rec if isinstance(rec, tuple) else (rec, None))
        entry = {
            "Id": f"m{i}",
            "MessageBody": json.dumps({"type": "task", "task_id": task_id}),
        }
        if group is not None:
            entry["MessageGroupId"] = group
        out.append(entry)
    return out


def _dlq_ids(drain, dlq_url, min_count, timeout=180):
    """Drain the DLQ and return the set of task_id identifiers."""
    out = set()
    for body in drain(dlq_url, timeout=timeout, min_count=min_count):
        out.add(json.loads(body)["task_id"])
    return out


def test_strict_mode_one_poison_redrives_whole_batch(aws, pipeline, lambda_factory, drain):
    """STRICT (partial_batch_failure=False) vs DEFAULT, same co-batched batch.

    Strict: fastsqs raises BatchFailedError -> unhandled top-level exception ->
    the ESM has nothing to delete and redrives the ENTIRE batch, so ok-A, ok-B
    AND boom-C all dead-letter. Default: only boom-C dead-letters; ok-A/ok-B are
    deleted. The delta is exactly the value of ReportBatchItemFailures.

    partial_batch_failure is queue-agnostic, so we run on a FIFO single-group queue
    purely to get DETERMINISTIC co-batching (start_disabled + one SendMessageBatch
    -> the first poll grabs the whole group as one invocation; a standard queue
    would split the records across pollers).
    """
    sqs = aws["sqs"]

    # STRICT pipeline bound to the partial_batch_failure=False function, FIFO so the
    # enqueue-before-enable group co-batches deterministically into ONE invocation.
    strict_fn = lambda_factory({"FASTSQS_PARTIAL": "0"})
    strict_main, strict_dlq, strict_enable = pipeline(
        fifo=True, max_receive_count=1, fn=strict_fn, start_disabled=True
    )
    # DEFAULT pipeline (partial_batch_failure=True) as the control.
    ctrl_main, ctrl_dlq, ctrl_enable = pipeline(
        fifo=True, max_receive_count=1, start_disabled=True
    )

    # One atomic SendMessageBatch per queue (single group G) while the ESMs are OFF,
    # so the first poll after enable() coalesces all three into one invocation.
    batch = _entries([("ok-A", "G"), ("ok-B", "G"), ("boom-C", "G")])
    sqs.send_message_batch(QueueUrl=strict_main, Entries=batch)
    sqs.send_message_batch(QueueUrl=ctrl_main, Entries=batch)

    # Enable AFTER enqueue: the first poll grabs the already-queued records as one
    # batch (deterministic co-batching).
    strict_enable()
    ctrl_enable()

    # Strict: the whole batch redrives -> all three dead-letter.
    strict_ids = _dlq_ids(drain, strict_dlq, min_count=3)
    assert {"ok-A", "ok-B", "boom-C"} <= strict_ids, (
        "strict mode raises BatchFailedError -> whole batch redrives -> "
        "good siblings dead-letter too"
    )

    # Default control: only the poison dead-letters.
    ctrl_ids = _dlq_ids(drain, ctrl_dlq, min_count=1)
    assert "boom-C" in ctrl_ids
    assert "ok-A" not in ctrl_ids and "ok-B" not in ctrl_ids, (
        "default partial-batch reporting deletes the good siblings"
    )


def test_function_timeout_redrives_whole_cobatched_batch(aws, pipeline, drain):
    """A function timeout is a batch-level event: no return value -> no
    batchItemFailures -> every co-batched record (incl. fast ok-A/ok-B) redrives
    and dead-letters. Extends the single-record timeout test to prove the good
    siblings die too because they were batched with the timed-out record.

    FIFO single-group + start_disabled gives deterministic co-batching (a standard
    queue would split the records across pollers and defeat the point).
    """
    sqs = aws["sqs"]
    main_url, dlq_url, enable = pipeline(
        fifo=True, max_receive_count=1, start_disabled=True
    )

    # One SendMessageBatch (single group G) so sleep-15 + ok-A + ok-B coalesce into
    # one invocation that the 10s Timeout kills before any return value is produced.
    sqs.send_message_batch(
        QueueUrl=main_url, Entries=_entries([("sleep-15", "G"), ("ok-A", "G"), ("ok-B", "G")])
    )

    # Enable AFTER enqueue: the first poll grabs all three as one batch.
    enable()

    ids = _dlq_ids(drain, dlq_url, min_count=3)
    assert {"sleep-15", "ok-A", "ok-B"} <= ids, (
        "timeout has no batchItemFailures, so ok-A/ok-B that were batched with "
        "the timed-out record also redrive to the DLQ"
    )


def test_fifo_halt_batch_redrives_full_arrival_tail(aws, pipeline, lambda_factory, drain):
    """FIFO halt_batch vs isolate_groups, same arrival order across groups.

    halt_batch: the poison AND every record AFTER it in arrival order (across
    groups) dead-letter, while the record before it succeeds. isolate_groups (the
    default): only the poison's OWN group blocks; an arrival-after record in a
    different group succeeds. Real-ESM ordering makes the halted tail observable.
    """
    sqs = aws["sqs"]

    # halt_batch variant bound to its own FIFO pipeline. start_disabled=True buffers
    # the SendMessageBatch behind the OFF ESM so enable() pulls all three groups in
    # ONE invocation, preserving arrival order within the co-batched batch that
    # halt_batch acts on. No batching_window: FIFO rejects it (auto-guarded to 0).
    halt_fn = lambda_factory({"FASTSQS_FIFO_MODE": "halt_batch"})
    halt_main, halt_dlq, halt_enable = pipeline(
        fifo=True, max_receive_count=2, fn=halt_fn, start_disabled=True
    )
    # isolate_groups (default) FIFO pipeline as the contrast.
    iso_main, iso_dlq, iso_enable = pipeline(
        fifo=True, max_receive_count=2, start_disabled=True
    )

    # Arrival order: a1 (group A) -> boom-b (group B, poison) -> c1 (group C).
    # c1 arrives AFTER the poison but in a different group. One SendMessageBatch so
    # all three coalesce into a single invocation once enabled.
    batch = _entries([("a1", "A"), ("boom-b", "B"), ("c1", "C")])
    sqs.send_message_batch(QueueUrl=halt_main, Entries=batch)
    sqs.send_message_batch(QueueUrl=iso_main, Entries=batch)

    # Enable AFTER enqueue: the first poll grabs the already-queued records as one
    # batch in arrival order.
    halt_enable()
    iso_enable()

    # halt_batch: poison + arrival-after tail (c1) dead-letter; a1 (before) does not.
    halt_ids = _dlq_ids(drain, halt_dlq, min_count=2)
    assert "boom-b" in halt_ids
    assert "c1" in halt_ids, "halt_batch dead-letters the arrival-after tail across groups"
    assert "a1" not in halt_ids, "the record arriving before the poison still succeeds"

    # isolate_groups: only the poison's own group is affected; c1 (different
    # group, arrival-after) succeeds and is NOT dead-lettered.
    iso_ids = _dlq_ids(drain, iso_dlq, min_count=1)
    assert "boom-b" in iso_ids
    assert "a1" not in iso_ids and "c1" not in iso_ids, (
        "isolate_groups only blocks the poison's own group; other groups survive"
    )

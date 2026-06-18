"""Real-AWS e2e: the ESM partial-batch RESPONSE CONTRACT (opt-in: ``pytest --run-aws``).

These pin the exact serialization fastsqs hands the event-source mapping. They
can only be proven against a real ESM because the ESM — not fastsqs — interprets
``{"batchItemFailures": [{"itemIdentifier": <sqs messageId>}]}`` and decides which
records to delete vs redeliver:

- The default handler keys ``itemIdentifier`` on the SQS *messageId* (not the
  internal task_id). A real ESM deletes exactly the records NOT reported, so a
  task_id-vs-messageId regression (invisible in-process, where the same string is
  echoed back) would here cause every record to redrive. We send 5 successes + 1
  poison and prove only the poison dead-letters.
- A structurally defective response (empty/blank ``itemIdentifier``) flips the
  WHOLE batch to complete failure per the AWS docs full-batch-failure matrix —
  even though every record's business logic succeeded. The ``FASTSQS_CORRUPT=1``
  handler variant emits exactly that; a control on the well-formed default handler
  (same input) dead-letters nothing.

Harness + fixtures live in tests/aws/conftest.py; the deployed handler is
tests/aws/_e2e_handler.py.
"""

import json

import pytest

pytestmark = pytest.mark.aws


def _attr(results_url):
    return {"ResultsQueue": {"DataType": "String", "StringValue": results_url}}


def test_succeeded_records_are_deleted_not_redriven(aws, pipeline, drain):
    """The real ESM honors fastsqs's {batchItemFailures:[{itemIdentifier: messageId}]}
    shape: the messageId-keyed identifiers delete exactly the unreported set. The 5
    ok-* records are deleted (each processed once, never redriven) while only the
    lone boom-C is kept and dead-lettered. A task_id-vs-messageId regression — which
    in-process tests cannot catch because the echoed string is identical — would here
    redrive every record."""
    sqs = aws["sqs"]
    main_url, dlq_url, results_url = pipeline(max_receive_count=2, results=True)

    ok_ids = [f"ok-{i}" for i in range(5)]
    for tid in (*ok_ids, "boom-C"):
        sqs.send_message(
            QueueUrl=main_url,
            MessageBody=json.dumps({"type": "task", "task_id": tid}),
            MessageAttributes=_attr(results_url),
        )

    # The results echo fires for every record BEFORE the pass/fail decision, so we
    # observe SUCCESS positively. Drain a FIXED window long enough that a redrive
    # would have manifested (> visibility timeout x maxReceiveCount): min_count is
    # set huge so drain waits the whole window and returns EVERY receipt (stragglers
    # included), letting us assert each ok-* echoed exactly once (no redrive).
    receipts = drain(results_url, timeout=90, min_count=10_000)
    by_id: dict[str, int] = {}
    for body in receipts:
        ident = json.loads(body)["identifier"]
        by_id[ident] = by_id.get(ident, 0) + 1

    # Every ok-* was processed; messageId-keyed deletion means each is processed
    # exactly once (a redrive of a "succeeded" record would echo a second receipt).
    for tid in ok_ids:
        assert tid in by_id, f"{tid} should have been processed and echoed a receipt"
        assert by_id[tid] == 1, (
            f"{tid} echoed {by_id[tid]} receipts; >1 means a succeeded record was "
            "redelivered -> the ESM did not delete it -> messageId keying is broken"
        )

    # boom-C is the only record kept and dead-lettered; the 5 ok-* never redrive.
    moved = drain(dlq_url, timeout=180, min_count=1)
    dlq_ids = {json.loads(b)["task_id"] for b in moved}
    assert "boom-C" in dlq_ids, "the unreported failure should dead-letter"
    assert not (set(ok_ids) & dlq_ids), "reported-as-succeeded records must not redrive"

    # After settle the main queue holds nothing in-flight or visible: every record
    # reached a terminal state (deleted or dead-lettered), none is mid-redelivery.
    attrs = sqs.get_queue_attributes(
        QueueUrl=main_url,
        AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"],
    )["Attributes"]
    assert int(attrs["ApproximateNumberOfMessagesNotVisible"]) == 0, (
        f"main queue still has in-flight messages: {attrs}"
    )
    assert int(attrs["ApproximateNumberOfMessages"]) == 0, (
        f"main queue still has visible messages: {attrs}"
    )


def test_defective_batchItemFailures_shape_redrives_whole_batch(aws, pipeline, lambda_factory, drain):
    """AWS full-batch-failure matrix: a structurally defective response (here a blank
    itemIdentifier) flips the WHOLE batch to complete failure even though every
    record's business logic succeeded. The FASTSQS_CORRUPT=1 variant emits that
    defect; all 3 GOOD records dead-letter. A control on the well-formed default
    handler (same 3 records) dead-letters nothing — pinning fastsqs's serialization
    contract: the difference is purely the response shape, not the inputs."""
    sqs = aws["sqs"]

    corrupt_fn = lambda_factory({"FASTSQS_CORRUPT": "1"})
    corrupt_main, corrupt_dlq = pipeline(max_receive_count=1, fn=corrupt_fn)
    control_main, control_dlq = pipeline(max_receive_count=1)

    good = [f"good-{i}" for i in range(3)]
    for tid in good:
        body = json.dumps({"type": "task", "task_id": tid})
        sqs.send_message(QueueUrl=corrupt_main, MessageBody=body)
        sqs.send_message(QueueUrl=control_main, MessageBody=body)

    # Corrupt handler: blank itemIdentifier => the ESM treats the whole batch as
    # failed, so all 3 succeeding-business-logic records redrive (maxReceiveCount=1).
    moved = drain(corrupt_dlq, timeout=180, min_count=3)
    corrupt_ids = {json.loads(b)["task_id"] for b in moved}
    assert set(good) <= corrupt_ids, (
        f"defective response should dead-letter the whole batch; DLQ had {corrupt_ids}"
    )

    # Control: identical inputs on the well-formed default handler dead-letter
    # nothing. A short drain on an empty DLQ returns [] at timeout.
    control_moved = drain(control_dlq, timeout=30, min_count=1)
    control_ids = {json.loads(b)["task_id"] for b in control_moved}
    assert not (set(good) & control_ids), (
        f"well-formed response must not dead-letter succeeding records; DLQ had {control_ids}"
    )

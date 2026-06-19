"""Real-AWS e2e: ESM FilterCriteria drops non-matching records BEFORE the Lambda
(opt-in: ``pytest --run-aws``).

When the event-source mapping has a content filter, SQS evaluates it and DISCARDS
non-matching messages without invoking the Lambda — fastsqs never sees them, and
they do NOT go to the DLQ. This can only be proven against a real ESM (the filter
lives on the mapping, not in fastsqs). We attach a filter that keeps only
``body.type == "task"`` and send a matching task, a non-matching order, and a
poison-task control: the task is processed (echoes a receipt), the order is
filtered out (no receipt, no DLQ), and the control redrives — anchoring the
timing so the order's ABSENCE is meaningful.

Harness + fixtures in tests/aws/conftest.py; deployed handler tests/aws/_e2e_handler.py.
"""

import json

import pytest

pytestmark = pytest.mark.aws


def _attr(results_url):
    return {"ResultsQueue": {"DataType": "String", "StringValue": results_url}}


def test_filter_criteria_drops_nonmatching_before_lambda(aws, pipeline, drain, drain_full):
    sqs = aws["sqs"]
    # Keep only messages whose JSON body has type == "task"; everything else is
    # discarded by the ESM before the Lambda is invoked.
    main_url, dlq_url, results_url = pipeline(
        max_receive_count=2,
        results=True,
        filter_criteria={"Filters": [{"Pattern": json.dumps({"body": {"type": ["task"]}})}]},
    )

    def send(body):
        sqs.send_message(
            QueueUrl=main_url, MessageBody=json.dumps(body), MessageAttributes=_attr(results_url)
        )

    send({"type": "task", "task_id": "keep"})           # matches filter -> processed
    send({"type": "order", "order_id": "dropped"})      # no match -> discarded pre-Lambda
    send({"type": "task", "task_id": "boom-control"})   # matches -> fails -> DLQ (anchor)

    # Wide fixed window: collect every receipt that the Lambda emits. "dropped"
    # never reaches the handler, so it must never echo.
    receipts = drain_full(results_url, timeout=120, min_count=10_000)
    echoed = {json.loads(m["Body"])["identifier"] for m in receipts}

    assert "keep" in echoed, "the matching task should be processed and echo a receipt"
    assert "dropped" not in echoed, "the non-matching order must be filtered out before the Lambda"

    # The control (a matching task that fails) dead-letters; the filtered-out order
    # is NOT in the DLQ (it was discarded by the ESM, not failed by fastsqs).
    moved = drain(dlq_url, timeout=180, min_count=1)
    dlq_ids = {json.loads(b).get("task_id") or json.loads(b).get("order_id") for b in moved}
    assert "boom-control" in dlq_ids, "the poison control should dead-letter"
    assert "dropped" not in dlq_ids, "a filtered-out message must not reach the DLQ"

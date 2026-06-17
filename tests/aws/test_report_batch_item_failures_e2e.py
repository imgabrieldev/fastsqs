"""Real-AWS e2e: ReportBatchItemFailures through a real SQS -> Lambda ESM
(opt-in: ``pytest --run-aws``).

The flagship test: send a mixed batch to a real queue whose event-source mapping
triggers a deployed fastsqs Lambda. fastsqs returns only the failed records in
``batchItemFailures``; the ESM then deletes the succeeded ones and keeps
redelivering the failed one until it exceeds maxReceiveCount and SQS redrives it
to the DLQ. We assert on the DLQ only (no competition with the ESM on the main
queue). Harness + fixtures live in tests/aws/conftest.py.
"""

import json

import pytest

pytestmark = pytest.mark.aws


def test_only_failed_records_redrive_to_dlq(aws, pipeline, drain):
    sqs = aws["sqs"]
    main_url, dlq_url = pipeline(max_receive_count=2)

    for tid in ("ok-A", "ok-B", "boom-C"):
        sqs.send_message(QueueUrl=main_url, MessageBody=json.dumps({"type": "task", "task_id": tid}))

    moved = drain(dlq_url, timeout=120)
    task_ids = {json.loads(b)["task_id"] for b in moved}

    assert "boom-C" in task_ids, "the failed record should be redriven to the DLQ"
    assert "ok-A" not in task_ids and "ok-B" not in task_ids, "succeeded records must not reach the DLQ"

"""Real-AWS e2e: mixed failure modes + Lambda timeout (opt-in: ``pytest --run-aws``).

Both exercise the deployed fastsqs Lambda behind a real SQS ESM with
ReportBatchItemFailures; failing/timed-out records redrive to the DLQ while
successful ones are deleted. Harness in tests/aws/conftest.py.
"""

import json

import pytest

pytestmark = pytest.mark.aws


def test_validation_failure_and_handler_failure_both_redrive(aws, pipeline, drain):
    """A malformed body (InvalidMessageError) and a handler exception both fail and
    redrive to the DLQ; a valid/successful record does not."""
    sqs = aws["sqs"]
    main_url, dlq_url = pipeline(max_receive_count=2)

    sqs.send_message(QueueUrl=main_url, MessageBody=json.dumps({"type": "task", "task_id": "ok-A"}))
    sqs.send_message(QueueUrl=main_url, MessageBody="{not valid json")  # -> InvalidMessageError
    sqs.send_message(QueueUrl=main_url, MessageBody=json.dumps({"type": "task", "task_id": "boom-C"}))

    moved = drain(dlq_url, timeout=150, min_count=2)
    blob = " || ".join(moved)
    assert "{not valid json" in blob   # validation failure redriven
    assert "boom-C" in blob            # handler failure redriven
    assert "ok-A" not in blob          # success not redriven


def test_lambda_timeout_redrives_message(aws, pipeline, drain):
    """A handler that runs past the Lambda timeout (sleep-15 vs 10s timeout)
    never returns success -> SQS redelivers and eventually redrives to the DLQ."""
    sqs = aws["sqs"]
    main_url, dlq_url = pipeline(max_receive_count=1)

    sqs.send_message(QueueUrl=main_url, MessageBody=json.dumps({"type": "task", "task_id": "sleep-15"}))

    moved = drain(dlq_url, timeout=150)
    assert any("sleep-15" in b for b in moved), "timed-out message should redrive to the DLQ"

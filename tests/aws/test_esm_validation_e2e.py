"""Real-AWS e2e: server-side validation at the SQS/ESM boundary
(opt-in: ``pytest --run-aws``).

These assert what AWS itself rejects or defers BEFORE fastsqs ever runs, so they
document the harness's defaults and the broker contract fastsqs depends on:

- ``create_event_source_mapping`` refuses a queue whose VisibilityTimeout is below
  the function Timeout (why ``pipeline`` defaults visibility=10, matching the
  deployed fn Timeout=10, and why lowering it must be guarded).
- a FIFO queue rejects per-message ``DelaySeconds`` (only queue-level delay is
  allowed) — pure broker validation, no Lambda.
- on a standard queue ``DelaySeconds`` withholds the first delivery, so a later
  non-delayed message reaches the Lambda (and the DLQ) first; fastsqs never sees a
  message until SQS releases it.

The first two are near-zero cost (raw boto, no ESM enable wait, no drain). The
third reuses one standard pipeline. Harness in conftest.py.
"""

import json
import uuid

import pytest
from botocore.exceptions import ClientError

pytestmark = pytest.mark.aws


def test_esm_rejects_visibility_below_function_timeout(aws, deployed_lambda):
    """AWS server-side ESM validation: create_event_source_mapping refuses a
    standard queue whose VisibilityTimeout (5) is below the deployed function's
    Timeout (10). No binding is established (the create call raises), so there is
    nothing to enable or drain. This is exactly why ``pipeline`` defaults
    visibility=10 and why a future lowering must be guarded.
    """
    sqs, lam = aws["sqs"], aws["lambda"]
    queue_url = sqs.create_queue(
        QueueName=f"fastsqs-esmvis-{uuid.uuid4().hex[:8]}",
        Attributes={"VisibilityTimeout": "5"},  # < deployed fn Timeout=10
    )["QueueUrl"]
    try:
        queue_arn = sqs.get_queue_attributes(
            QueueUrl=queue_url, AttributeNames=["QueueArn"]
        )["Attributes"]["QueueArn"]

        with pytest.raises(ClientError) as exc:
            lam.create_event_source_mapping(
                EventSourceArn=queue_arn,
                FunctionName=deployed_lambda,
                Enabled=True,
                BatchSize=10,
                FunctionResponseTypes=["ReportBatchItemFailures"],
            )

        err = exc.value.response["Error"]
        assert err["Code"] == "InvalidParameterValueException"
        # AWS spells out the visibility-vs-function-timeout rule in the message.
        assert "visibility timeout" in err["Message"].lower()
        assert "function timeout" in err["Message"].lower()
    finally:
        sqs.delete_queue(QueueUrl=queue_url)


def test_fifo_rejects_per_message_delay_seconds(aws):
    """A FIFO queue rejects per-message DelaySeconds: only queue-level
    DelaySeconds is allowed on FIFO. Pure broker validation (no Lambda, no ESM):
    SendMessage raises ClientError InvalidParameterValue. Near-zero cost.
    """
    sqs = aws["sqs"]
    queue_url = sqs.create_queue(
        QueueName=f"fastsqs-fifodelay-{uuid.uuid4().hex[:8]}.fifo",
        Attributes={"FifoQueue": "true", "ContentBasedDeduplication": "true"},
    )["QueueUrl"]
    try:
        with pytest.raises(ClientError) as exc:
            sqs.send_message(
                QueueUrl=queue_url,
                MessageBody=json.dumps({"type": "task", "task_id": "fifo-delay-probe"}),
                MessageGroupId="g",
                DelaySeconds=5,  # per-message delay is illegal on FIFO
            )
        assert exc.value.response["Error"]["Code"] == "InvalidParameterValue"
    finally:
        sqs.delete_queue(QueueUrl=queue_url)


def test_standard_delay_seconds_defers_first_delivery(aws, pipeline, drain):
    """On a standard queue, per-message DelaySeconds withholds the first delivery,
    so a later non-delayed message is released (and reaches the Lambda + DLQ)
    before the delayed one. Both are poison with max_receive_count=1, so both
    dead-letter; draining the DLQ in arrival order shows boom-now ahead of
    boom-delayed. fastsqs never sees a message until SQS releases it.
    """
    sqs = aws["sqs"]
    main_url, dlq_url = pipeline(fifo=False, max_receive_count=1)

    # Delayed poison first, then immediately a non-delayed poison. SQS holds the
    # delayed one for 8s, so the non-delayed one is delivered (and dead-lettered)
    # first.
    sqs.send_message(
        QueueUrl=main_url,
        MessageBody=json.dumps({"type": "task", "task_id": "boom-delayed"}),
        DelaySeconds=8,
    )
    sqs.send_message(
        QueueUrl=main_url,
        MessageBody=json.dumps({"type": "task", "task_id": "boom-now"}),
    )

    # Drain returns bodies in arrival order (delete-as-read); assert the
    # non-delayed message dead-letters before the delayed one. Both eventually
    # arrive (min_count=2).
    bodies = drain(dlq_url, timeout=180, min_count=2)
    order = [json.loads(b)["task_id"] for b in bodies]

    assert "boom-now" in order and "boom-delayed" in order  # both dead-letter
    assert order.index("boom-now") < order.index("boom-delayed")  # now before delayed

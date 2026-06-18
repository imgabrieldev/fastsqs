"""Real-AWS e2e: DLQ copy fidelity across a REAL redrive (opt-in: ``pytest --run-aws``).

When a poison message exhausts ``maxReceiveCount`` on the main queue, SQS moves a
COPY of it to the dead-letter queue. These tests prove what survives that real
redrive (as opposed to a same-queue round-trip): the body is byte-identical, the
sender's custom MessageAttributes (String + Number) come along, and the system
Attributes on the dead-lettered copy carry the full receive tally plus the
original timestamps. The FIFO variant additionally proves MessageGroupId and
MessageDeduplicationId travel on the wire — the standard-vs-FIFO divergence.

The deployed handler fails any "boom*" task, so a poison message redrives to the
DLQ after ``maxReceiveCount`` deliveries. Harness in conftest.py.
"""

import json

import pytest

pytestmark = pytest.mark.aws


def test_dlq_copy_preserves_body_and_message_attributes_standard(aws, pipeline, drain_full):
    """A standard poison message redriven to the DLQ keeps its exact body, its
    custom String + Number MessageAttributes, the full receive count, and the
    original SentTimestamp / ApproximateFirstReceiveTimestamp."""
    sqs = aws["sqs"]
    main_url, dlq_url = pipeline(fifo=False, max_receive_count=2)

    body = json.dumps({"type": "task", "task_id": "boom-C", "marker": "dlq-fidelity"})
    sqs.send_message(
        QueueUrl=main_url,
        MessageBody=body,
        MessageAttributes={
            "RequestId": {"DataType": "String", "StringValue": "req-X"},
            "Priority": {"DataType": "Number", "StringValue": "5"},
        },
    )

    def is_boom_c(m):
        try:
            return json.loads(m["Body"]).get("task_id") == "boom-C"
        except (KeyError, ValueError):
            return False

    msgs = drain_full(dlq_url, timeout=180, min_count=1, predicate=is_boom_c)
    assert msgs, "poison boom-C never reached the DLQ"
    m = msgs[0]

    # Body is byte-identical across the redrive.
    assert m["Body"] == body

    # Custom MessageAttributes survive (String + Number), with dataType preserved.
    mattrs = m["MessageAttributes"]
    assert mattrs["RequestId"]["StringValue"] == "req-X"
    assert mattrs["RequestId"]["DataType"] == "String"
    assert mattrs["Priority"]["DataType"] == "Number"
    # Number attributes ride on the wire as a string value.
    assert mattrs["Priority"]["StringValue"] == "5"

    # System Attributes reflect the full receive tally and carry the timestamps.
    sysattrs = m["Attributes"]
    # maxReceiveCount=2 -> two deliveries on the main queue exhaust the policy and
    # the dead-lettered copy reports ApproximateReceiveCount == 3.
    assert int(sysattrs["ApproximateReceiveCount"]) == 3
    assert sysattrs.get("SentTimestamp")
    assert sysattrs.get("ApproximateFirstReceiveTimestamp")


def test_fifo_dlq_copy_preserves_group_and_dedup_ids(aws, pipeline, drain_full):
    """A FIFO poison message redriven to the (FIFO) DLQ preserves its
    MessageGroupId and MessageDeduplicationId on the wire, and the dead-lettered
    copy's ApproximateReceiveCount reflects the full tally."""
    sqs = aws["sqs"]
    # content_dedup=False so an explicit MessageDeduplicationId is required on send.
    main_url, dlq_url = pipeline(fifo=True, max_receive_count=2, content_dedup=False)

    body = json.dumps({"type": "task", "task_id": "boom-g"})
    sqs.send_message(
        QueueUrl=main_url,
        MessageBody=body,
        MessageGroupId="G",
        MessageDeduplicationId="dedup-boom-g",
    )

    def is_boom_g(m):
        try:
            return json.loads(m["Body"]).get("task_id") == "boom-g"
        except (KeyError, ValueError):
            return False

    msgs = drain_full(dlq_url, timeout=180, min_count=1, predicate=is_boom_g)
    assert msgs, "poison boom-g never reached the FIFO DLQ"
    m = msgs[0]

    sysattrs = m["Attributes"]
    assert sysattrs["MessageGroupId"] == "G"
    assert sysattrs.get("MessageDeduplicationId")
    # Full receive tally on the dead-lettered copy (string, PascalCase Attributes).
    assert sysattrs["ApproximateReceiveCount"] == "3"

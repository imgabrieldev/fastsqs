"""Real-AWS: explicit MessageDeduplicationId + DeduplicationScope on SQS FIFO
(opt-in: ``pytest --run-aws``).

The inverse of the content-based dedup test: on a FIFO queue WITHOUT
ContentBasedDeduplication, SQS keys deduplication on the explicit
MessageDeduplicationId carried by each send (within the 5-minute window), NOT on
body content. So two identical bodies under distinct ids both deliver, while two
sends with the same id collapse to one (the second SendMessage still returns 200
but enqueues nothing). DeduplicationScope then controls whether that id space is
per-queue (the default: same id across groups collapses) or per messageGroup
(same id in different groups both deliver).

These are raw SQS send + receive_message probes — no Lambda, no ESM, no DLQ — so
they assert exactly what the broker admits, which is precisely the set of records
fastsqs would ever see. Cheap: throwaway queues, direct receive. Uses the ``gabe``
profile.
"""

import json
import time
import uuid

import boto3
import pytest

pytestmark = pytest.mark.aws

REGION = "us-east-1"
PROFILE = "gabe"


@pytest.fixture(scope="module")
def sqs():
    return boto3.Session(profile_name=PROFILE, region_name=REGION).client("sqs")


@pytest.fixture
def make_queue(sqs):
    """Create throwaway FIFO queues (explicit-dedup: ContentBasedDeduplication
    OFF) and tear them all down. ``attrs`` lets a test set DeduplicationScope /
    FifoThroughputLimit together at creation."""
    created = []

    def make(**attrs):
        url = sqs.create_queue(
            QueueName=f"fastsqs-dedupid-{uuid.uuid4().hex[:8]}.fifo",
            Attributes={"FifoQueue": "true", **attrs},
        )["QueueUrl"]
        created.append(url)
        return url

    try:
        yield make
    finally:
        for url in created:
            try:
                sqs.delete_queue(QueueUrl=url)
            except Exception:
                pass


def _receive_all(sqs, url, *, timeout=20):
    """Drain everything the broker will hand out within ``timeout`` seconds,
    delete-as-read, and return the parsed task_id list (order not asserted)."""
    got = []
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10, WaitTimeSeconds=2)
        msgs = r.get("Messages", [])
        for m in msgs:
            got.append(json.loads(m["Body"])["task_id"])
            sqs.delete_message(QueueUrl=url, ReceiptHandle=m["ReceiptHandle"])
    return got


def test_explicit_dedup_id_distinct_ids_both_deliver_same_collapse(sqs, make_queue):
    """content_dedup=False FIFO: dedup is keyed on the explicit
    MessageDeduplicationId in the 5-min window, not on body content.

    (a) identical body B sent with distinct ids d1/d2 -> BOTH deliver (2 msgs);
    (b) identical body B sent twice with the same id d1 -> collapses to ONE
        (the second SendMessage returns 200 but enqueues nothing).
    """
    body = json.dumps({"type": "task", "task_id": "dedup-id-probe"})
    group = "g"

    # (a) distinct dedup ids, identical body -> both admitted.
    q_distinct = make_queue()
    sqs.send_message(
        QueueUrl=q_distinct, MessageBody=body, MessageGroupId=group,
        MessageDeduplicationId="d1",
    )
    sqs.send_message(
        QueueUrl=q_distinct, MessageBody=body, MessageGroupId=group,
        MessageDeduplicationId="d2",
    )
    distinct = _receive_all(sqs, q_distinct)
    assert distinct == ["dedup-id-probe", "dedup-id-probe"]  # 2 deliveries

    # (b) same dedup id twice -> the second send is accepted (200) but dropped.
    q_same = make_queue()
    r1 = sqs.send_message(
        QueueUrl=q_same, MessageBody=body, MessageGroupId=group,
        MessageDeduplicationId="d1",
    )
    r2 = sqs.send_message(
        QueueUrl=q_same, MessageBody=body, MessageGroupId=group,
        MessageDeduplicationId="d1",
    )
    # Both calls succeed at the API level; dedup returns the SAME MessageId for
    # the collapsed second send (SQS-observed truth).
    assert r1["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert r2["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert r1["MessageId"] == r2["MessageId"]
    same = _receive_all(sqs, q_same)
    assert same == ["dedup-id-probe"]  # collapsed to a single delivery


def test_dedup_scope_per_group_vs_queue(sqs, make_queue):
    """DeduplicationScope governs the id space for the same explicit dedup id
    sent to two different message groups:

    - queue scope (default): same id across groups collapses to ONE delivery;
    - messageGroup scope: same id per group yields TWO deliveries.

    FifoThroughputLimit=perMessageGroupId and DeduplicationScope=messageGroup are
    set TOGETHER at creation (AWS requires the throughput limit to be
    perMessageGroupId for messageGroup-scoped dedup). We assert GetQueueAttributes
    reflects each scope before relying on the receive counts.
    """
    body = json.dumps({"type": "task", "task_id": "scope-probe"})
    dedup_id = "d1"

    # --- queue scope (default): one shared dedup id space across all groups ---
    q_queue = make_queue()
    qattrs = sqs.get_queue_attributes(
        QueueUrl=q_queue,
        AttributeNames=["DeduplicationScope", "FifoThroughputLimit"],
    )["Attributes"]
    # Default scope is queue (AWS may omit it when defaulted; treat absence as
    # the documented default rather than asserting an explicit string).
    assert qattrs.get("DeduplicationScope", "queue") == "queue"
    sqs.send_message(
        QueueUrl=q_queue, MessageBody=body, MessageGroupId="A",
        MessageDeduplicationId=dedup_id,
    )
    sqs.send_message(
        QueueUrl=q_queue, MessageBody=body, MessageGroupId="B",
        MessageDeduplicationId=dedup_id,
    )
    queue_scope = _receive_all(sqs, q_queue)
    assert queue_scope == ["scope-probe"]  # same id collapses across groups

    # --- messageGroup scope: per-group dedup id space ---
    q_group = make_queue(
        DeduplicationScope="messageGroup",
        FifoThroughputLimit="perMessageGroupId",
    )
    gattrs = sqs.get_queue_attributes(
        QueueUrl=q_group,
        AttributeNames=["DeduplicationScope", "FifoThroughputLimit"],
    )["Attributes"]
    assert gattrs["DeduplicationScope"] == "messageGroup"
    assert gattrs["FifoThroughputLimit"] == "perMessageGroupId"
    sqs.send_message(
        QueueUrl=q_group, MessageBody=body, MessageGroupId="A",
        MessageDeduplicationId=dedup_id,
    )
    sqs.send_message(
        QueueUrl=q_group, MessageBody=body, MessageGroupId="B",
        MessageDeduplicationId=dedup_id,
    )
    group_scope = _receive_all(sqs, q_group)
    assert sorted(group_scope) == ["scope-probe", "scope-probe"]  # both groups deliver

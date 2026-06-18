"""Real-AWS e2e: FIFO poison-message behavior via a real ESM
(opt-in: ``pytest --run-aws``).

Under ``fifo_failure_mode="isolate_groups"`` (default) with a redrive policy, a
poison message blocks the rest of ITS messageGroupId, so when the group is
delivered as one batch the poison AND its blocked tail share the same
receiveCount and BOTH dead-letter at maxReceiveCount — the tail never gets a
poison-free batch. Messages BEFORE the poison succeed, and OTHER message groups
are unaffected. This is the inherent FIFO+DLQ ordering tradeoff.

The whole group is enqueued BEFORE the ESM is enabled (``start_disabled`` +
``send_message_batch``) so the first poll co-batches it into one invocation —
the deterministic way to observe tail blocking (a live ESM otherwise does not
guarantee co-batching). Harness in conftest.py.
"""

import json

import pytest

pytestmark = pytest.mark.aws


def test_fifo_poison_and_blocked_tail_dead_letter_other_groups_safe(aws, pipeline, drain):
    sqs = aws["sqs"]
    main_url, dlq_url, enable = pipeline(fifo=True, max_receive_count=2, start_disabled=True)

    # group A: a1 ok, boom-a2 poison, a3 (blocked tail); group B: b1 ok.
    # One atomic batch while the ESM is OFF -> the first poll co-batches the group.
    sqs.send_message_batch(
        QueueUrl=main_url,
        Entries=[
            {"Id": "a1", "MessageBody": json.dumps({"type": "task", "task_id": "a1"}), "MessageGroupId": "A"},
            {"Id": "a2", "MessageBody": json.dumps({"type": "task", "task_id": "boom-a2"}), "MessageGroupId": "A"},
            {"Id": "a3", "MessageBody": json.dumps({"type": "task", "task_id": "a3"}), "MessageGroupId": "A"},
            {"Id": "b1", "MessageBody": json.dumps({"type": "task", "task_id": "b1"}), "MessageGroupId": "B"},
        ],
    )
    enable()

    # The poison and its blocked tail both dead-letter (the tail shares the
    # poison's redelivery count and never gets a poison-free batch).
    moved = drain(dlq_url, timeout=180, min_count=2)
    ids = {json.loads(b)["task_id"] for b in moved}

    assert "boom-a2" in ids                # the poison
    assert "a3" in ids                     # blocked tail dead-letters with the poison
    assert not ({"a1", "b1"} & ids)        # a1 (before poison) and group B succeed

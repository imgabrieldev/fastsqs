"""Real-AWS e2e: FIFO poison-message behavior via a real ESM
(opt-in: ``pytest --run-aws``).

Under ``fifo_failure_mode="isolate_groups"`` (default) with a redrive policy, a
poison message blocks the rest of ITS messageGroupId on every redelivery, so the
poison AND its blocked tail share the same receiveCount and BOTH dead-letter at
maxReceiveCount — the tail never gets a poison-free batch. Messages BEFORE the
poison in the group succeed, and OTHER message groups are unaffected throughout.
This is the inherent FIFO+DLQ ordering tradeoff (you cannot skip the poison
without breaking order). The deployed handler runs with the default mode.
Harness in conftest.py.
"""

import json

import pytest

pytestmark = pytest.mark.aws


def test_fifo_poison_and_blocked_tail_dead_letter_other_groups_safe(aws, pipeline, drain):
    sqs = aws["sqs"]
    main_url, dlq_url = pipeline(fifo=True, max_receive_count=2)

    def send(task_id, group):
        sqs.send_message(
            QueueUrl=main_url,
            MessageBody=json.dumps({"type": "task", "task_id": task_id}),
            MessageGroupId=group,
        )

    # group A: a1 ok, boom-a2 poison, a3 (blocked tail); group B: b1 ok
    send("a1", "A")
    send("boom-a2", "A")
    send("a3", "A")
    send("b1", "B")

    # The poison and its blocked tail both dead-letter (the tail shares the
    # poison's redelivery count and never gets a poison-free batch).
    moved = drain(dlq_url, timeout=180, min_count=2)
    ids = {json.loads(b)["task_id"] for b in moved}

    assert "boom-a2" in ids                # the poison
    assert "a3" in ids                     # blocked tail dead-letters with the poison
    assert not ({"a1", "b1"} & ids)        # a1 (before poison) and group B succeed

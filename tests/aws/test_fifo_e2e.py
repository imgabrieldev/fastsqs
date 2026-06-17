"""Real-AWS e2e: FIFO poison-message isolation via a real ESM
(opt-in: ``pytest --run-aws``).

A poison message in a FIFO group is isolated to the DLQ. Within a single batch
fastsqs blocks the tail (preserving order); but once SQS dead-letters the poison
(maxReceiveCount), the group unblocks and the rest of the group succeeds — so
ONLY the poison ends up in the DLQ. Other message groups are unaffected
throughout. The deployed handler runs with skip_group_on_error=True (default).
Harness in conftest.py.
"""

import json

import pytest

pytestmark = pytest.mark.aws


def test_fifo_poison_isolated_to_dlq_and_group_unblocks(aws, pipeline, drain):
    sqs = aws["sqs"]
    main_url, dlq_url = pipeline(fifo=True, max_receive_count=2)

    def send(task_id, group):
        sqs.send_message(
            QueueUrl=main_url,
            MessageBody=json.dumps({"type": "task", "task_id": task_id}),
            MessageGroupId=group,
        )

    # group A: a1 ok, boom-a2 poison (blocks a3 within a batch), a3 ok; group B: b1 ok
    send("a1", "A")
    send("boom-a2", "A")
    send("a3", "A")
    send("b1", "B")

    moved = drain(dlq_url, timeout=180, min_count=1)
    ids = {json.loads(b)["task_id"] for b in moved}

    assert "boom-a2" in ids                       # poison isolated to the DLQ
    assert not ({"a1", "a3", "b1"} & ids)         # rest succeed (group unblocks after dead-letter)

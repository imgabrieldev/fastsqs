"""Batch failure & FIFO ordering battle tests."""

import json

import pytest

from fastsqs import FastSQS, SQSEvent, QueueType, BatchFailedError
from fastsqs.testing import SQSTestClient


class Task(SQSEvent):
    task_id: str


def _rec(mid, task_id, group=None):
    rec = {"messageId": mid, "body": json.dumps({"type": "task", "task_id": task_id})}
    if group is not None:
        rec["attributes"] = {"messageGroupId": group}
    return rec


def _failing_on(*bad_ids):
    app = FastSQS()
    processed = []

    @app.route(Task)
    async def handle(msg: Task):
        processed.append(msg.task_id)
        if msg.task_id in bad_ids:
            raise ValueError(f"boom {msg.task_id}")

    return app, processed


# ---- standard queue partial-failure attribution ----

def test_all_succeed_no_failures():
    app, _ = _failing_on()
    r = SQSTestClient(app).send_batch([{"type": "task", "task_id": str(i)} for i in range(4)])
    assert r == {"batchItemFailures": []}


def test_all_fail_reports_every_id():
    app, _ = _failing_on("0", "1", "2")
    r = SQSTestClient(app).send_batch([{"type": "task", "task_id": str(i)} for i in range(3)])
    ids = {f["itemIdentifier"] for f in r["batchItemFailures"]}
    assert ids == {"m0", "m1", "m2"}


def test_mixed_reports_only_failed():
    app, _ = _failing_on("1")
    r = SQSTestClient(app).send_batch(
        [{"type": "task", "task_id": "0"}, {"type": "task", "task_id": "1"}, {"type": "task", "task_id": "2"}]
    )
    assert r["batchItemFailures"] == [{"itemIdentifier": "m1"}]


# ---- enable_partial_batch_failure=False (regression for bug C) ----

def test_partial_disabled_standard_raises_whole_batch():
    app = FastSQS(enable_partial_batch_failure=False)

    @app.route(Task)
    async def handle(msg: Task):
        if msg.task_id == "boom":
            raise ValueError("boom")

    with pytest.raises(BatchFailedError):
        SQSTestClient(app).send_batch([{"type": "task", "task_id": "ok"}, {"type": "task", "task_id": "boom"}])


def test_partial_disabled_all_success_returns_empty():
    app = FastSQS(enable_partial_batch_failure=False)

    @app.route(Task)
    async def handle(msg: Task):
        pass

    r = SQSTestClient(app).send_batch([{"type": "task", "task_id": "ok"}])
    assert r == {"batchItemFailures": []}


def test_partial_disabled_fifo_raises_whole_batch():
    app = FastSQS(enable_partial_batch_failure=False)
    app.set_queue_type(QueueType.FIFO)

    @app.route(Task)
    async def handle(msg: Task):
        if msg.task_id == "boom":
            raise ValueError("boom")

    with pytest.raises(BatchFailedError):
        app.handler({"Records": [_rec("m0", "ok", "g"), _rec("m1", "boom", "g")]}, None)


# ---- FIFO ordering: failure position within a group ----

@pytest.mark.parametrize("fail_pos,expected_failed,expected_processed", [
    (0, {"m0", "m1", "m2"}, ["a"]),            # first fails -> whole group blocked
    (1, {"m1", "m2"}, ["a", "b"]),             # middle fails -> it + tail
    (2, {"m2"}, ["a", "b", "c"]),              # last fails -> only it
])
def test_fifo_failure_position(fail_pos, expected_failed, expected_processed):
    app = FastSQS()
    app.set_queue_type(QueueType.FIFO)
    processed = []
    ids = ["a", "b", "c"]
    bad = ids[fail_pos]

    @app.route(Task)
    async def handle(msg: Task):
        processed.append(msg.task_id)
        if msg.task_id == bad:
            raise ValueError("boom")

    records = [_rec(f"m{i}", ids[i], "g") for i in range(3)]
    r = app.handler({"Records": records}, None)
    assert {f["itemIdentifier"] for f in r["batchItemFailures"]} == expected_failed
    assert processed == expected_processed


def test_fifo_skip_group_on_error_false_halts_whole_batch():
    """skip_group_on_error=False (Powertools default): first failure halts the
    whole batch in arrival order; the failed record and every record after it
    (any group) are reported and not processed."""
    app = FastSQS(skip_group_on_error=False)
    app.set_queue_type(QueueType.FIFO)
    processed = []

    @app.route(Task)
    async def handle(msg: Task):
        processed.append(msg.task_id)
        if msg.task_id == "A2":
            raise ValueError("boom")

    # arrival order interleaves groups: A1, A2(fail), B1, A3
    records = [_rec("A1", "A1", "A"), _rec("A2", "A2", "A"), _rec("B1", "B1", "B"), _rec("A3", "A3", "A")]
    r = app.handler({"Records": records}, None)
    failed = {f["itemIdentifier"] for f in r["batchItemFailures"]}
    assert failed == {"A2", "B1", "A3"}   # failed + everything after it
    assert processed == ["A1", "A2"]      # halted: B1 (other group) and A3 never ran


def test_fifo_groups_are_isolated():
    app = FastSQS()
    app.set_queue_type(QueueType.FIFO)
    processed = []

    @app.route(Task)
    async def handle(msg: Task):
        processed.append(msg.task_id)
        if msg.task_id == "A2":
            raise ValueError("boom")

    records = [
        _rec("A1", "A1", "A"), _rec("A2", "A2", "A"), _rec("A3", "A3", "A"),
        _rec("B1", "B1", "B"), _rec("B2", "B2", "B"),
    ]
    r = app.handler({"Records": records}, None)
    failed = {f["itemIdentifier"] for f in r["batchItemFailures"]}
    assert failed == {"A2", "A3"}          # group A blocked from A2 on
    assert "B1" in processed and "B2" in processed  # group B fully processed


# ---- size edge cases ----

def test_empty_batch():
    app, _ = _failing_on()
    assert app.handler({"Records": []}, None) == {"batchItemFailures": []}


def test_single_record():
    app, _ = _failing_on()
    assert SQSTestClient(app).send({"type": "task", "task_id": "1"}) == {"batchItemFailures": []}


def test_large_batch_all_processed():
    app = FastSQS(max_concurrent_messages=20)
    seen = []

    @app.route(Task)
    async def handle(msg: Task):
        seen.append(msg.task_id)

    SQSTestClient(app).send_batch([{"type": "task", "task_id": str(i)} for i in range(500)])
    assert len(seen) == 500


# ---- malformed event structures ----

@pytest.mark.parametrize("event", [
    {},                          # no Records key
    {"Records": None},           # null
    {"Records": "not-a-list"},   # wrong type
])
def test_malformed_event_no_crash(event):
    app, _ = _failing_on()
    assert app.handler(event, None) == {"batchItemFailures": []}


# ---- D1/D2: messageId quirks (real SQS guarantees unique ids; document behavior) ----

def test_missing_messageid_falls_back_to_unknown():
    app = FastSQS()

    @app.route(Task)
    async def handle(msg: Task):
        raise ValueError("boom")

    r = app.handler({"Records": [{"body": json.dumps({"type": "task", "task_id": "1"})}]}, None)
    assert r["batchItemFailures"] == [{"itemIdentifier": "UNKNOWN"}]

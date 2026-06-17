"""FIFO concurrency & message-group battle tests.

FIFO's partial-batch path (``RecordProcessingMixin._handle_fifo_event`` /
``_handle_fifo_halt_batch``) was untested for concurrency: standard queues
parallelise records under a semaphore, while FIFO instead ``gather``s over
message GROUPS (groups run concurrently; records WITHIN a group run serially in
arrival order). These tests pin that behaviour plus the grouping rules
(``group_records_by_message_group``), the two ``fifo_failure_mode`` edges, and
the exact ordered ``batchItemFailures`` shape.

Observation strategy: handlers await shared ``asyncio.Event``s (set by the OTHER
group) so an interleaving that requires both groups to be in-flight at once can
only complete if they genuinely run concurrently. ``asyncio.wait_for`` bounds
the test so a serialised implementation would deadlock and fail fast instead of
hanging the suite.

Notes / adjustments to match REAL v1 behaviour:
- ``process_group`` is a private CLOSURE inside ``_handle_fifo_event``; it is not
  reachable from the outside. The public path that ``gather``s over groups is
  ``async_handler`` -> ``_handle_event`` -> ``_handle_fifo_event``, so the
  concurrency cases drive that entry point (awaiting it inside one event loop)
  rather than calling ``process_group`` directly.
- For isolate_groups across multiple groups, ``batchItemFailures`` ordering is
  not defined across groups (it follows dict/group iteration order), so the
  multi-group case asserts MEMBERSHIP. The single-group and halt_batch cases DO
  have a defined arrival order, so those assert the exact ordered list.
"""

from __future__ import annotations

import asyncio
import json

import pytest

from fastsqs import FastSQS, QueueType, SQSEvent
from fastsqs.testing import RecordSpec, SQSTestClient
from fastsqs.utils import group_records_by_message_group


class Task(SQSEvent):
    task_id: str


_FIFO_ARN = "arn:aws:sqs:us-east-1:000000000000:test-queue.fifo"


def _rec(mid, task_id, group=None, *, arn=None):
    """Build one raw SQS record. ``group`` sets messageGroupId; a ``.fifo`` ARN is
    attached when a group is given so a QueueType.AUTO app would infer FIFO."""
    rec = {
        "messageId": mid,
        "body": json.dumps({"type": "task", "task_id": task_id}),
    }
    if group is not None:
        rec["attributes"] = {"messageGroupId": group}
    if arn is not None:
        rec["eventSourceARN"] = arn
    elif group is not None:
        rec["eventSourceARN"] = _FIFO_ARN
    return rec


def _run(coro):
    """Run a coroutine on a fresh event loop (matches the asyncio.run style used
    across the suite; no pytest-asyncio plugin is configured)."""
    return asyncio.run(coro)


# ---------------------------------------------------------------------------
# Concurrency across groups / seriality within a group
# ---------------------------------------------------------------------------

def test_fifo_groups_run_concurrently():
    """Two groups g1, g2 each await an Event the OTHER group sets. The batch can
    only finish if both groups are in-flight simultaneously (FIFO gathers over
    groups). Bounded by wait_for so a serial impl would fail, not hang."""
    app = FastSQS(queue_type=QueueType.FIFO)
    ev = {"g1": asyncio.Event(), "g2": asyncio.Event()}

    @app.route(Task)
    async def handle(msg: Task):
        mine = msg.task_id          # "g1" / "g2"
        other = "g2" if mine == "g1" else "g1"
        ev[mine].set()              # signal I'm in-flight
        # Block until the other group is also in-flight: only possible if the
        # two groups run concurrently.
        await asyncio.wait_for(ev[other].wait(), timeout=2.0)

    records = [_rec("m1", "g1", "g1"), _rec("m2", "g2", "g2")]

    async def run():
        return await asyncio.wait_for(
            app.async_handler({"Records": records}, None), timeout=3.0
        )

    result = _run(run())
    assert result == {"batchItemFailures": []}


def test_fifo_records_within_group_run_serially_in_order():
    """A single group g1 with 3 records: the handler awaits between appends. If
    the group ran them concurrently the order could scramble; FIFO runs them
    serially in arrival order, so the list is exactly ['1','2','3']."""
    app = FastSQS(queue_type=QueueType.FIFO)
    order = []

    @app.route(Task)
    async def handle(msg: Task):
        order.append(msg.task_id)
        await asyncio.sleep(0)  # yield: a concurrent impl would interleave here

    records = [_rec(f"m{i}", str(i), "g1") for i in (1, 2, 3)]
    result = app.handler({"Records": records}, None)

    assert result == {"batchItemFailures": []}
    assert order == ["1", "2", "3"]


def test_fifo_does_not_apply_max_concurrent_semaphore():
    """max_concurrent_messages bounds STANDARD queues only. With FIFO + a limit
    of 1, two groups whose handlers block on each other's events still both make
    progress: the semaphore is never applied on the FIFO path (it gathers over
    groups instead)."""
    app = FastSQS(max_concurrent_messages=1, queue_type=QueueType.AUTO)
    ev = {"g1": asyncio.Event(), "g2": asyncio.Event()}

    @app.route(Task)
    async def handle(msg: Task):
        mine = msg.task_id
        other = "g2" if mine == "g1" else "g1"
        ev[mine].set()
        await asyncio.wait_for(ev[other].wait(), timeout=2.0)

    # group_id sets a .fifo ARN, so AUTO infers FIFO.
    records = [_rec("m1", "g1", "g1"), _rec("m2", "g2", "g2")]

    async def run():
        return await asyncio.wait_for(
            app.async_handler({"Records": records}, None), timeout=3.0
        )

    # If the semaphore (size 1) were applied, only one handler could enter and
    # the cross-wait would deadlock past the timeout -> TimeoutError. Reaching a
    # clean result proves concurrency despite max_concurrent_messages=1.
    result = _run(run())
    assert result == {"batchItemFailures": []}


# ---------------------------------------------------------------------------
# Records without messageGroupId collapse to the 'default' group
# ---------------------------------------------------------------------------

def test_fifo_records_without_group_id_use_default_group():
    """On a FIFO queue, records lacking a messageGroupId all collapse into the
    single 'default' group and run serially in arrival order; one failure blocks
    the tail of that single group."""
    app = FastSQS(queue_type=QueueType.FIFO)
    order = []

    @app.route(Task)
    async def handle(msg: Task):
        order.append(msg.task_id)
        await asyncio.sleep(0)
        if msg.task_id == "1":  # the SECOND record fails
            raise ValueError("boom")

    # No group attribute on any record (explicit FIFO queue_type forces FIFO).
    records = [_rec("m0", "0"), _rec("m1", "1"), _rec("m2", "2")]
    result = app.handler({"Records": records}, None)

    # Single default group, serial arrival order; m1 fails and blocks its tail.
    assert order == ["0", "1"]              # m2 never ran (blocked behind m1)
    assert result["batchItemFailures"] == [
        {"itemIdentifier": "m1"},
        {"itemIdentifier": "m2"},
    ]


# ---------------------------------------------------------------------------
# isolate_groups (default)
# ---------------------------------------------------------------------------

def test_isolate_groups_trailing_success_reported_failed():
    """isolate_groups, single group g1 with [m0 fail, m1 would-succeed]: FIFO
    short-circuits at m0 to preserve ordering, so m1's handler NEVER runs and
    BOTH m0 and m1 are reported, in arrival order."""
    app = FastSQS(queue_type=QueueType.FIFO)  # isolate_groups is the default
    probe = {"m1_ran": False}

    @app.route(Task)
    async def handle(msg: Task):
        if msg.task_id == "m0":
            raise ValueError("boom")
        if msg.task_id == "m1":
            probe["m1_ran"] = True

    records = [_rec("m0", "m0", "g1"), _rec("m1", "m1", "g1")]
    result = app.handler({"Records": records}, None)

    assert result["batchItemFailures"] == [
        {"itemIdentifier": "m0"},
        {"itemIdentifier": "m1"},
    ]
    assert probe["m1_ran"] is False  # trailing record short-circuited


def test_isolate_groups_failed_group_blocks_only_itself():
    """Three groups g1=[fail], g2=[ok], g3=[ok]. Only g1's record is reported;
    g2 and g3 both run to completion (other groups proceed independently)."""
    app = FastSQS(queue_type=QueueType.FIFO)
    ran = set()

    @app.route(Task)
    async def handle(msg: Task):
        ran.add(msg.task_id)
        if msg.task_id == "g1":
            raise ValueError("boom")

    records = [
        _rec("g1", "g1", "g1"),
        _rec("g2", "g2", "g2"),
        _rec("g3", "g3", "g3"),
    ]
    result = app.handler({"Records": records}, None)

    failed = {f["itemIdentifier"] for f in result["batchItemFailures"]}
    assert failed == {"g1"}            # only the failed group's record
    assert "g2" in ran and "g3" in ran  # independent groups still processed


def test_isolate_groups_all_success_returns_empty():
    """Multiple FIFO groups, every record succeeds -> empty failures."""
    app = FastSQS(queue_type=QueueType.FIFO)
    ran = []

    @app.route(Task)
    async def handle(msg: Task):
        ran.append(msg.task_id)

    records = [
        _rec("a1", "a1", "ga"),
        _rec("a2", "a2", "ga"),
        _rec("b1", "b1", "gb"),
        _rec("c1", "c1", "gc"),
    ]
    result = app.handler({"Records": records}, None)

    assert result == {"batchItemFailures": []}
    assert set(ran) == {"a1", "a2", "b1", "c1"}


# ---------------------------------------------------------------------------
# halt_batch
# ---------------------------------------------------------------------------

def test_halt_batch_first_record_failure_reports_all():
    """halt_batch: batch [fail, ok, ok] in arrival order. The first record fails,
    halting the whole batch: all three ids are reported in arrival order and the
    two trailing handlers never run."""
    app = FastSQS(fifo_failure_mode="halt_batch", queue_type=QueueType.FIFO)
    ran = []

    @app.route(Task)
    async def handle(msg: Task):
        ran.append(msg.task_id)
        if msg.task_id == "0":
            raise ValueError("boom")

    # Interleave two groups to prove halt is by ARRIVAL order, not per-group.
    records = [
        _rec("m0", "0", "ga"),
        _rec("m1", "1", "gb"),
        _rec("m2", "2", "ga"),
    ]
    result = app.handler({"Records": records}, None)

    assert result["batchItemFailures"] == [
        {"itemIdentifier": "m0"},
        {"itemIdentifier": "m1"},
        {"itemIdentifier": "m2"},
    ]
    assert ran == ["0"]  # halted immediately; trailing handlers never ran


def test_halt_batch_last_record_failure_reports_only_last():
    """halt_batch: batch [ok, ok, fail]. Only the last id is reported; the first
    two handlers ran (the halt happens at the last record)."""
    app = FastSQS(fifo_failure_mode="halt_batch", queue_type=QueueType.FIFO)
    ran = []

    @app.route(Task)
    async def handle(msg: Task):
        ran.append(msg.task_id)
        if msg.task_id == "2":
            raise ValueError("boom")

    records = [
        _rec("m0", "0", "ga"),
        _rec("m1", "1", "ga"),
        _rec("m2", "2", "ga"),
    ]
    result = app.handler({"Records": records}, None)

    assert result["batchItemFailures"] == [{"itemIdentifier": "m2"}]
    assert ran == ["0", "1", "2"]


def test_halt_batch_all_success_returns_empty():
    """halt_batch: every record succeeds -> empty failures and every handler ran."""
    app = FastSQS(fifo_failure_mode="halt_batch", queue_type=QueueType.FIFO)
    ran = []

    @app.route(Task)
    async def handle(msg: Task):
        ran.append(msg.task_id)

    records = [
        _rec("m0", "0", "ga"),
        _rec("m1", "1", "gb"),
        _rec("m2", "2", "ga"),
    ]
    result = app.handler({"Records": records}, None)

    assert result == {"batchItemFailures": []}
    assert ran == ["0", "1", "2"]


# ---------------------------------------------------------------------------
# Unit: group_records_by_message_group
# ---------------------------------------------------------------------------

def test_group_records_by_message_group_unit():
    """Direct unit call: keys are the distinct group ids plus 'default' for the
    record with no messageGroupId, and each value preserves arrival order."""
    rec_g1a = _rec("a1", "a1", "g1")
    rec_g2 = _rec("b1", "b1", "g2")
    rec_g1b = _rec("a2", "a2", "g1")
    rec_no_group = _rec("n1", "n1")  # no messageGroupId attribute

    groups = group_records_by_message_group(
        [rec_g1a, rec_g2, rec_g1b, rec_no_group]
    )

    assert set(groups.keys()) == {"g1", "g2", "default"}
    # arrival order preserved within each group
    assert groups["g1"] == [rec_g1a, rec_g1b]
    assert groups["g2"] == [rec_g2]
    assert groups["default"] == [rec_no_group]


# ---------------------------------------------------------------------------
# Adjacent: the test client / RecordSpec also drives the FIFO path via AUTO
# ---------------------------------------------------------------------------

def test_isolate_groups_via_test_client_send_batch():
    """SQSTestClient.send_batch with per-record group_ids drives the FIFO path
    under AUTO (group_id sets a .fifo ARN). g1's failure is isolated to g1."""
    app = FastSQS()  # AUTO + isolate_groups defaults
    ran = set()

    @app.route(Task)
    async def handle(msg: Task):
        ran.add(msg.task_id)
        if msg.task_id == "x":
            raise ValueError("boom")

    result = SQSTestClient(app).send_batch([
        RecordSpec({"type": "task", "task_id": "x"}, message_id="g1", group_id="g1"),
        RecordSpec({"type": "task", "task_id": "y"}, message_id="g2", group_id="g2"),
    ])

    failed = {f["itemIdentifier"] for f in result["batchItemFailures"]}
    assert failed == {"g1"}
    assert "y" in ran


@pytest.mark.parametrize("fail_pos,expected,processed", [
    (0, [{"itemIdentifier": "m0"}, {"itemIdentifier": "m1"}, {"itemIdentifier": "m2"}], ["0"]),
    (1, [{"itemIdentifier": "m1"}, {"itemIdentifier": "m2"}], ["0", "1"]),
    (2, [{"itemIdentifier": "m2"}], ["0", "1", "2"]),
])
def test_halt_batch_ordered_failures_by_position(fail_pos, expected, processed):
    """halt_batch reports the failing record plus the unprocessed tail, in exact
    arrival order, for any failure position in a single-group batch."""
    app = FastSQS(fifo_failure_mode="halt_batch", queue_type=QueueType.FIFO)
    ran = []
    bad = str(fail_pos)

    @app.route(Task)
    async def handle(msg: Task):
        ran.append(msg.task_id)
        if msg.task_id == bad:
            raise ValueError("boom")

    records = [_rec(f"m{i}", str(i), "g") for i in range(3)]
    result = app.handler({"Records": records}, None)

    assert result["batchItemFailures"] == expected
    assert ran == processed

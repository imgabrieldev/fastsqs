"""QueueType.AUTO inference (_resolve_queue_type) + FifoInfo population.

AUTO is the default queue_type and the production path that picks FIFO vs
standard, yet ``_resolve_queue_type`` is never exercised directly and the FIFO
suite always forces ``queue_type=FIFO``. These tests cover the ARN-suffix
branch, the missing/empty-ARN fallbacks, the empty-records branch, and explicit
overrides of AUTO. They also cover FifoInfo population, which is gated on the
resolved FIFO type and reads from ``record['attributes']``.
"""

import pytest

from fastsqs import Context, FastSQS, FifoInfo, QueueType, SQSEvent
from fastsqs.testing import SQSTestClient


class Task(SQSEvent):
    task_id: str


# ---- _resolve_queue_type: AUTO inference (pure unit calls) ----

def test_resolve_queue_type_auto_infers_fifo_from_fifo_arn():
    app = FastSQS()  # queue_type defaults to AUTO
    resolved = app._resolve_queue_type(
        [{"eventSourceARN": "arn:aws:sqs:us-east-1:0:q.fifo"}]
    )
    assert resolved is QueueType.FIFO


def test_resolve_queue_type_auto_infers_standard_from_plain_arn():
    app = FastSQS()
    resolved = app._resolve_queue_type(
        [{"eventSourceARN": "arn:aws:sqs:us-east-1:0:q"}]
    )
    assert resolved is QueueType.STANDARD


def test_resolve_queue_type_auto_with_missing_arn_defaults_standard():
    app = FastSQS()
    # No eventSourceARN key -> `arn = ... or ''` fallback.
    resolved = app._resolve_queue_type([{"messageId": "m0"}])
    assert resolved is QueueType.STANDARD


def test_resolve_queue_type_auto_with_empty_string_arn_defaults_standard():
    app = FastSQS()
    # Empty string does not endswith '.fifo'.
    resolved = app._resolve_queue_type([{"eventSourceARN": ""}])
    assert resolved is QueueType.STANDARD


def test_resolve_queue_type_empty_records_defaults_standard():
    app = FastSQS()
    # `if records:` guard skipped -> falls through to STANDARD.
    resolved = app._resolve_queue_type([])
    assert resolved is QueueType.STANDARD


# ---- _resolve_queue_type: explicit type short-circuits ARN inspection ----

def test_resolve_queue_type_explicit_fifo_overrides_plain_arn():
    app = FastSQS(queue_type=QueueType.FIFO)
    resolved = app._resolve_queue_type(
        [{"eventSourceARN": "arn:aws:sqs:us-east-1:0:q"}]
    )
    assert resolved is QueueType.FIFO


def test_resolve_queue_type_explicit_standard_overrides_fifo_arn():
    app = FastSQS(queue_type=QueueType.STANDARD)
    resolved = app._resolve_queue_type(
        [{"eventSourceARN": "arn:aws:sqs:us-east-1:0:q.fifo"}]
    )
    assert resolved is QueueType.STANDARD


@pytest.mark.parametrize(
    "queue_type,arn,expected",
    [
        (QueueType.AUTO, "arn:aws:sqs:us-east-1:0:q.fifo", QueueType.FIFO),
        (QueueType.AUTO, "arn:aws:sqs:us-east-1:0:q", QueueType.STANDARD),
        (QueueType.FIFO, "arn:aws:sqs:us-east-1:0:q", QueueType.FIFO),
        (QueueType.STANDARD, "arn:aws:sqs:us-east-1:0:q.fifo", QueueType.STANDARD),
    ],
)
def test_resolve_queue_type_matrix(queue_type, arn, expected):
    app = FastSQS(queue_type=queue_type)
    assert app._resolve_queue_type([{"eventSourceARN": arn}]) is expected


# ---- end-to-end: AUTO selects the FIFO path through the test client ----

def test_auto_queue_routes_fifo_path_when_group_id_set_via_testclient():
    app = FastSQS()  # AUTO
    captured = []

    @app.route(Task)
    async def handle(msg: Task, ctx: Context):
        captured.append(ctx.queue_type)

    # group_id sets a ".fifo" eventSourceARN so AUTO infers FIFO.
    r = SQSTestClient(app).send({"type": "task", "task_id": "1"}, group_id="g1")
    assert r == {"batchItemFailures": []}
    assert captured == [QueueType.FIFO]


# ---- FifoInfo population (gated on resolved FIFO type) ----

def test_fifo_info_populated_with_group_and_dedup_id():
    app = FastSQS()  # AUTO
    captured = []

    @app.route(Task)
    async def handle(msg: Task, ctx: Context):
        captured.append(ctx.fifo_info)

    SQSTestClient(app).send(
        {"type": "task", "task_id": "1"}, group_id="g1", deduplication_id="d1"
    )
    info = captured[0]
    assert isinstance(info, FifoInfo)
    assert info.message_group_id == "g1"
    assert info.message_deduplication_id == "d1"


def test_fifo_info_is_none_on_standard_queue():
    app = FastSQS()  # AUTO
    captured = []

    @app.route(Task)
    async def handle(msg: Task, ctx: Context):
        captured.append(ctx.fifo_info)

    # Plain body, no group_id -> standard ARN -> STANDARD queue -> no FifoInfo.
    SQSTestClient(app).send({"type": "task", "task_id": "1"})
    assert captured == [None]


def test_fifo_info_fields_none_when_group_attribute_absent():
    # Explicit FIFO short-circuits ARN inspection, so even with the default
    # standard ARN the FIFO branch runs; with empty attributes both fields
    # resolve to None.
    app = FastSQS(queue_type=QueueType.FIFO)
    captured = []

    @app.route(Task)
    async def handle(msg: Task, ctx: Context):
        captured.append(ctx.fifo_info)

    SQSTestClient(app).send({"type": "task", "task_id": "1"}, attributes={})
    info = captured[0]
    assert isinstance(info, FifoInfo)
    assert info.message_group_id is None
    assert info.message_deduplication_id is None

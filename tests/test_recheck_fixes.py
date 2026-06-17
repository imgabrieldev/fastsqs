"""Regression tests for the heavy re-audit fixes (B1, B2, B3, B6, B7).

Each test pins down a path that was previously broken and untested.
"""

import json

from fastsqs import (
    FastSQS,
    SQSRouter,
    SQSEvent,
    QueueType,
    InvalidMessageError,
)
from fastsqs.middleware import Middleware
from fastsqs.testing import SQSTestClient


class Task(SQSEvent):
    task_id: str


class Recorder(Middleware):
    def __init__(self, log):
        super().__init__()
        self._sink = log

    async def before(self, payload, record, context, ctx):
        self._sink.append("before")

    async def after(self, payload, record, context, ctx, error):
        self._sink.append("after")


# B1 — per-route middlewares on a PYDANTIC route used to raise TypeError
# (wrapper called before/after with the wrong arity). Now they run correctly.
def test_per_route_middleware_runs_on_pydantic_route():
    calls = []

    app = FastSQS()

    @app.route(Task, middlewares=[Recorder(calls)])
    async def handle(msg: Task):
        calls.append("handler")

    result = SQSTestClient(app).send({"type": "task", "task_id": "1"})

    assert result == {"batchItemFailures": []}
    assert calls == ["before", "handler", "after"]


# B2 — router-level middlewares were silently skipped for pydantic routes
# (pydantic tier called invoke_handler directly). Now they run.
def test_router_level_middleware_runs_on_pydantic_route():
    calls = []

    router = SQSRouter()
    router.add_middleware(Recorder(calls))

    @router.route(Task)
    async def handle(msg: Task):
        calls.append("handler")

    app = FastSQS()
    app.include_router(router)

    result = SQSTestClient(app).send({"type": "task", "task_id": "1"})

    assert result == {"batchItemFailures": []}
    assert calls == ["before", "handler", "after"]


# B3 — a validation failure on a key-value route with model= raised
# `ValidationError(str)` which itself blew up as a TypeError in pydantic v2.
# It must surface as InvalidMessageError and become a clean batch failure.
def test_validation_failure_surfaces_as_invalid_message():
    seen_errors = []

    class Capture(Middleware):
        async def after(self, payload, record, context, ctx, error):
            seen_errors.append(error)

    app = FastSQS()
    app.add_middleware(Capture())

    router = SQSRouter(discriminator="action")

    @router.route("do", model=Task)
    async def handle(msg: Task):
        return "ok"

    app.include_router(router)

    # missing required task_id -> validation fails inside _execute_handler
    result = SQSTestClient(app).send({"action": "do"}, message_id="bad-1")

    assert result == {"batchItemFailures": [{"itemIdentifier": "bad-1"}]}
    assert len(seen_errors) == 1
    assert isinstance(seen_errors[0], InvalidMessageError)


# B6 — retry machinery was dead code; it has been removed.
def test_retryconfig_is_gone():
    import fastsqs
    import fastsqs.middleware as mw

    assert not hasattr(fastsqs, "RetryConfig")
    assert not hasattr(mw, "RetryConfig")


# B7 — a failure in a FIFO group must halt the group (ordering) AND mark the
# failed record plus every later record in the group as a batch failure so SQS
# redelivers the tail in order.
def test_fifo_failure_halts_group_and_marks_tail():
    app = FastSQS(queue_type=QueueType.FIFO)

    processed = []

    @app.route(Task)
    async def handle(msg: Task):
        processed.append(msg.task_id)
        if msg.task_id == "2":
            raise ValueError("boom on 2")

    def rec(mid, task_id):
        return {
            "messageId": mid,
            "body": json.dumps({"type": "task", "task_id": task_id}),
            "attributes": {"messageGroupId": "g"},
        }

    result = app.handler(
        {"Records": [rec("m0", "1"), rec("m1", "2"), rec("m2", "3")]}, None
    )

    assert processed == ["1", "2"]  # record 3 never runs (group halted)
    failed = {f["itemIdentifier"] for f in result["batchItemFailures"]}
    assert failed == {"m1", "m2"}  # failed record + blocked tail


# B7 — independent FIFO groups are not affected by each other's failures.
def test_fifo_failure_isolated_per_group():
    app = FastSQS(queue_type=QueueType.FIFO)

    processed = []

    @app.route(Task)
    async def handle(msg: Task):
        processed.append(msg.task_id)
        if msg.task_id == "a2":
            raise ValueError("boom")

    def rec(mid, task_id, group):
        return {
            "messageId": mid,
            "body": json.dumps({"type": "task", "task_id": task_id}),
            "attributes": {"messageGroupId": group},
        }

    result = app.handler(
        {
            "Records": [
                rec("a1", "a1", "A"),
                rec("a2", "a2", "A"),
                rec("b1", "b1", "B"),
            ]
        },
        None,
    )

    failed = {f["itemIdentifier"] for f in result["batchItemFailures"]}
    assert failed == {"a2"}  # group B unaffected
    assert "b1" in processed

"""Routing & handler-dispatch battle tests."""

import json

import pytest

from fastsqs import FastSQS, SQSRouter, SQSEvent
from fastsqs.testing import SQSTestClient


class Task(SQSEvent):
    task_id: str = "x"


# ---- handler signature variants (select_kwargs injection) ----

def test_handler_msg_only():
    app = FastSQS()
    got = {}

    @app.route(Task)
    async def h(msg: Task):
        got["id"] = msg.task_id

    SQSTestClient(app).send({"type": "task", "task_id": "1"})
    assert got["id"] == "1"


def test_handler_msg_and_ctx():
    app = FastSQS()
    got = {}

    @app.route(Task)
    async def h(msg: Task, ctx):
        got["ctx_type"] = ctx.message_type

    SQSTestClient(app).send({"type": "task", "task_id": "1"})
    assert got["ctx_type"] == "task"


def test_handler_full_injection():
    app = FastSQS()
    got = {}

    @app.route(Task)
    async def h(msg: Task, record, context, ctx):
        got["mid"] = record["messageId"]
        got["has_ctx"] = ctx is not None

    SQSTestClient(app).send({"type": "task", "task_id": "1"}, message_id="abc")
    assert got["mid"] == "abc" and got["has_ctx"]


def test_sync_handler_supported():
    app = FastSQS()
    got = {}

    @app.route(Task)
    def h(msg: Task):  # not async
        got["id"] = msg.task_id

    SQSTestClient(app).send({"type": "task", "task_id": "1"})
    assert got["id"] == "1"


# ---- default-handler fallback (regression for bugs B1/B2) ----

def test_default_catches_missing_type():
    app = FastSQS()
    seen = []

    @app.route(Task)
    async def h(msg: Task):
        seen.append("task")

    @app.default()
    async def d(msg, ctx):
        seen.append("default")

    r = SQSTestClient(app).send({"task_id": "no-type"})
    assert r == {"batchItemFailures": []}
    assert seen == ["default"]


def test_default_catches_null_type():
    app = FastSQS()
    seen = []

    @app.route(Task)
    async def h(msg: Task):
        seen.append("task")

    @app.default()
    async def d(msg, ctx):
        seen.append("default")

    SQSTestClient(app).send({"type": None, "task_id": "z"})
    assert seen == ["default"]


def test_unknown_type_without_default_fails():
    app = FastSQS()

    @app.route(Task)
    async def h(msg: Task):
        pass

    r = SQSTestClient(app).send({"type": "nope", "task_id": "1"}, message_id="u1")
    assert r == {"batchItemFailures": [{"itemIdentifier": "u1"}]}


def test_missing_type_without_default_fails():
    app = FastSQS()

    @app.route(Task)
    async def h(msg: Task):
        pass

    r = SQSTestClient(app).send({"task_id": "1"}, message_id="u2")
    assert r == {"batchItemFailures": [{"itemIdentifier": "u2"}]}


# ---- duplicate registration ----

def test_duplicate_route_raises():
    app = FastSQS()

    @app.route(Task)
    async def h(msg: Task):
        pass

    with pytest.raises(ValueError):
        @app.route(Task)
        async def h2(msg: Task):
            pass


# ---- base_event_class enforcement on a router ----

def test_base_event_class_enforced():
    class Base(SQSEvent):
        pass

    class Other(SQSEvent):
        task_id: str = "x"

    router = SQSRouter(base_event_class=Base)

    with pytest.raises(ValueError):
        @router.route(Other)
        async def h(msg: Other):
            pass


# ---- key-value routing via an included router ----

def test_included_router_key_value_routing():
    router = SQSRouter(discriminator="action")
    seen = []

    @router.route("ping")
    async def ping(msg, ctx):
        seen.append("ping")

    app = FastSQS()
    app.include_router(router)

    app.handler({"Records": [{"messageId": "m0", "body": json.dumps({"action": "ping"})}]}, None)
    assert seen == ["ping"]

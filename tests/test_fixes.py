"""Regression tests for the earlier audit fixes."""

import asyncio

import pytest

from fastsqs import FastSQS, SQSEvent
from fastsqs.middleware import Middleware
from fastsqs.testing import SQSTestClient
from fastsqs.utils import maybe_await


class Task(SQSEvent):
    task_id: str


# A failing after-hook must not abort the others nor mask the handler error.
def test_after_hook_failure_is_isolated():
    seen_errors = []

    class Boom(Middleware):
        async def after(self, payload, record, context, ctx, error):
            raise RuntimeError("after boom")

    class Recorder(Middleware):
        async def after(self, payload, record, context, ctx, error):
            seen_errors.append(error)

    app = FastSQS()
    app.add_middleware(Recorder())   # after() runs reversed -> Boom first, then Recorder
    app.add_middleware(Boom())

    @app.route(Task)
    async def handle(msg: Task):
        raise ValueError("handler fail")

    result = SQSTestClient(app).send({"type": "task", "task_id": "1"}, message_id="m-1")

    assert len(seen_errors) == 1
    assert isinstance(seen_errors[0], ValueError)      # saw the real handler error
    assert result == {"batchItemFailures": [{"itemIdentifier": "m-1"}]}  # not masked


# Calling the sync handler from inside a running loop must raise.
def test_handler_inside_running_loop_raises():
    app = FastSQS()

    @app.route(Task)
    async def handle(msg: Task):
        pass

    async def call_from_loop():
        with pytest.raises(RuntimeError):
            app.handler({"Records": [{"messageId": "m", "body": '{"type":"task","task_id":"1"}'}]}, None)

    asyncio.run(call_from_loop())


# maybe_await accepts both sync values and awaitables.
def test_maybe_await_handles_sync_and_async():
    async def run():
        assert await maybe_await(42) == 42

        async def coro():
            return "ok"

        assert await maybe_await(coro()) == "ok"

    asyncio.run(run())


# Registering two event models that resolve to the same message type raises.
def test_duplicate_message_type_raises():
    class Foo(SQSEvent):
        x: int = 0

    class foo(SQSEvent):  # same message type "foo"
        y: int = 0

    app = FastSQS()
    app.route(Foo)(lambda msg: None)
    with pytest.raises(ValueError, match="already exists"):
        app.route(foo)(lambda msg: None)

"""Regression tests for the audit fixes (3 HIGH + 3 MEDIUM)."""

import asyncio

import pytest

from fastsqs import FastSQS, SQSEvent
from fastsqs.middleware import (
    Middleware,
    ParallelizationMiddleware,
    ParallelizationConfig,
    ProcessingTimeMiddleware,
)
from fastsqs.testing import SQSTestClient
from fastsqs.utils import maybe_await


class Task(SQSEvent):
    task_id: str


# Built at IMPORT time (no running loop) to exercise the lazy/loop-aware semaphore.
_PARALLEL_APP = FastSQS()
_PARALLEL_APP.add_middleware(ParallelizationMiddleware(ParallelizationConfig(max_concurrent_messages=3)))


@_PARALLEL_APP.route(Task)
async def _parallel_handler(msg: Task):
    return msg.task_id


# HIGH #1 — a failing after-hook must not abort the others nor mask the handler error.
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

    assert len(seen_errors) == 1                       # Recorder.after still ran
    assert isinstance(seen_errors[0], ValueError)      # it saw the real handler error
    assert result == {"batchItemFailures": [{"itemIdentifier": "m-1"}]}  # error not masked


# MEDIUM #4 — calling the sync handler from inside a running loop must raise (type-based detection).
def test_handler_inside_running_loop_raises():
    app = FastSQS()

    @app.route(Task)
    async def handle(msg: Task):
        pass

    async def call_from_loop():
        with pytest.raises(RuntimeError):
            app.handler({"Records": [{"messageId": "m", "body": '{"type":"task","task_id":"1"}'}]}, None)

    asyncio.run(call_from_loop())


# MEDIUM #6 — module-level ParallelizationMiddleware reused across two asyncio.run loops.
def test_parallelization_works_across_invocations():
    client = SQSTestClient(_PARALLEL_APP)
    first = client.send({"type": "task", "task_id": "1"})    # loop #1
    second = client.send({"type": "task", "task_id": "2"})   # loop #2 (fresh asyncio.run)
    assert first == {"batchItemFailures": []}
    assert second == {"batchItemFailures": []}


# MEDIUM #5 — thread pool size scales with concurrency (was hardcoded min(32, 5)).
def test_thread_pool_size_scales_with_concurrency():
    assert ParallelizationConfig().thread_pool_size == 10
    assert ParallelizationConfig(max_concurrent_messages=20).thread_pool_size == 20
    assert ParallelizationConfig(max_concurrent_messages=100).thread_pool_size == 32  # capped


# HIGH #3 — maybe_await accepts both sync values and awaitables.
def test_maybe_await_handles_sync_and_async():
    async def run():
        assert await maybe_await(42) == 42

        async def coro():
            return "ok"

        assert await maybe_await(coro()) == "ok"

    asyncio.run(run())


# HIGH #3 — a sync callback is accepted by a middleware that awaits it.
def test_sync_callback_accepted_by_middleware():
    seen = []
    app = FastSQS()
    app.add_middleware(ProcessingTimeMiddleware(metrics_callback=lambda metrics: seen.append(metrics)))

    @app.route(Task)
    async def handle(msg: Task):
        pass

    result = SQSTestClient(app).send({"type": "task", "task_id": "1"})
    assert result == {"batchItemFailures": []}
    assert len(seen) == 1


# LOW — registering two event models that collide on a flexible-matching variant warns.
def test_flexible_match_variant_collision_warns():
    class Foo(SQSEvent):
        x: int = 0

    class FOO(SQSEvent):
        y: int = 0

    app = FastSQS()
    app.route(Foo)(lambda msg: None)  # first registration: no collision
    with pytest.warns(UserWarning, match="already maps"):
        app.route(FOO)(lambda msg: None)  # shares a flexible-matching variant -> warns

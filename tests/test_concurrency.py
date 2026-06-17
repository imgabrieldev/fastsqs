"""Concurrency & resource-safety battle tests."""

import asyncio

from fastsqs import FastSQS, SQSEvent
from fastsqs.middleware import (
    Middleware,
    ParallelizationMiddleware,
    ConcurrencyLimiter,
    VisibilityTimeoutMonitor,
)
from fastsqs.testing import SQSTestClient


class Task(SQSEvent):
    task_id: str


def _bodies(n):
    return [{"type": "task", "task_id": str(i)} for i in range(n)]


def test_max_concurrent_messages_is_enforced():
    """No more than max_concurrent_messages records run at once."""
    app = FastSQS(max_concurrent_messages=2)
    state = {"current": 0, "peak": 0}

    @app.route(Task)
    async def handle(msg: Task):
        state["current"] += 1
        state["peak"] = max(state["peak"], state["current"])
        await asyncio.sleep(0.02)
        state["current"] -= 1

    SQSTestClient(app).send_batch(_bodies(8))
    assert state["peak"] <= 2
    assert state["peak"] >= 2  # and it actually parallelizes up to the limit


def test_higher_limit_allows_more_overlap():
    app = FastSQS(max_concurrent_messages=5)
    state = {"current": 0, "peak": 0}

    @app.route(Task)
    async def handle(msg: Task):
        state["current"] += 1
        state["peak"] = max(state["peak"], state["current"])
        await asyncio.sleep(0.02)
        state["current"] -= 1

    SQSTestClient(app).send_batch(_bodies(10))
    assert 2 <= state["peak"] <= 5


def test_shared_middleware_atomic_counter_is_exact():
    """fastsqs's built-in stateful middleware increments with a plain ``+=``
    (no await between read and write), which is atomic under asyncio's single
    thread — so a concurrent batch counts exactly, with no lost updates. This
    pins that contract (e.g. QueueMetricsMiddleware counters)."""
    app = FastSQS(max_concurrent_messages=10)
    hits = {"n": 0}

    class Counter(Middleware):
        async def after(self, payload, record, context, ctx, error):
            hits["n"] += 1  # atomic: no await between read and write

    app.add_middleware(Counter())

    @app.route(Task)
    async def handle(msg: Task):
        await asyncio.sleep(0.001)  # force overlap across the batch

    SQSTestClient(app).send_batch(_bodies(50))
    assert hits["n"] == 50


# Regression for bug A: a before-hook raising must NOT leak the concurrency slot
# acquired by an earlier middleware (the after/release must still run).
def test_before_hook_failure_releases_concurrency_slot():
    app = FastSQS(max_concurrent_messages=2)
    limiter = ConcurrencyLimiter(max_concurrent=2)
    app.add_middleware(ParallelizationMiddleware(concurrency_limiter=limiter))

    class FailingBefore(Middleware):
        async def before(self, payload, record, context, ctx):
            raise ValueError("before boom")

    app.add_middleware(FailingBefore())

    @app.route(Task)
    async def handle(msg: Task):
        pass

    result = SQSTestClient(app).send_batch(_bodies(2))
    # both records fail (before raised) ...
    assert len(result["batchItemFailures"]) == 2
    # ... but the slots were released (no leak/deadlock)
    assert limiter.stats["active_count"] == 0

    # a subsequent batch must still run (would hang if slots had leaked)
    second = SQSTestClient(app).send_batch(_bodies(2))
    assert limiter.stats["active_count"] == 0
    assert len(second["batchItemFailures"]) == 2  # still fails, but no hang


def test_before_hook_failure_does_not_deadlock_subsequent_invocations():
    """The visibility monitor + concurrency slot must be cleaned even when a
    later before-hook raises, so repeated invocations never starve."""
    app = FastSQS(max_concurrent_messages=1)
    limiter = ConcurrencyLimiter(max_concurrent=1)
    app.add_middleware(ParallelizationMiddleware(concurrency_limiter=limiter))
    app.add_middleware(VisibilityTimeoutMonitor(default_visibility_timeout=30.0))

    class FailingBefore(Middleware):
        async def before(self, payload, record, context, ctx):
            raise RuntimeError("boom")

    app.add_middleware(FailingBefore())

    @app.route(Task)
    async def handle(msg: Task):
        pass

    client = SQSTestClient(app)
    for _ in range(5):
        client.send({"type": "task", "task_id": "1"})
        assert limiter.stats["active_count"] == 0

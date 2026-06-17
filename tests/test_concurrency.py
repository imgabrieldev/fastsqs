"""Concurrency & resource-safety battle tests."""

import asyncio

from fastsqs import FastSQS, SQSEvent
from fastsqs.middleware import Middleware
from fastsqs.testing import SQSTestClient


class Task(SQSEvent):
    task_id: str


def _bodies(n):
    return [{"type": "task", "task_id": str(i)} for i in range(n)]


class SlotMiddleware(Middleware):
    """Acquires a 'slot' in before() and releases it in after() — stands in for
    any resource-holding middleware (concurrency slot, monitor task, ...)."""

    def __init__(self, state):
        super().__init__()
        self.state = state

    async def before(self, payload, record, context, ctx):
        self.state["held"] += 1

    async def after(self, payload, record, context, ctx, error):
        self.state["held"] -= 1


class FailingBefore(Middleware):
    async def before(self, payload, record, context, ctx):
        raise ValueError("before boom")


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
    assert state["peak"] == 2  # caps at the limit and actually parallelizes to it


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
    """A plain ``+=`` (no await between read and write) is atomic under asyncio's
    single thread — a concurrent batch counts exactly, no lost updates."""
    app = FastSQS(max_concurrent_messages=10)
    hits = {"n": 0}

    class Counter(Middleware):
        async def after(self, payload, record, context, ctx, error):
            hits["n"] += 1

    app.add_middleware(Counter())

    @app.route(Task)
    async def handle(msg: Task):
        await asyncio.sleep(0.001)

    SQSTestClient(app).send_batch(_bodies(50))
    assert hits["n"] == 50


# Regression for the wind/unwind fix: a before-hook raising must NOT leak the
# resource a prior middleware acquired in before() — its after() must still run.
def test_before_hook_failure_releases_prior_middleware_resource():
    app = FastSQS(max_concurrent_messages=2)
    state = {"held": 0}
    app.add_middleware(SlotMiddleware(state))
    app.add_middleware(FailingBefore())  # before() raises AFTER SlotMiddleware entered

    @app.route(Task)
    async def handle(msg: Task):
        pass

    result = SQSTestClient(app).send_batch(_bodies(2))
    assert len(result["batchItemFailures"]) == 2  # both fail (before raised)
    assert state["held"] == 0  # SlotMiddleware.after still ran -> no leak


def test_before_hook_failure_does_not_leak_across_invocations():
    app = FastSQS(max_concurrent_messages=1)
    state = {"held": 0}
    app.add_middleware(SlotMiddleware(state))
    app.add_middleware(FailingBefore())

    @app.route(Task)
    async def handle(msg: Task):
        pass

    client = SQSTestClient(app)
    for _ in range(5):
        client.send({"type": "task", "task_id": "1"})
        assert state["held"] == 0  # never accumulates -> no starvation

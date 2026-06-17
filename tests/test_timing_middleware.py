"""TimingMiddleware: ctx.state output and unwind safety.

TimingMiddleware writes ``start_ns`` (time.perf_counter_ns) in ``before`` and a
rounded ``duration_ms`` float in ``after`` into ``ctx.state``, under configurable
keys. ``after`` reads the start via ``ctx.state.get`` so it no-ops when ``before``
never ran (a before-hook-failure unwind must not crash).

Note on ordering: the framework unwinds ``after`` hooks in REVERSE of the order
their ``before`` completed. So a middleware that wants to *read* the value Timing
writes in ``after`` must be added BEFORE TimingMiddleware, so its own ``after``
runs last (after Timing's). The app-driven cases below rely on that.
"""

import asyncio

import pytest

from fastsqs import FastSQS, SQSEvent, Context, State, QueueType
from fastsqs.middleware import Middleware, TimingMiddleware
from fastsqs.testing import SQSTestClient


class Task(SQSEvent):
    task_id: str = "x"


class CaptureAfter(Middleware):
    """Records a snapshot of selected ctx.state keys when its ``after`` runs."""

    def __init__(self, *keys):
        self.keys = keys
        self.captured = None

    async def after(self, payload, record, context, ctx, error):
        # Added BEFORE TimingMiddleware, so this ``after`` runs after Timing's
        # ``after`` during the reverse unwind — Timing has already written.
        self.captured = {k: ctx.state.get(k) for k in self.keys}


def _make_context():
    """A bare Context with an empty State, as the app would build per record."""
    return Context(
        message_id="m0",
        record={"messageId": "m0"},
        lambda_context=None,
        queue_type=QueueType.STANDARD,
        state=State(),
    )


# ---- duration_ms / start_ns presence via the app ----

def test_timing_writes_duration_ms_into_ctx_state():
    app = FastSQS()
    cap = CaptureAfter("duration_ms")
    app.add_middleware(cap)            # before Timing -> its after runs last
    app.add_middleware(TimingMiddleware())

    @app.route(Task)
    async def handle(msg: Task):
        pass

    r = SQSTestClient(app).send({"type": "task", "task_id": "1"})
    assert r == {"batchItemFailures": []}
    duration = cap.captured["duration_ms"]
    assert isinstance(duration, float)
    assert duration >= 0


def test_timing_writes_start_ns_into_ctx_state():
    app = FastSQS()
    app.add_middleware(TimingMiddleware())
    seen = {}

    @app.route(Task)
    async def handle(msg: Task, ctx: Context):
        # TimingMiddleware.before has already run by the time the handler runs.
        seen["start_ns"] = ctx.state.get("start_ns")

    SQSTestClient(app).send({"type": "task", "task_id": "1"})
    assert isinstance(seen["start_ns"], int)


def test_timing_custom_store_keys_honored():
    app = FastSQS()
    cap = CaptureAfter("t0", "ms", "start_ns", "duration_ms")
    app.add_middleware(cap)
    app.add_middleware(TimingMiddleware(store_key_start="t0", store_key_ms="ms"))

    @app.route(Task)
    async def handle(msg: Task):
        pass

    SQSTestClient(app).send({"type": "task", "task_id": "1"})
    assert isinstance(cap.captured["t0"], int)
    assert isinstance(cap.captured["ms"], float)
    # default keys must NOT be touched when custom keys are configured
    assert cap.captured["start_ns"] is None
    assert cap.captured["duration_ms"] is None


# ---- unwind safety: after no-ops when before never ran ----

def test_timing_after_noop_when_before_did_not_run():
    mw = TimingMiddleware()
    ctx = _make_context()
    payload = {"type": "task", "task_id": "1"}
    record = ctx.record

    # No before() -> start key absent -> guarded no-op, must not raise.
    asyncio.run(mw.after(payload, record, None, ctx, error=None))
    assert "duration_ms" not in ctx.state
    assert "start_ns" not in ctx.state


def test_timing_records_duration_even_on_handler_failure():
    app = FastSQS()
    cap = CaptureAfter("duration_ms")
    app.add_middleware(cap)
    app.add_middleware(TimingMiddleware())

    @app.route(Task)
    async def handle(msg: Task):
        raise ValueError("boom")

    r = SQSTestClient(app).send({"type": "task", "task_id": "1"}, message_id="f1")
    # The record failed, but Timing's before ran so after still records the duration.
    assert r == {"batchItemFailures": [{"itemIdentifier": "f1"}]}
    duration = cap.captured["duration_ms"]
    assert isinstance(duration, float)
    assert duration >= 0


# ---- direct unit: before -> after sets duration ----

@pytest.mark.parametrize("error", [None, ValueError("x")])
def test_timing_before_then_after_directly_sets_duration(error):
    mw = TimingMiddleware()
    ctx = _make_context()
    payload = {"type": "task", "task_id": "1"}
    record = ctx.record

    async def drive():
        await mw.before(payload, record, None, ctx)
        await mw.after(payload, record, None, ctx, error=error)

    asyncio.run(drive())

    assert isinstance(ctx.state[mw.store_key_start], int)
    duration = ctx.state["duration_ms"]
    assert isinstance(duration, float)
    assert duration >= 0

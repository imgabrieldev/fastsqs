"""ctx.handler_result capture/propagation + async/sync handler entry points.

The ``after`` contract and ``LoggingMiddleware`` both read ``ctx.handler_result``,
so these tests pin down that a handler return value lands there, stays ``None`` on
a ``None``-return or on a handler exception, and that an ``after`` hook can observe
it. They also cover ``async_handler()`` as the correct in-loop entry point (the
positive case; the sync-in-loop ``handler()`` raise is exercised here too) and
sync-vs-async parity.

real-behavior note: ``ctx.handler_result`` is assigned in the router only AFTER
``invoke_handler`` returns (router.py: ``ctx.handler_result = result``). If the
handler raises, ``_invoke`` raises before that assignment, so ``handler_result``
keeps its ``Context`` default of ``None`` even though the ``after`` hook still runs
with the error.
"""

import asyncio

import pytest

from fastsqs import FastSQS, SQSEvent, Context, QueueType
from fastsqs.middleware import Middleware
from fastsqs.testing import SQSTestClient, make_event, make_record


class Task(SQSEvent):
    task_id: str = "x"


class _Capture(Middleware):
    """after-hook that records ctx.handler_result and the error it was given."""

    def __init__(self):
        self.results = []
        self.errors = []
        self.captured = "<unset>"

    async def after(self, payload, record, context, ctx, error):
        self.captured = ctx.handler_result
        self.results.append(ctx.handler_result)
        self.errors.append(error)


# ---- handler_result capture / propagation ----------------------------------

def test_handler_return_value_stored_on_ctx_handler_result():
    app = FastSQS()
    holder = {}

    class Noop(Middleware):
        async def after(self, payload, record, context, ctx, error):
            holder["result"] = ctx.handler_result

    app.add_middleware(Noop())

    @app.route(Task)
    async def handle(msg: Task):
        return "RESULT"

    r = SQSTestClient(app).send({"type": "task", "task_id": "1"})
    assert r == {"batchItemFailures": []}
    assert holder["result"] == "RESULT"


def test_after_hook_can_read_handler_result():
    app = FastSQS()
    cap = _Capture()
    app.add_middleware(cap)

    @app.route(Task)
    async def handle(msg: Task):
        return {"ok": True}

    SQSTestClient(app).send({"type": "task", "task_id": "1"})
    assert cap.results == [{"ok": True}]


def test_handler_result_none_when_handler_returns_none():
    app = FastSQS()
    cap = _Capture()
    app.add_middleware(cap)

    @app.route(Task)
    async def handle(msg: Task):
        return None

    r = SQSTestClient(app).send({"type": "task", "task_id": "1"})
    assert r == {"batchItemFailures": []}
    assert cap.captured is None
    assert cap.results == [None]


def test_handler_result_remains_none_on_handler_exception():
    """A raising handler never reaches the ``ctx.handler_result = result``
    assignment, so it stays None; the after hook still runs and sees the error,
    and the record is reported as failed."""
    app = FastSQS()
    cap = _Capture()
    app.add_middleware(cap)

    @app.route(Task)
    async def handle(msg: Task):
        raise ValueError("boom")

    r = SQSTestClient(app).send({"type": "task", "task_id": "1"}, message_id="err1")
    assert r == {"batchItemFailures": [{"itemIdentifier": "err1"}]}
    assert cap.captured is None
    assert len(cap.errors) == 1
    assert isinstance(cap.errors[0], ValueError)
    assert str(cap.errors[0]) == "boom"


def test_sync_handler_return_value_captured():
    """A non-async handler returns directly; invoke_handler returns it without
    awaiting and the value still lands on ctx.handler_result."""
    app = FastSQS()
    cap = _Capture()
    app.add_middleware(cap)

    @app.route(Task)
    def handle(msg: Task):  # sync handler
        return 42

    SQSTestClient(app).send({"type": "task", "task_id": "1"})
    assert cap.captured == 42
    assert cap.results == [42]


@pytest.mark.parametrize("is_async", [True, False])
def test_handler_result_captured_for_sync_and_async(is_async):
    """sync/async parity: the same return value reaches ctx.handler_result
    regardless of whether the handler is a coroutine function."""
    app = FastSQS()
    cap = _Capture()
    app.add_middleware(cap)

    if is_async:
        @app.route(Task)
        async def handle(msg: Task):
            return "VAL"
    else:
        @app.route(Task)
        def handle(msg: Task):
            return "VAL"

    SQSTestClient(app).send({"type": "task", "task_id": "1"})
    assert cap.captured == "VAL"


# ---- async_handler entry point ----------------------------------------------

def _build_app():
    app = FastSQS()

    @app.route(Task)
    async def handle(msg: Task):
        if msg.task_id == "boom":
            raise ValueError("boom")
        return msg.task_id

    return app


def test_async_handler_returns_batch_item_failures():
    app = _build_app()
    record = make_record({"type": "task", "task_id": "ok"}, message_id="a1")

    async def run():
        return await app.async_handler(make_event([record]), None)

    result = asyncio.run(run())
    assert result == {"batchItemFailures": []}


def test_async_handler_works_inside_running_loop():
    """async_handler is the correct entry point inside a running loop: awaiting
    it for a failing record returns the failure list WITHOUT raising
    RuntimeError (unlike the sync handler())."""
    app = _build_app()
    record = make_record({"type": "task", "task_id": "boom"}, message_id="bad1")

    async def run():
        # Confirm we are genuinely inside a running loop.
        assert asyncio.get_running_loop() is not None
        return await app.async_handler(make_event([record]), None)

    result = asyncio.run(run())
    assert result == {"batchItemFailures": [{"itemIdentifier": "bad1"}]}


def test_sync_handler_raises_inside_running_loop():
    """Contrast: the sync handler() entry point refuses to run inside a loop."""
    app = _build_app()
    record = make_record({"type": "task", "task_id": "ok"}, message_id="x1")

    async def run():
        app.handler(make_event([record]), None)

    with pytest.raises(RuntimeError):
        asyncio.run(run())


def test_async_handler_and_sync_handler_produce_same_result():
    """Same app + same event (one passing, one failing record): the sync
    handler() (outside any loop) and a fresh asyncio.run(async_handler(...))
    produce identical batchItemFailures."""
    app = _build_app()
    event = make_event([
        make_record({"type": "task", "task_id": "ok"}, message_id="ok1"),
        make_record({"type": "task", "task_id": "boom"}, message_id="bad1"),
    ])
    expected = {"batchItemFailures": [{"itemIdentifier": "bad1"}]}

    sync_result = app.handler(event, None)
    async_result = asyncio.run(app.async_handler(event, None))

    assert sync_result == expected
    assert async_result == expected
    assert sync_result == async_result


def test_handler_result_typed_attribute_default_is_none():
    """Sanity check on the Context contract: handler_result defaults to None."""
    ctx = Context(
        message_id="m",
        record={},
        lambda_context=None,
        queue_type=QueueType.STANDARD,
    )
    assert ctx.handler_result is None

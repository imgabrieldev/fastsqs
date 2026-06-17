"""Multi-tier middleware composition battle tests.

Existing tests only exercise a single middleware. These cover the full chain
``parent_middlewares + self._middlewares + route_middlewares`` across app,
included-router and per-route tiers: before runs in order, after in strict
reverse, app-level wraps OUTSIDE route-level, the same Context is threaded
everywhere, sync (non-async) hooks and the missing-hook noop branch are
supported, and a ``before`` raising aborts only its own record without
poisoning batch siblings.

Layering note (verified against the v1 source): app-level middlewares live on
``FastSQS._middlewares`` and wrap ``_route`` inside ``_handle_record`` (the
outermost stack). They are NOT forwarded as ``parent_middlewares`` to included
routers; ``_route`` calls ``router.dispatch`` with the default empty
``parent_middlewares``. So the effective order is app (outer) -> router
``_middlewares`` -> per-route middlewares (inner), which still yields the
``['A','B','C']`` before / ``['C','B','A']`` after ordering the cases expect.
"""

import json

import pytest

from fastsqs import FastSQS, SQSRouter, SQSEvent, Context
from fastsqs.middleware import Middleware
from fastsqs.testing import SQSTestClient, RecordSpec


class Task(SQSEvent):
    task_id: str = "x"


def _recorder(order, label):
    """A Middleware that appends ``label`` in before() and after()."""

    class _Recorder(Middleware):
        async def before(self, payload, record, context, ctx):
            order.append((label, "before"))

        async def after(self, payload, record, context, ctx, error):
            order.append((label, "after"))

    return _Recorder()


# ---- before / after ordering across app + router + per-route tiers ----

def test_before_order_app_then_router_then_route():
    order = []
    app = FastSQS()
    app.add_middleware(_recorder(order, "A"))          # app-level (outermost)

    router = SQSRouter()
    router.add_middleware(_recorder(order, "B"))       # included-router level

    @router.route(Task, middlewares=[_recorder(order, "C")])  # per-route
    async def handle(msg: Task):
        order.append(("handler", "run"))

    app.include_router(router)

    r = SQSTestClient(app).send({"type": "task", "task_id": "1"})
    assert r == {"batchItemFailures": []}

    befores = [label for (label, phase) in order if phase == "before"]
    assert befores == ["A", "B", "C"]               # parent + self + route


def test_after_order_is_strict_reverse_of_before():
    order = []
    app = FastSQS()
    app.add_middleware(_recorder(order, "A"))

    router = SQSRouter()
    router.add_middleware(_recorder(order, "B"))

    @router.route(Task, middlewares=[_recorder(order, "C")])
    async def handle(msg: Task):
        order.append(("handler", "run"))

    app.include_router(router)

    SQSTestClient(app).send({"type": "task", "task_id": "1"})

    afters = [label for (label, phase) in order if phase == "after"]
    assert afters == ["C", "B", "A"]                # LIFO unwind, strict reverse


def test_app_middleware_wraps_outside_route_middleware():
    seq = []
    app = FastSQS()

    class App(Middleware):
        async def before(self, payload, record, context, ctx):
            seq.append("app_before")

        async def after(self, payload, record, context, ctx, error):
            seq.append("app_after")

    class Route(Middleware):
        async def before(self, payload, record, context, ctx):
            seq.append("route_before")

        async def after(self, payload, record, context, ctx, error):
            seq.append("route_after")

    app.add_middleware(App())

    @app.route(Task, middlewares=[Route()])
    async def handle(msg: Task):
        seq.append("handler")

    SQSTestClient(app).send({"type": "task", "task_id": "1"})
    assert seq == [
        "app_before",
        "route_before",
        "handler",
        "route_after",
        "app_after",
    ]


def test_multiple_middlewares_all_receive_same_ctx_instance():
    seen_ids = []
    app = FastSQS()

    class CaptureCtx(Middleware):
        async def before(self, payload, record, context, ctx):
            seen_ids.append(id(ctx))

        async def after(self, payload, record, context, ctx, error):
            seen_ids.append(id(ctx))

    app.add_middleware(CaptureCtx())

    router = SQSRouter()
    router.add_middleware(CaptureCtx())

    @router.route(Task, middlewares=[CaptureCtx()])
    async def handle(msg: Task, ctx: Context):
        seen_ids.append(id(ctx))

    app.include_router(router)

    SQSTestClient(app).send({"type": "task", "task_id": "1"})
    # 3 middlewares x (before + after) + 1 handler = 7 captures, all identical.
    assert len(seen_ids) == 7
    assert len(set(seen_ids)) == 1


# ---- sync (non-async) hook support via call_middleware_hook ----

def test_sync_before_and_after_hooks_supported():
    order = []
    app = FastSQS()

    class SyncMW(Middleware):
        def before(self, payload, record, context, ctx):   # plain def
            order.append("before")

        def after(self, payload, record, context, ctx, error):  # plain def
            order.append("after")

    app.add_middleware(SyncMW())

    @app.route(Task)
    async def handle(msg: Task):
        order.append("handler")

    r = SQSTestClient(app).send({"type": "task", "task_id": "1"})
    assert r == {"batchItemFailures": []}
    assert order == ["before", "handler", "after"]


def test_middleware_missing_before_method_is_noop():
    order = []
    app = FastSQS()

    # A middleware-like object that defines ONLY after(); it has no ``before``
    # attribute at all, so call_middleware_hook hits the getattr None -> _noop
    # branch. Hooks are duck-typed, so it need not subclass Middleware.
    class OnlyAfter:
        async def after(self, payload, record, context, ctx, error):
            order.append("after")

    app.add_middleware(OnlyAfter())

    @app.route(Task)
    async def handle(msg: Task):
        order.append("handler")

    r = SQSTestClient(app).send({"type": "task", "task_id": "1"})
    assert r == {"batchItemFailures": []}
    assert order == ["handler", "after"]            # no before, no crash


def test_mixed_sync_and_async_hooks_in_one_chain():
    order = []
    app = FastSQS()

    class SyncMW(Middleware):
        def before(self, payload, record, context, ctx):
            order.append(("sync", "before"))

        def after(self, payload, record, context, ctx, error):
            order.append(("sync", "after"))

    class AsyncMW(Middleware):
        async def before(self, payload, record, context, ctx):
            order.append(("async", "before"))

        async def after(self, payload, record, context, ctx, error):
            order.append(("async", "after"))

    app.add_middleware(SyncMW())
    app.add_middleware(AsyncMW())

    @app.route(Task)
    async def handle(msg: Task):
        order.append(("handler", "run"))

    r = SQSTestClient(app).send({"type": "task", "task_id": "1"})
    assert r == {"batchItemFailures": []}

    befores = [name for (name, phase) in order if phase == "before"]
    afters = [name for (name, phase) in order if phase == "after"]
    assert befores == ["sync", "async"]
    assert afters == ["async", "sync"]              # strict reverse, flavor-agnostic


# ---- a before() raising aborts the handler; entered middlewares still unwind ----

def test_before_raise_aborts_handler_but_after_of_entered_still_runs():
    events = []
    captured_errors = []
    app = FastSQS()

    class M1(Middleware):
        async def before(self, payload, record, context, ctx):
            events.append("m1_before")

        async def after(self, payload, record, context, ctx, error):
            events.append("m1_after")
            captured_errors.append(error)

    class M2(Middleware):
        async def before(self, payload, record, context, ctx):
            raise RuntimeError("m2 before boom")

        async def after(self, payload, record, context, ctx, error):
            # M2 never entered (its before raised), so this must NOT run.
            events.append("m2_after")

    app.add_middleware(M1())
    app.add_middleware(M2())

    @app.route(Task)
    async def handle(msg: Task):
        events.append("handler")

    r = SQSTestClient(app).send({"type": "task", "task_id": "1"}, message_id="x1")

    assert "handler" not in events                  # handler aborted
    assert "m1_before" in events                    # M1 entered
    assert "m1_after" in events                     # M1 unwound
    assert "m2_after" not in events                 # M2 never entered, not unwound
    assert len(captured_errors) == 1
    assert isinstance(captured_errors[0], RuntimeError)   # after sees the error
    assert r == {"batchItemFailures": [{"itemIdentifier": "x1"}]}  # record failed


def test_before_failure_on_one_record_does_not_poison_siblings():
    processed = []
    app = FastSQS()

    class FailOnBody(Middleware):
        async def before(self, payload, record, context, ctx):
            if "fail" in record.get("body", ""):
                raise RuntimeError("targeted before failure")

    app.add_middleware(FailOnBody())

    @app.route(Task)
    async def handle(msg: Task):
        processed.append(msg.task_id)

    r = SQSTestClient(app).send_batch([
        RecordSpec({"type": "task", "task_id": "ok"}, message_id="good"),
        RecordSpec({"type": "task", "task_id": "fail"}, message_id="bad"),
    ])

    assert processed == ["ok"]                      # sibling handler still ran
    assert r["batchItemFailures"] == [{"itemIdentifier": "bad"}]  # only failer


# ---- adjacent: app-only single-tier sanity (no router) still orders correctly ----

@pytest.mark.parametrize("n", [2, 3])
def test_app_level_only_chain_orders_and_reverses(n):
    order = []
    app = FastSQS()
    labels = [chr(ord("A") + i) for i in range(n)]
    for label in labels:
        app.add_middleware(_recorder(order, label))

    @app.route(Task)
    async def handle(msg: Task):
        order.append(("handler", "run"))

    app.handler(
        {"Records": [{"messageId": "m0", "body": json.dumps({"type": "task", "task_id": "1"})}]},
        None,
    )

    befores = [label for (label, phase) in order if phase == "before"]
    afters = [label for (label, phase) in order if phase == "after"]
    assert befores == labels
    assert afters == list(reversed(labels))

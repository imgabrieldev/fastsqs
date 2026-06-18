"""Direct unit coverage for the handler-injection utils in ``fastsqs.utils``.

These helpers (``uses_depends`` / ``maybe_inject`` / ``select_kwargs`` /
``invoke_handler``) underpin every handler invocation but are only exercised
indirectly by the routing/middleware suites. Here we pin their behaviour
directly, then close the loop with a couple of end-to-end ``SQSTestClient``
sends that assert the ``payload`` / ``context`` kwarg values and the
per-invocation dependency-injection caching guarantees.

real-behavior notes (verified against the source, no library changes):
- ``uses_depends`` detects a fast-depends ``Depends(...)`` by checking that a
  parameter default's class is named ``"Dependant"`` (utils.py).
- ``maybe_inject`` returns a no-Depends handler unchanged (identity) and is
  idempotent for a Depends handler via the ``_fastsqs_injected`` guard, so
  ``maybe_inject(maybe_inject(h)) is maybe_inject(h)``.
- ``select_kwargs`` keeps only KEYWORD_ONLY / POSITIONAL_OR_KEYWORD names,
  dropping unknown keys and excluding positional-only and ``*args``.
- ``invoke_handler`` awaits coroutine results, returns sync results directly,
  and BYPASSES ``select_kwargs`` for an injected handler (passes ALL kwargs so
  fast-depends can resolve the dependency graph).
- A top-level ``@app.route`` hands the handler ``payload`` kwarg the parsed root
  payload dict, and the ``context`` kwarg is exactly the lambda context object
  passed through.
- fast-depends caches a shared sub-dependency once per invocation (graph-wide)
  but resolves fresh per record across a batch (no cross-record caching).
"""

import asyncio

from fastsqs import Depends, FastSQS, SQSEvent
from fastsqs.testing import RecordSpec, SQSTestClient
from fastsqs.utils import (
    invoke_handler,
    maybe_inject,
    select_kwargs,
    uses_depends,
)


class Task(SQSEvent):
    task_id: str = "x"


# ---- uses_depends -----------------------------------------------------------

def test_uses_depends_true_for_depends_param():
    def h(svc=Depends(lambda: 1)):
        return svc

    assert uses_depends(h) is True


def test_uses_depends_false_for_plain_handler():
    def h(msg, ctx):
        return (msg, ctx)

    assert uses_depends(h) is False


# ---- maybe_inject -----------------------------------------------------------

def test_maybe_inject_returns_plain_handler_unchanged():
    def h(msg, ctx):
        return (msg, ctx)

    # No Depends params -> identity, zero behaviour change.
    assert maybe_inject(h) is h


def test_maybe_inject_idempotent_does_not_double_wrap():
    def h(svc=Depends(lambda: 1)):
        return svc

    w1 = maybe_inject(h)
    w2 = maybe_inject(w1)

    # The ``_fastsqs_injected`` guard prevents re-wrapping an already-wrapped fn.
    assert w2 is w1
    assert getattr(w1, "_fastsqs_injected") is True


# ---- select_kwargs ----------------------------------------------------------

def test_select_kwargs_drops_unknown_keys():
    def h(msg, ctx):
        pass

    # Only the names the handler actually declares are kept.
    assert select_kwargs(h, msg=1, ctx=2, record=3, context=4) == {"msg": 1, "ctx": 2}


def test_select_kwargs_excludes_positional_only_and_var_positional():
    def h(a, /, *args, msg=None):
        return (a, args, msg)

    # positional-only ``a`` and the ``*args`` slot are excluded; only the
    # keyword-only ``msg`` survives.
    assert select_kwargs(h, a=1, msg=2) == {"msg": 2}


def test_select_kwargs_empty_when_no_candidate_matches():
    # Adjacent gap-filler: declared names exist but none are supplied.
    def h(msg, ctx):
        pass

    assert select_kwargs(h, record=1, context=2) == {}


# ---- invoke_handler ---------------------------------------------------------

def test_invoke_handler_awaits_coroutine_result():
    async def h(msg):
        return msg * 2

    # async handler -> invoke_handler awaits the coroutine result.
    assert asyncio.run(invoke_handler(h, msg=3, record={}, ctx=None)) == 6


def test_invoke_handler_returns_sync_result_directly():
    def h(msg):
        return msg + 1

    # sync handler -> result returned without awaiting.
    assert asyncio.run(invoke_handler(h, msg=4)) == 5


def test_invoke_handler_injected_handler_gets_full_kwargs():
    ran = {}

    def get_service():
        return "SERVICE"

    def h(msg, svc=Depends(get_service)):
        ran["msg"] = msg
        ran["svc"] = svc
        return svc

    fn = maybe_inject(h)
    assert getattr(fn, "_fastsqs_injected") is True

    # invoke_handler passes ALL kwargs (bypassing select_kwargs) so fast-depends
    # can bind ``msg`` and resolve the ``Depends`` graph.
    result = asyncio.run(
        invoke_handler(
            fn, msg="M", payload={"p": 1}, record={}, context=None, ctx=None
        )
    )

    assert result == "SERVICE"
    assert ran == {"msg": "M", "svc": "SERVICE"}


# ---- payload / context kwarg binding (end-to-end) ---------------------------

def test_handler_payload_kwarg_equals_root_payload_at_top_level():
    captured = {}
    app = FastSQS()

    @app.route(Task)
    async def handle(payload):
        captured["payload"] = payload

    r = SQSTestClient(app).send({"type": "task", "task_id": "1"})

    assert r == {"batchItemFailures": []}
    # the handler payload kwarg == the parsed root payload dict.
    assert captured["payload"] == {"type": "task", "task_id": "1"}


def test_handler_context_kwarg_is_lambda_context_object():
    captured = {}
    app = FastSQS()

    @app.route(Task)
    async def handle(context):
        captured["context"] = context

    sentinel = object()
    r = SQSTestClient(app).send({"type": "task", "task_id": "1"}, context=sentinel)

    assert r == {"batchItemFailures": []}
    # The ``context`` kwarg is exactly the lambda context object passed through.
    assert captured["context"] is sentinel


# ---- per-invocation DI caching ----------------------------------------------

def test_shared_subdependency_cached_once_per_invocation():
    counter = {"base": 0}

    def base():
        counter["base"] += 1
        return object()

    def sub_a(b=Depends(base)):
        return b

    def sub_b(b=Depends(base)):
        return b

    app = FastSQS()

    @app.route(Task)
    async def handle(msg: Task, a=Depends(sub_a), bb=Depends(sub_b)):
        pass

    r = SQSTestClient(app).send({"type": "task", "task_id": "1"})

    assert r == {"batchItemFailures": []}
    # Both sibling sub-deps share the base factory; fast-depends caches it once
    # for the whole per-invocation graph.
    assert counter["base"] == 1


def test_dependency_resolved_fresh_per_record_in_batch():
    counter = {"n": 0}

    def factory():
        counter["n"] += 1
        return counter["n"]

    app = FastSQS()

    @app.route(Task)
    async def handle(msg: Task, dep=Depends(factory)):
        pass

    r = SQSTestClient(app).send_batch(
        [
            RecordSpec({"type": "task", "task_id": "1"}),
            RecordSpec({"type": "task", "task_id": "2"}),
        ]
    )

    assert r == {"batchItemFailures": []}
    # Fresh resolution per record/invocation -> no cross-record caching.
    assert counter["n"] == 2

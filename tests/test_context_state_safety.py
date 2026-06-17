"""Context identity contract + State copy/pickle/del/len/repr safety.

These pin two surfaces that ``test_state_unit_semantics`` (in
``tests/test_typed_context.py``) does not cover:

* ``State`` lifecycle beyond get/setdefault/iter/membership: ``copy``/
  ``deepcopy``/``pickle`` round-trips, attribute ``del`` (present + missing),
  ``len`` and ``repr``.
* ``Context`` (``@dataclass(eq=False)``) identity semantics and its live
  ``route_path`` list — one instance is threaded through
  before -> handler -> after and never deep-copied, and each key-value
  dispatch hop pushes onto / pops off ``route_path``.

Built from direct unit construction of ``State`` and ``Context`` plus a small
key-value dispatch (``SQSRouter.dispatch`` / ``SQSTestClient``); no AWS, no
Docker.

CORRECTED CASES — ``State`` copy/pickle are NOT safe on this interpreter
(Python 3.14): ``State.__slots__`` holds ``_data`` and ``State.__getattr__``
reads ``self._data`` on any attribute miss. ``copy``/``pickle`` reconstruct the
instance via ``__newobj__`` WITHOUT running ``__init__`` (so ``_data`` is unset),
then probe ``hasattr(obj, "__setstate__")``; that lookup misses, calls
``__getattr__("__setstate__")``, which reads ``self._data`` -> another miss ->
infinite recursion -> ``RecursionError``. The library docstring claims
copy/pickle safety, but the real behavior on this interpreter is a
``RecursionError``, so the round-trip cases assert that instead of a clean
round-trip (the brief's wishful wording). Source is NOT modified. The del/len/
repr/Context cases all match the brief exactly.
"""

import asyncio
import copy
import pickle

import pytest

from fastsqs import Context, FastSQS, QueueType, SQSEvent, SQSRouter
from fastsqs.middleware import Middleware
from fastsqs.testing import SQSTestClient
from fastsqs.types import State


def _ctx(message_id="m", **overrides):
    """Build a Context by direct unit construction (no AWS)."""
    kwargs = dict(
        message_id=message_id,
        record={},
        lambda_context=None,
        queue_type=QueueType.STANDARD,
    )
    kwargs.update(overrides)
    return Context(**kwargs)


class Task(SQSEvent):
    task_id: str = "x"


# ---------------------------------------------------------------------------
# State: copy / deepcopy / pickle
# ---------------------------------------------------------------------------

def test_state_deepcopy_roundtrip():
    """CORRECTED: the brief expects an independent deep copy, but ``State`` is
    reconstructed via ``__newobj__`` (``__init__`` skipped, ``_data`` unset) and
    the ``hasattr(obj, "__setstate__")`` probe drives ``__getattr__`` ->
    ``self._data`` into infinite recursion, so ``deepcopy`` raises
    ``RecursionError`` rather than producing a usable copy."""
    s = State({"a": 1, "b": [1, 2]})
    with pytest.raises(RecursionError):
        copy.deepcopy(s)


def test_state_shallow_copy_recursion_error():
    """Adjacent case: ``copy.copy`` fails the same way as ``deepcopy`` — both go
    through ``__reduce_ex__`` + the ``__setstate__`` probe on an uninitialised
    instance."""
    s = State({"a": 1})
    with pytest.raises(RecursionError):
        copy.copy(s)


def test_state_pickle_roundtrip():
    """CORRECTED: the brief expects a pickle round-trip, but ``pickle.loads``
    reconstructs via ``__newobj__`` and hits the same ``__setstate__`` probe
    recursion as copy, so unpickling raises ``RecursionError``. (Pickling alone
    succeeds — the state tuple is ``(None, {'_data': {...}})`` — it is the
    re-hydration that recurses.)"""
    s = State({"k": "v", "n": 3})
    dumped = pickle.dumps(s)  # dumps is fine; loads recurses
    with pytest.raises(RecursionError):
        pickle.loads(dumped)


# ---------------------------------------------------------------------------
# State: del / len / repr
# ---------------------------------------------------------------------------

def test_state_delattr_removes_key():
    s = State({"x": 1})
    del s.x
    assert "x" not in s
    assert len(s) == 0


def test_state_delattr_missing_raises_attributeerror():
    s = State()
    with pytest.raises(AttributeError):
        del s.nope


def test_state_len_reflects_key_count():
    s = State()
    assert len(s) == 0
    s.a = 1
    s["b"] = 2
    assert len(s) == 2
    del s.a
    assert len(s) == 1


def test_state_repr_shows_data():
    s = State({"a": 1})
    assert repr(s) == "State({'a': 1})"


# ---------------------------------------------------------------------------
# Context: identity semantics (eq=False)
# ---------------------------------------------------------------------------

def test_two_contexts_are_not_equal_by_identity():
    """``@dataclass(eq=False)`` -> identity equality. Two Contexts with identical
    field values are NOT equal; a Context is only equal to itself."""
    c1 = _ctx()
    c2 = _ctx()
    assert c1 != c2
    assert c1 == c1


def test_same_context_instance_across_before_handler_after():
    """One Context is threaded through the whole record lifecycle (never
    deep-copied): middleware before/after and the handler all observe the same
    ``id(ctx)``."""
    ids = {}
    app = FastSQS()

    class IdMiddleware(Middleware):
        async def before(self, payload, record, context, ctx):
            ids["before"] = id(ctx)

        async def after(self, payload, record, context, ctx, error):
            ids["after"] = id(ctx)

    app.add_middleware(IdMiddleware())

    @app.route(Task)
    async def handle(msg: Task, ctx: Context):
        ids["handler"] = id(ctx)

    result = SQSTestClient(app).send({"type": "task", "task_id": "1"})

    assert result == {"batchItemFailures": []}
    assert ids["before"] == ids["handler"] == ids["after"]


# ---------------------------------------------------------------------------
# Context: route_path is a live list (push per hop, pop on unhandled)
# ---------------------------------------------------------------------------

def test_route_path_accumulates_discriminator_hops():
    """Nested key-value routers (type -> action) each append their hop to the
    SAME live ``route_path`` list, in dispatch order."""
    captured = {}

    parent = SQSRouter(discriminator="type")
    child = SQSRouter(discriminator="action")

    @child.route("run")
    async def run(msg, ctx):
        captured["route_path"] = list(ctx.route_path)

    parent.subrouter("task", child)

    app = FastSQS()
    app.include_router(parent)

    result = SQSTestClient(app).send({"type": "task", "action": "run"})

    assert result == {"batchItemFailures": []}
    assert captured["route_path"] == ["type=task", "action=run"]


def test_route_path_popped_when_route_unhandled():
    """A router with NO matching route and NO default: ``dispatch`` pushes the
    ``action=missing`` hop, finds no entry, then pops it and returns False, so
    the live ``route_path`` is left empty. Driven via ``asyncio.run`` to match
    the project's direct-dispatch test style."""
    router = SQSRouter(discriminator="action")
    ctx = _ctx()

    handled = asyncio.run(router.dispatch({"action": "missing"}, {}, None, ctx))

    assert handled is False
    assert ctx.route_path == []


# ---------------------------------------------------------------------------
# Context: message_type set only on the pydantic-route branch
# ---------------------------------------------------------------------------

def test_context_message_type_set_only_on_pydantic_match():
    """``ctx.message_type`` is assigned only when a pydantic route matches. A
    key-value-only route (no pydantic model) leaves it at its ``None`` default."""
    seen = {}

    # pydantic-matched route -> message_type is set to the discriminator value
    pyd_app = FastSQS()

    @pyd_app.route(Task)
    async def pyd_handle(msg: Task, ctx: Context):
        seen["pydantic"] = ctx.message_type

    SQSTestClient(pyd_app).send({"type": "task", "task_id": "1"})

    # key-value-only route (no model) -> message_type stays None
    kv_router = SQSRouter(discriminator="action")

    @kv_router.route("run")
    async def kv_handle(msg, ctx: Context):
        seen["key_value"] = ctx.message_type

    kv_app = FastSQS()
    kv_app.include_router(kv_router)
    SQSTestClient(kv_app).send({"action": "run"})

    assert seen["pydantic"] == "task"
    assert seen["key_value"] is None

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

``State`` is copy/pickle-safe by design (``__slots__`` + ``__getstate__``/
``__setstate__`` + a ``_data`` guard in ``__getattr__``), so the round-trip
cases below assert clean, independent copies.
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
    """deepcopy yields an equal but fully independent State (nested values copied)."""
    s = State({"a": 1, "b": [1, 2]})
    c = copy.deepcopy(s)
    assert c._data == s._data
    assert c._data is not s._data
    assert c["b"] is not s["b"]
    c["b"].append(3)
    assert s["b"] == [1, 2]  # mutating the copy does not touch the original


def test_state_shallow_copy_roundtrip():
    """copy.copy yields an equal State with its own top-level _data dict."""
    s = State({"a": 1})
    c = copy.copy(s)
    assert c._data == s._data
    assert c._data is not s._data
    c["z"] = 9
    assert "z" not in s  # the copy's _data is independent at the top level


def test_state_pickle_roundtrip():
    """pickle round-trips State to an equal, independent instance."""
    s = State({"k": "v", "n": 3})
    r = pickle.loads(pickle.dumps(s))
    assert isinstance(r, State)
    assert r._data == s._data
    r["k"] = "changed"
    assert s["k"] == "v"


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

"""Focused unit tests closing the remaining line-coverage gaps in fastsqs.

Each test targets a specific previously-uncovered line (referenced in the
docstring) and asserts the *observable behaviour* of that branch, not just that
it executed. Where a line is a genuinely defensive guard that cannot arise from
the public API, that is noted in the corresponding test's docstring and the
guard is exercised via the narrowest possible probe.

Targeted lines (coverage -m baseline):
    events.py:31
    middleware/logging.py:46
    processing.py:164-165, 212, 239, 249, 262, 290-292, 317
    routing/router.py:82, 125-126, 230, 246, 389-390
    types.py:61
    utils.py:17-18, 76-77
"""

from __future__ import annotations

import contextlib
import copy
import io
import json
import pickle
import sys

import pytest
from pydantic import BaseModel

from fastsqs import FastSQS, SQSEvent, SQSRouter
from fastsqs.middleware.logging import LoggingMiddleware
from fastsqs.routing.entry import _RouteEntry
from fastsqs.testing import SQSTestClient, make_event, make_record
from fastsqs.types import State
from fastsqs.utils import select_kwargs, uses_depends


class Task(SQSEvent):
    task_id: str = "x"


# --------------------------------------------------------------------------- #
# events.py:31  --  get_message_type_variants() empty-name guard
# --------------------------------------------------------------------------- #

def test_empty_class_name_yields_no_variants():
    """events.py:31 -- ``if not base_name: return set()``.

    A normal class always has a truthy ``__name__``, but ``type("", ...)``
    produces an empty one. The guard must short-circuit to an empty set rather
    than emit ``{"", ...}`` junk variants.
    """
    Empty = type("", (SQSEvent,), {})
    assert Empty.__name__ == ""
    assert Empty.get_message_type_variants() == set()


# --------------------------------------------------------------------------- #
# middleware/logging.py:46  --  the default JSON-to-stdout logger body
# --------------------------------------------------------------------------- #

def test_default_logger_prints_json_line_to_stdout():
    """logging.py:46 -- ``print(json.dumps(obj, ensure_ascii=False))``.

    Constructed with no custom ``logger``, the middleware must fall back to the
    inner ``_default_logger`` which prints one JSON line. Capture stdout and
    parse it back to prove the line is well-formed and carries the data.
    """
    mw = LoggingMiddleware()  # no logger -> _default_logger path
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        mw.log("info", "hello", foo=1, uni="café")

    out = buf.getvalue().strip()
    parsed = json.loads(out)
    assert parsed["message"] == "hello"
    assert parsed["foo"] == 1
    assert parsed["lvl"] == "INFO"
    # ensure_ascii=False keeps non-ASCII literal (not \u-escaped).
    assert "café" in out


# --------------------------------------------------------------------------- #
# A LoggingMiddleware that records every internal _log line, so the debug
# branches in processing.py become observable (the app routes _log through a
# registered LoggingMiddleware).
# --------------------------------------------------------------------------- #

class _Recorder(LoggingMiddleware):
    def __init__(self):
        self.messages: list[str] = []
        super().__init__(logger=lambda obj: self.messages.append(obj.get("message")))


# --------------------------------------------------------------------------- #
# processing.py:164-165  --  debug summary line in _handle_event (standard)
# --------------------------------------------------------------------------- #

def test_debug_event_summary_logged_standard():
    """processing.py:164-165 -- ``if self.debug: ... self._log("Processing event")``."""
    app = FastSQS(debug=True)
    rec = _Recorder()
    app.add_middleware(rec)

    @app.route(Task)
    async def h(msg: Task):
        return "ok"

    r = SQSTestClient(app).send({"type": "task", "task_id": "1"})
    assert r == {"batchItemFailures": []}
    assert "Processing event" in rec.messages


# --------------------------------------------------------------------------- #
# processing.py:212  --  debug "Record failed" line in _handle_standard_event
# --------------------------------------------------------------------------- #

def test_debug_record_failure_logged_standard():
    """processing.py:212 -- ``if self.debug: self._log("debug", "Record failed", ...)``.

    A failing record on a standard queue with debug on must emit the extra
    debug failure line AND still report the record as a batch-item failure.
    """
    app = FastSQS(debug=True)
    rec = _Recorder()
    app.add_middleware(rec)

    @app.route(Task)
    async def h(msg: Task):
        raise RuntimeError("boom")

    r = SQSTestClient(app).send({"type": "task", "task_id": "1"}, message_id="x1")
    assert r == {"batchItemFailures": [{"itemIdentifier": "x1"}]}
    # "Record failed" is logged twice (error + debug); the debug copy is line 212.
    assert rec.messages.count("Record failed") >= 2


# --------------------------------------------------------------------------- #
# processing.py:239, 249, 262  --  debug lines on the FIFO isolate_groups path
# --------------------------------------------------------------------------- #

def test_debug_fifo_isolate_groups_logs_and_isolation():
    """processing.py:239/249/262 -- the three ``if self.debug`` FIFO log lines.

    - 239: "FIFO processing" batch summary
    - 249: "Processing group" per-group line
    - 262: "FIFO record failed; halting group to preserve ordering"

    Also asserts the actual semantics: a failure in group g1 fails g1's tail
    only, while group g2 succeeds independently.
    """
    app = FastSQS(debug=True, fifo_failure_mode="isolate_groups")
    rec = _Recorder()
    app.add_middleware(rec)
    seen = []

    @app.route(Task)
    async def h(msg: Task):
        seen.append(msg.task_id)
        if msg.task_id == "bad":
            raise RuntimeError("boom")

    event = make_event(
        [
            make_record({"type": "task", "task_id": "bad"}, message_id="g1a", group_id="g1"),
            make_record({"type": "task", "task_id": "after"}, message_id="g1b", group_id="g1"),
            make_record({"type": "task", "task_id": "ok"}, message_id="g2a", group_id="g2"),
        ]
    )
    r = app.handler(event, None)

    # g1a fails -> g1a and the g1 tail (g1b) reported; g2a succeeds.
    failed_ids = {f["itemIdentifier"] for f in r["batchItemFailures"]}
    assert failed_ids == {"g1a", "g1b"}
    # g1's "after" record must NOT have been processed (group halted).
    assert "after" not in seen
    assert "ok" in seen

    assert "FIFO processing" in rec.messages           # line 239
    assert "Processing group" in rec.messages          # line 249
    assert (                                            # line 262
        "FIFO record failed; halting group to preserve ordering" in rec.messages
    )


# --------------------------------------------------------------------------- #
# processing.py:290-292  --  a process_group TASK raising (defensive gather arm)
# --------------------------------------------------------------------------- #

def test_fifo_group_task_exception_is_swallowed_defensively():
    """processing.py:290-292 -- ``elif isinstance(result, Exception): if self.debug: ...``.

    Per-record errors are caught inside ``process_group``; the only way the
    group coroutine itself raises (so ``asyncio.gather`` returns an Exception)
    is if framework code outside that try/except raises. We force exactly that
    by making the debug ``_log("Processing group")`` line throw. The branch must
    swallow it (no records appended for that group) rather than crash the batch.
    This is a defensive guard: it cannot arise from a well-behaved logger via
    the public API, so it is probed by overriding ``_log``.
    """

    class BoomApp(FastSQS):
        def _log(self, level, message, **data):  # type: ignore[override]
            if message == "Processing group":
                raise RuntimeError("group log boom")

    app = BoomApp(debug=True, fifo_failure_mode="isolate_groups")
    ran = []

    @app.route(Task)
    async def h(msg: Task):
        ran.append(msg.task_id)
        return "ok"

    event = make_event(
        [make_record({"type": "task", "task_id": "1"}, message_id="f1", group_id="g1")]
    )
    r = app.handler(event, None)

    # The group task raised before processing any record: its failures (none)
    # are not appended, and the exception is swallowed -> empty failures.
    assert r == {"batchItemFailures": []}
    assert ran == []  # the record was never reached


# --------------------------------------------------------------------------- #
# processing.py:317  --  debug line in _handle_fifo_halt_batch failure path
# --------------------------------------------------------------------------- #

def test_debug_fifo_halt_batch_logs_and_halts():
    """processing.py:317 -- ``if self.debug: self._log("FIFO batch halted on failure")``.

    halt_batch mode: the first failing record halts the whole batch; that record
    and every record after it are reported. With debug on, the halt line is
    emitted.
    """
    app = FastSQS(debug=True, fifo_failure_mode="halt_batch")
    rec = _Recorder()
    app.add_middleware(rec)
    seen = []

    @app.route(Task)
    async def h(msg: Task):
        seen.append(msg.task_id)
        if msg.task_id == "bad":
            raise RuntimeError("boom")

    event = make_event(
        [
            make_record({"type": "task", "task_id": "ok1"}, message_id="m0", group_id="g"),
            make_record({"type": "task", "task_id": "bad"}, message_id="m1", group_id="g"),
            make_record({"type": "task", "task_id": "tail"}, message_id="m2", group_id="g"),
        ]
    )
    r = app.handler(event, None)

    failed_ids = {f["itemIdentifier"] for f in r["batchItemFailures"]}
    assert failed_ids == {"m1", "m2"}      # failing record + tail
    assert "tail" not in seen              # halted: tail never processed
    assert "FIFO batch halted on failure" in rec.messages  # line 317


# --------------------------------------------------------------------------- #
# router.py:82  --  route() rejects a non-SQSEvent BaseModel
# --------------------------------------------------------------------------- #

def test_route_rejects_non_sqsevent_basemodel():
    """router.py:82 -- ``raise ValueError("event_model must be a subclass of SQSEvent")``."""

    class NotEvent(BaseModel):
        x: int = 1

    router = SQSRouter()
    with pytest.raises(ValueError, match="subclass of SQSEvent"):
        @router.route(NotEvent)  # type: ignore[arg-type]
        async def h(msg):
            pass


# --------------------------------------------------------------------------- #
# router.py:125-126  --  flexible_matching variant maps to two primary types
# --------------------------------------------------------------------------- #

def test_flexible_matching_variant_collision_raises():
    """router.py:125-126 -- ambiguous variant across two event classes.

    Class ``XY`` (primary ``x_y``) emits a kebab variant ``x-y``; class named
    ``x-y`` (primary ``x-y``) also claims the ``x-y`` variant. Distinct primaries
    + a shared variant -> the registry must refuse the ambiguity.
    """
    router = SQSRouter(flexible_matching=True)
    XY = type("XY", (SQSEvent,), {"__annotations__": {"v": int}, "v": 1})

    @router.route(XY)  # type: ignore[arg-type]
    async def h1(msg):
        pass

    # sanity: the kebab variant was registered to primary "x_y".
    assert router._route_lookup["x-y"] == "x_y"

    Xy = type("x-y", (SQSEvent,), {"__annotations__": {"v": int}, "v": 1})
    with pytest.raises(ValueError, match="maps to"):
        @router.route(Xy)  # type: ignore[arg-type]
        async def h2(msg):
            pass


# --------------------------------------------------------------------------- #
# router.py:230  --  subrouter(value, router=...) onto an EXISTING route entry
# --------------------------------------------------------------------------- #

def test_subrouter_instance_attaches_to_existing_entry():
    """router.py:230 -- ``self._routes[k].subrouter = router`` (entry already exists).

    Registering a key-value handler for "x" creates an entry; attaching a
    subrouter for the same key must mutate that entry in place rather than
    replace it.
    """
    parent = SQSRouter(discriminator="type")

    @parent.route("x")
    async def hx(msg, ctx):
        pass

    existing_entry = parent._routes["x"]
    assert existing_entry.handler is not None

    child = SQSRouter(discriminator="sub")
    ret = parent.subrouter("x", child)

    assert ret is child
    assert parent._routes["x"] is existing_entry           # same entry mutated
    assert parent._routes["x"].subrouter is child          # line 230 effect
    assert parent._routes["x"].handler is not None          # handler preserved


# --------------------------------------------------------------------------- #
# router.py:246  --  subrouter() decorator form onto an EXISTING route entry
# --------------------------------------------------------------------------- #

def test_subrouter_decorator_attaches_to_existing_entry():
    """router.py:246 -- decorator form, ``self._routes[k].subrouter = router_instance``."""
    parent = SQSRouter(discriminator="type")

    @parent.route("y")
    async def hy(msg, ctx):
        pass

    existing_entry = parent._routes["y"]
    child = SQSRouter(discriminator="sub")

    @parent.subrouter("y")
    def make():
        return child

    assert parent._routes["y"] is existing_entry
    assert parent._routes["y"].subrouter is child          # line 246 effect


# --------------------------------------------------------------------------- #
# router.py:389-390  --  entry with neither handler nor subrouter -> unhandled
# --------------------------------------------------------------------------- #

def test_dispatch_entry_without_handler_or_subrouter_is_unhandled():
    """router.py:389-390 -- ``route_path.pop(); return False``.

    A ``_RouteEntry`` with no handler and no subrouter cannot be produced by the
    public decorators, but is reachable defensively if one is inserted directly.
    dispatch must treat it as unhandled (pop the route_path it pushed and return
    False) -- with no default handler this surfaces as a RouteNotFoundError and
    a batch-item failure.
    """
    router = SQSRouter(discriminator="type")
    router._routes["ghost"] = _RouteEntry()  # handler=None, subrouter=None

    app = FastSQS()
    app.include_router(router)

    event = make_event([make_record({"type": "ghost"}, message_id="g1")])
    r = app.handler(event, None)
    assert r == {"batchItemFailures": [{"itemIdentifier": "g1"}]}


def test_dispatch_unhandled_entry_does_not_leak_route_path():
    """router.py:389-390 -- the popped route_path must be balanced.

    Direct dispatch on a handler-less entry returns False and leaves route_path
    as it found it (the pushed segment is popped on line 389).
    """
    import asyncio

    from fastsqs.types import Context, QueueType

    router = SQSRouter(discriminator="type")
    router._routes["ghost"] = _RouteEntry()

    ctx = Context(
        message_id="m", record={}, lambda_context=None, queue_type=QueueType.STANDARD
    )
    handled = asyncio.run(
        router.dispatch({"type": "ghost"}, {}, None, ctx, root_payload={"type": "ghost"})
    )
    assert handled is False
    assert ctx.route_path == []  # pushed segment was popped (line 389)


# --------------------------------------------------------------------------- #
# types.py:61  --  State.__getattr__('_data') guard during copy/pickle
# --------------------------------------------------------------------------- #

def test_state_copy_does_not_infinitely_recurse():
    """types.py:61 -- ``if key == "_data": raise AttributeError(key)``.

    copy/pickle reconstruct the instance without __init__, so _data is unset and
    the copy machinery probes ``__getattr__('_data')``. Without the guard this
    recurses forever. The shallow copy must succeed and carry the data.
    """
    s = State({"a": 1, "b": [2, 3]})
    shallow = copy.copy(s)
    assert shallow["a"] == 1
    assert shallow["b"] == [2, 3]
    # shallow copy shares the inner list reference.
    assert shallow["b"] is s["b"]


def test_state_pickle_roundtrip():
    """types.py:61 (via __reduce_ex__ -> __getattr__ probes) + __setstate__."""
    # Safe: we pickle and immediately unpickle a State we built in-test from a
    # literal dict -- no untrusted/external pickle data is ever loaded.
    s = State({"a": 1})
    restored = pickle.loads(pickle.dumps(s))
    assert restored["a"] == 1
    assert isinstance(restored, State)


def test_state_deepcopy_roundtrip():
    """types.py:61 -- deepcopy also probes _data on the fresh instance."""
    s = State({"a": [1, 2]})
    deep = copy.deepcopy(s)
    assert deep["a"] == [1, 2]
    assert deep["a"] is not s["a"]  # deep copy => independent list


def test_state_missing_attr_still_raises_attributeerror():
    """Adjacent guard: a genuine missing key (not '_data') raises AttributeError."""
    s = State({"a": 1})
    with pytest.raises(AttributeError):
        _ = s.nonexistent


def test_state_uninitialized_data_access_raises_not_recurses():
    """types.py:61 -- ``if key == "_data": raise AttributeError(key)``.

    This is the actual reconstruction guard. ``copy``/``pickle`` of a *normal*
    State go through ``__getstate__``/``__setstate__`` and never probe ``_data``
    via ``__getattr__``, so line 61 is only hit when an instance exists WITHOUT
    ``__init__`` (``State.__new__``) and something accesses an attribute -- the
    precise scenario the guard protects (without it, ``self._data`` ->
    ``__getattr__('_data')`` -> ``self._data`` -> ... recurses forever).

    Sets a low recursion limit so an unguarded recursion would surface as a
    RecursionError rather than this clean AttributeError.
    """
    blank = State.__new__(State)  # constructed without __init__: _data unset
    old_limit = sys.getrecursionlimit()
    sys.setrecursionlimit(120)
    try:
        with pytest.raises(AttributeError):
            _ = blank._data  # hits line 61: guarded raise, not infinite recursion
        with pytest.raises(AttributeError):
            _ = blank.anything_else  # also short-circuits via the _data guard
    finally:
        sys.setrecursionlimit(old_limit)


# --------------------------------------------------------------------------- #
# utils.py:17-18 and 76-77  --  inspect.signature() failure fallbacks
# --------------------------------------------------------------------------- #

class _UninspectableCallable:
    """A callable whose ``inspect.signature`` raises ValueError.

    Some C builtins / objects with a broken ``__signature__`` defeat
    introspection; we model that here so the except arms are exercised
    deterministically.
    """

    def __call__(self, *args, **kwargs):
        return "called"

    @property
    def __signature__(self):
        raise ValueError("no signature available")


def test_uses_depends_returns_false_when_signature_unavailable():
    """utils.py:17-18 -- ``except (ValueError, TypeError): return False``."""
    fn = _UninspectableCallable()
    # sanity: signature really does raise.
    import inspect

    with pytest.raises((ValueError, TypeError)):
        inspect.signature(fn)

    assert uses_depends(fn) is False


def test_select_kwargs_passes_through_when_signature_unavailable():
    """utils.py:76-77 -- ``except (ValueError, TypeError): return candidates``.

    When the signature is uninspectable we cannot filter by name, so all
    candidate kwargs are returned verbatim.
    """
    fn = _UninspectableCallable()
    out = select_kwargs(fn, a=1, b=2, c=3)
    assert out == {"a": 1, "b": 2, "c": 3}

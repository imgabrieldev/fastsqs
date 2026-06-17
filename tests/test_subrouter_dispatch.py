"""SQSRouter.subrouter nested dispatch: payload_scope, inherit_middlewares, route_path.

These pin the public ``subrouter`` surface (instance arg, decorator over a
pre-built router, decorator over a zero-arg factory) and the nested branches of
``SQSRouter.dispatch``: combined-middleware accumulation
(``parent_middlewares + self._middlewares + entry.middlewares``), the
``route_path`` push on each discriminator hop and pop on an unhandled return,
and the handled-vs-unhandled fall-through between a parent router and its
subrouter.

Built with two key-value ``SQSRouter`` s wired through ``app.include_router`` and
driven by ``SQSTestClient`` — no AWS, no Docker.

Note on a corrected case (see ``test_nested_unmatched_*`` below): when the
parent matches a discriminator value to a *subrouter* and that subrouter
declines (returns False), ``dispatch`` pops the parent's hop and returns False
immediately (router.py: the ``entry.is_nested`` branch does ``route_path.pop();
return False``). It does NOT fall through to the parent router's own ``default``
handler — the default is only consulted on the ``entry is None`` / missing-
discriminator paths. So a nested miss is genuinely unhandled and the record
fails; the assertions reflect that true behavior rather than the wishful
"parent default runs" wording in the original brief.
"""

import pytest

from fastsqs import FastSQS, SQSRouter
from fastsqs.middleware import Middleware
from fastsqs.testing import SQSTestClient


# ---------------------------------------------------------------------------
# subrouter() registration surfaces
# ---------------------------------------------------------------------------

def test_subrouter_registered_via_instance_arg():
    seen = []

    parent = SQSRouter(discriminator="type")
    child = SQSRouter(discriminator="action")

    @child.route("ping")
    async def ping(msg, ctx):
        seen.append("ping")

    returned = parent.subrouter("task", child)
    assert returned is child  # instance-arg form returns the same child

    app = FastSQS()
    app.include_router(parent)

    result = SQSTestClient(app).send({"type": "task", "action": "ping"})

    assert seen == ["ping"]
    assert result == {"batchItemFailures": []}


def test_subrouter_registered_via_decorator_returns_router():
    seen = []

    parent = SQSRouter(discriminator="type")
    child = SQSRouter(discriminator="action")

    @child.route("ping")
    async def ping(msg, ctx):
        seen.append("ping")

    # Decorator form applied directly to a pre-built SQSRouter instance.
    decorator = parent.subrouter("task")
    returned = decorator(child)
    assert returned is child  # decorator hands back the same instance

    app = FastSQS()
    app.include_router(parent)

    result = SQSTestClient(app).send({"type": "task", "action": "ping"})

    assert seen == ["ping"]
    assert result == {"batchItemFailures": []}


def test_subrouter_decorator_accepts_factory_callable():
    seen = []

    parent = SQSRouter(discriminator="type")

    def build_child():
        child = SQSRouter(discriminator="action")

        @child.route("ping")
        async def ping(msg, ctx):
            seen.append("ping")

        return child

    # The decorator detects a non-SQSRouter callable and invokes it, registering
    # (and returning) the SQSRouter the factory produced.
    returned = parent.subrouter("task")(build_child)
    assert isinstance(returned, SQSRouter)

    app = FastSQS()
    app.include_router(parent)

    result = SQSTestClient(app).send({"type": "task", "action": "ping"})

    assert seen == ["ping"]
    assert result == {"batchItemFailures": []}


# ---------------------------------------------------------------------------
# nested discriminator dispatch
# ---------------------------------------------------------------------------

def test_subrouter_dispatches_nested_discriminator():
    seen = []

    parent = SQSRouter(discriminator="type")
    child = SQSRouter(discriminator="action")

    @child.route("run")
    async def run(msg, ctx):
        seen.append("run")

    @child.route("stop")
    async def stop(msg, ctx):
        seen.append("stop")

    parent.subrouter("task", child)

    app = FastSQS()
    app.include_router(parent)
    client = SQSTestClient(app)

    r1 = client.send({"type": "task", "action": "run"})
    assert seen == ["run"]
    assert r1 == {"batchItemFailures": []}

    r2 = client.send({"type": "task", "action": "stop"})
    assert seen == ["run", "stop"]
    assert r2 == {"batchItemFailures": []}


# ---------------------------------------------------------------------------
# payload_scope: which dict reaches the handler's `payload` kwarg
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("scope", ["current", "root", "both"])
def test_payload_scope_passes_full_message_to_handler(scope):
    """In a nested key-value dispatch the framework threads the SAME payload dict
    all the way down (parent.dispatch hands its own ``payload`` to the
    subrouter), so ``payload`` and ``root_payload`` are the same object. For
    'current' ``_execute_handler`` uses ``payload``; for 'root'/'both' it uses
    ``root_payload`` — all three therefore deliver the full original message."""
    captured = {}

    parent = SQSRouter(discriminator="type")
    child = SQSRouter(discriminator="action", payload_scope=scope)

    @child.route("run")
    async def run(payload, ctx):
        captured["payload"] = payload

    parent.subrouter("task", child)

    app = FastSQS()
    app.include_router(parent)

    message = {"type": "task", "action": "run"}
    result = SQSTestClient(app).send(message)

    assert result == {"batchItemFailures": []}
    assert captured["payload"] == message


def test_payload_scope_current_passes_subpayload_to_handler():
    captured = {}

    parent = SQSRouter(discriminator="type")
    child = SQSRouter(discriminator="action", payload_scope="current")

    @child.route("run")
    async def run(payload, ctx):
        captured["payload"] = payload

    parent.subrouter("task", child)

    app = FastSQS()
    app.include_router(parent)

    message = {"type": "task", "action": "run"}
    result = SQSTestClient(app).send(message)

    assert result == {"batchItemFailures": []}
    # 'current' uses the `payload` param (the message dict that reached the
    # subrouter); in this path it is the full original message.
    assert captured["payload"] == message


def test_payload_scope_root_passes_root_payload():
    captured = {}

    parent = SQSRouter(discriminator="type")
    child = SQSRouter(discriminator="action", payload_scope="root")  # default

    @child.route("run")
    async def run(payload, ctx):
        captured["payload"] = payload

    parent.subrouter("task", child)

    app = FastSQS()
    app.include_router(parent)

    message = {"type": "task", "action": "run"}
    result = SQSTestClient(app).send(message)

    assert result == {"batchItemFailures": []}
    # 'root' uses root_payload, the original top-level message threaded from
    # _handle_record (payload == root_payload here).
    assert captured["payload"] == message


def test_payload_scope_both_passes_root_payload():
    captured = {}

    parent = SQSRouter(discriminator="type")
    child = SQSRouter(discriminator="action", payload_scope="both")

    @child.route("run")
    async def run(payload, ctx):
        captured["payload"] = payload

    parent.subrouter("task", child)

    app = FastSQS()
    app.include_router(parent)

    message = {"type": "task", "action": "run"}
    result = SQSTestClient(app).send(message)

    assert result == {"batchItemFailures": []}
    # The 'both' branch also resolves to root_payload per source.
    assert captured["payload"] == message


def test_payload_scope_invalid_value_raises_valueerror():
    with pytest.raises(ValueError) as exc_info:
        SQSRouter(payload_scope="bogus")
    message = str(exc_info.value)
    assert "current" in message or "root" in message or "both" in message


# ---------------------------------------------------------------------------
# inherit_middlewares: parent-router middlewares in the child's chain
# ---------------------------------------------------------------------------

class _RecordingMiddleware(Middleware):
    """Appends ``name`` into a shared list in ``before`` (and again in ``after``
    if ``record_after`` is set)."""

    def __init__(self, name, order):
        self.name = name
        self.order = order

    async def before(self, payload, record, context, ctx):
        self.order.append(self.name)


def test_inherit_middlewares_true_runs_parent_then_child():
    order = []

    # NOTE (corrected): the dispatch code gates on ``self.inherit_middlewares``
    # of the router that OWNS the subrouter (the parent), deciding whether to
    # thread ``parent_middlewares + self._middlewares`` into the child. So the
    # flag that matters at this hop lives on the PARENT router, not the child.
    parent = SQSRouter(discriminator="type", inherit_middlewares=True)
    parent.add_middleware(_RecordingMiddleware("parent", order))

    child = SQSRouter(discriminator="action")
    child.add_middleware(_RecordingMiddleware("child", order))

    @child.route("run")
    async def run(msg, ctx):
        order.append("handler")

    parent.subrouter("task", child)

    app = FastSQS()
    app.include_router(parent)

    result = SQSTestClient(app).send({"type": "task", "action": "run"})

    assert result == {"batchItemFailures": []}
    # combined = parent_middlewares + self._middlewares + entry.middlewares,
    # so the parent router's middleware before() runs before the child's.
    assert order == ["parent", "child", "handler"]


def test_inherit_middlewares_false_skips_parent_middlewares():
    order = []

    # Corrected per real v1 behavior: setting inherit_middlewares=False on the
    # PARENT router (the subrouter owner) makes its dispatch use
    # ``combined_mws = entry.middlewares`` (empty here), so neither the parent
    # router's own middleware nor any ancestor's propagates into the child.
    parent = SQSRouter(discriminator="type", inherit_middlewares=False)
    parent.add_middleware(_RecordingMiddleware("parent", order))

    child = SQSRouter(discriminator="action")
    child.add_middleware(_RecordingMiddleware("child", order))

    @child.route("run")
    async def run(msg, ctx):
        order.append("handler")

    parent.subrouter("task", child)

    app = FastSQS()
    app.include_router(parent)

    result = SQSTestClient(app).send({"type": "task", "action": "run"})

    assert result == {"batchItemFailures": []}
    # inherit_middlewares=False -> combined_mws = entry.middlewares (empty here),
    # so the parent router middleware never runs for the child handler.
    assert "parent" not in order
    assert order == ["child", "handler"]


# ---------------------------------------------------------------------------
# route_path push/pop and handled-vs-unhandled fall-through
# ---------------------------------------------------------------------------

class _RoutePathCapture(Middleware):
    """App-level middleware that snapshots ctx.route_path and which handlers ran
    via ctx.state, so the test can observe them after dispatch unwinds."""

    def __init__(self, sink):
        self.sink = sink

    async def after(self, payload, record, context, ctx, error):
        self.sink["route_path_after"] = list(ctx.route_path)
        self.sink["error"] = error


def test_nested_unmatched_falls_through_and_pops_route_path():
    """Parent subrouter on 'task' -> child on 'action' with NO matching action
    route and NO child default; the parent has a default handler.

    Corrected behavior: the matched-but-declined subrouter path returns False
    after popping the parent hop; the parent's own default is NOT consulted, so
    the record is unhandled and fails. The key invariant the original case cared
    about still holds: ctx.route_path is fully unwound and never retains the
    dangling 'action=unknown' hop the child pushed and popped."""
    seen = []
    sink = {}

    parent = SQSRouter(discriminator="type")

    @parent.default()
    async def parent_default(msg, ctx):
        seen.append("parent_default")

    child = SQSRouter(discriminator="action")  # no routes, no default

    parent.subrouter("task", child)

    app = FastSQS()
    app.add_middleware(_RoutePathCapture(sink))
    app.include_router(parent)

    result = SQSTestClient(app).send(
        {"type": "task", "action": "unknown"}, message_id="u1"
    )

    # The subrouter declined; the parent default did NOT run for the nested miss.
    assert seen == []
    # Genuinely unhandled -> the record fails (RouteNotFoundError under the hood).
    assert result == {"batchItemFailures": [{"itemIdentifier": "u1"}]}
    # route_path is fully unwound: no dangling child hop (and no parent hop).
    assert "action=unknown" not in sink["route_path_after"]
    assert sink["route_path_after"] == []


def test_parent_default_runs_when_discriminator_unmatched_at_parent():
    """Adjacent positive case: when the parent's OWN discriminator value has no
    matching route (and no subrouter), the parent default handler runs.

    Corrected detail: in the ``entry is None`` branch the hop is pushed and the
    default runs WITHOUT popping it first (the ``route_path.pop()`` lives only on
    the no-default ``return False`` path). So the 'type=missing' hop is still
    present when the default executes — which is exactly why the genuinely
    unhandled subrouter case above must rely on the subrouter branch's own pop."""
    seen = []
    sink = {}

    parent = SQSRouter(discriminator="type")

    @parent.route("known")
    async def known(msg, ctx):
        seen.append("known")

    @parent.default()
    async def parent_default(msg, ctx):
        seen.append("parent_default")

    app = FastSQS()
    app.add_middleware(_RoutePathCapture(sink))
    app.include_router(parent)

    result = SQSTestClient(app).send({"type": "missing"})

    assert seen == ["parent_default"]
    assert result == {"batchItemFailures": []}
    # The 'type=missing' hop is NOT popped on the default path; it remains.
    assert sink["route_path_after"] == ["type=missing"]


def test_route_path_records_each_discriminator_hop():
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
    # Each dispatch level appended its own discriminator hop, in order.
    assert captured["route_path"] == ["type=task", "action=run"]

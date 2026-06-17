"""Key-value routing breadth: SQSRouter.route value/model/None/duplicate paths.

These exercise the key-value (discriminator) routing surface of SQSRouter
that the existing routing tests only happy-path: multiple/iterable values,
int->str coercion, per-route model= validation (success + failure),
duplicate-key detection, the value=None -> default() delegation, and a value
handler attaching to a pre-existing bare-subrouter slot.

Direct SQSRouter construction + FastSQS.include_router + SQSTestClient; no AWS.
"""

import pytest

from fastsqs import FastSQS, SQSRouter, SQSEvent
from fastsqs.middleware import Middleware
from fastsqs.testing import SQSTestClient


class PingModel(SQSEvent):
    amount: int


# ---- multiple / iterable values for one handler ----

def test_route_registers_multiple_values_for_one_handler():
    router = SQSRouter(discriminator="action")
    seen = []

    @router.route(["ping", "pong"])
    async def handle(msg, ctx):
        seen.append(ctx.route_path[-1])

    app = FastSQS()
    app.include_router(router)
    client = SQSTestClient(app)

    r1 = client.send({"action": "ping"})
    r2 = client.send({"action": "pong"})

    assert seen == ["action=ping", "action=pong"]
    assert r1 == {"batchItemFailures": []}
    assert r2 == {"batchItemFailures": []}


def test_route_iterable_of_values_non_list():
    # A tuple is a non-str/int Iterable, so route() takes the ``list(value)`` path.
    router = SQSRouter(discriminator="action")
    seen = []

    @router.route(("a", "b"))
    async def handle(msg, ctx):
        seen.append(ctx.route_path[-1])

    app = FastSQS()
    app.include_router(router)
    client = SQSTestClient(app)

    assert client.send({"action": "a"}) == {"batchItemFailures": []}
    assert client.send({"action": "b"}) == {"batchItemFailures": []}
    assert seen == ["action=a", "action=b"]


# ---- int value coerced to str on both registration and dispatch ----

def test_route_with_int_value_coerced_to_str():
    router = SQSRouter(discriminator="action")
    seen = []

    @router.route(1)
    async def handle(msg, ctx):
        seen.append(ctx.route_path[-1])

    # registration key is str(1) == "1"
    assert "1" in router._routes

    app = FastSQS()
    app.include_router(router)

    # dispatch with an int discriminator value: str(1) == "1" matches the route
    r = SQSTestClient(app).send({"action": 1})
    assert r == {"batchItemFailures": []}
    assert seen == ["action=1"]


# ---- per-route model= validation (success) ----

def test_key_value_route_with_model_validates_and_binds():
    router = SQSRouter(discriminator="action")
    captured = {}

    @router.route("ping", model=PingModel)
    async def handle(msg):
        captured["msg"] = msg

    app = FastSQS()
    app.include_router(router)

    r = SQSTestClient(app).send({"action": "ping", "amount": 7})

    assert r == {"batchItemFailures": []}
    assert isinstance(captured["msg"], PingModel)
    assert captured["msg"].amount == 7


# ---- per-route model= validation (failure -> InvalidMessageError fails the record) ----

def test_key_value_route_with_model_validation_failure_is_invalid_message():
    router = SQSRouter(discriminator="action")
    ran = []

    @router.route("ping", model=PingModel)
    async def handle(msg):
        ran.append(msg)  # should never run: validation fails first

    app = FastSQS()
    app.include_router(router)

    # missing required ``amount`` -> model_validate raises ValidationError,
    # wrapped as InvalidMessageError, failing only this record.
    r = SQSTestClient(app).send({"action": "ping"}, message_id="bad-1")

    assert r == {"batchItemFailures": [{"itemIdentifier": "bad-1"}]}
    assert ran == []


# ---- duplicate key-value route ----

def test_duplicate_key_value_route_raises():
    router = SQSRouter(discriminator="action")

    @router.route("ping")
    async def first(msg):
        pass

    with pytest.raises(ValueError) as excinfo:
        @router.route("ping")
        async def second(msg):
            pass

    msg = str(excinfo.value)
    assert "Duplicate handler for" in msg
    assert "action=ping" in msg


# ---- value handler attaching to a pre-existing bare-subrouter slot ----

def test_route_value_handler_attaches_to_preexisting_subrouter_entry():
    router = SQSRouter(discriminator="action")
    child = SQSRouter(discriminator="sub")

    # creates a _RouteEntry with subrouter set and handler None
    router.subrouter("task", child)
    entry = router._routes["task"]
    assert entry.subrouter is child
    assert entry.handler is None

    # route() over the same value hits the ``existing.handler is None`` branch:
    # it updates the entry in place rather than raising on a duplicate key.
    @router.route("task")
    async def handle(msg):
        pass

    # no ValueError raised; same entry now carries a handler
    assert router._routes["task"] is entry
    assert entry.handler is not None
    assert entry.subrouter is child


# ---- value=None delegates to default() ----

def test_route_none_registers_default_handler():
    router = SQSRouter(discriminator="action")
    seen = []

    @router.route(None)
    async def fallback(msg, ctx):
        seen.append("default")

    # registered as the router default, not as a keyed route
    assert router._default_handler is not None
    assert router._routes == {}

    app = FastSQS()
    app.include_router(router)

    # discriminator present but matching no keyed route -> default catches it
    r = SQSTestClient(app).send({"action": "unmatched"})

    assert r == {"batchItemFailures": []}
    assert seen == ["default"]


def test_route_none_with_middlewares_registers_default():
    # The None path forwards to default(middlewares=...); the kwarg is accepted
    # (advisory) and the default handler still runs for unmatched messages.
    class NoopMW(Middleware):
        async def before(self, payload, record, context, ctx):
            pass

        async def after(self, payload, record, context, ctx, error):
            pass

    router = SQSRouter(discriminator="action")
    seen = []

    @router.route(None, middlewares=[NoopMW()])
    async def fallback(msg, ctx):
        seen.append("default")

    assert router._default_handler is not None

    app = FastSQS()
    app.include_router(router)

    r = SQSTestClient(app).send({"action": "nope"})

    assert r == {"batchItemFailures": []}
    assert seen == ["default"]

"""Multi-router dispatch: include_router ordering, fallthrough, RouteNotFoundError
message, and app-level custom discriminators.

``_handle_record`` tries the main router first, then each included router in
registration order, stopping at the first one whose ``dispatch`` returns True.
A router's ``dispatch`` returns False (declines) when the discriminator value is
missing/None with no default, or when no route matches and there is no default;
a registered ``default`` handler makes it return True. These tests pin that
ordering, the fallthrough across routers, the precedence of the main router, and
the exact text of ``RouteNotFoundError``.
"""

import asyncio

import pytest

from fastsqs import FastSQS, SQSEvent, SQSRouter
from fastsqs.exceptions import RouteNotFoundError
from fastsqs.testing import SQSTestClient


class OrderCreated(SQSEvent):
    order_id: str


# ---- fallthrough across included routers ----

def test_second_included_router_handles_when_first_declines():
    seen = []

    r1 = SQSRouter(discriminator="type")

    @r1.route("a")
    async def handle_a(msg, ctx):
        seen.append("r1")

    r2 = SQSRouter(discriminator="type")

    @r2.route("b")
    async def handle_b(msg, ctx):
        seen.append("r2")

    app = FastSQS()
    app.include_router(r1)
    app.include_router(r2)

    result = SQSTestClient(app).send({"type": "b"})

    assert seen == ["r2"]
    assert result == {"batchItemFailures": []}


def test_main_router_takes_precedence_over_included_router():
    seen = []

    app = FastSQS()

    # Main router pydantic route whose message_type is 'task'.
    class Task(SQSEvent):
        task_id: str

    assert Task.get_message_type() == "task"

    @app.route(Task)
    async def main_handler(msg: Task):
        seen.append("main")

    # Included router also registers key-value 'task' to a different handler.
    included = SQSRouter(discriminator="type")

    @included.route("task")
    async def included_handler(msg, ctx):
        seen.append("included")

    app.include_router(included)

    result = SQSTestClient(app).send({"type": "task", "task_id": "1"})

    assert seen == ["main"]  # main tried first, returned True; included never ran
    assert result == {"batchItemFailures": []}


def test_included_router_default_handler_catches_unmatched():
    seen = []

    app = FastSQS()

    # Main router has one pydantic route that will NOT match the message.
    @app.route(OrderCreated)
    async def main_handler(msg: OrderCreated):
        seen.append("main")

    # Included router has a default handler.
    included = SQSRouter(discriminator="type")

    @included.default()
    async def fallback(msg, ctx):
        seen.append("default")

    app.include_router(included)

    # Message matches neither the named pydantic route nor any named key-value
    # route; the included router's default catches it (dispatch returns True).
    result = SQSTestClient(app).send({"type": "unmatched"})

    assert seen == ["default"]
    assert result == {"batchItemFailures": []}


# ---- RouteNotFoundError text ----

def test_route_not_found_error_lists_available_routes_and_discriminators():
    app = FastSQS()

    @app.route(OrderCreated)
    async def main_handler(msg: OrderCreated):
        pass

    # Included router with a distinct discriminator and NO default.
    included = SQSRouter(discriminator="action")

    @included.route("ship")
    async def ship(msg, ctx):
        pass

    app.include_router(included)

    # Through the public handler: an unmatched message fails its record.
    result = SQSTestClient(app).send({"type": "nope"}, message_id="z")
    assert result == {"batchItemFailures": [{"itemIdentifier": "z"}]}

    # Directly: _handle_record raises RouteNotFoundError with a descriptive text.
    record = {"messageId": "z", "body": '{"type": "nope"}'}

    with pytest.raises(RouteNotFoundError) as exc_info:
        asyncio.run(app._handle_record(record, None))

    message = str(exc_info.value)
    assert "order_created" in message          # available main-router routes
    assert "action" in message                 # available router discriminators
    assert "nope" in message                   # the unmatched discriminator value


# ---- independent routing by distinct discriminators ----

def test_two_routers_different_discriminators_route_independently():
    seen = []

    r1 = SQSRouter(discriminator="type")

    @r1.route("x")
    async def handle_x(msg, ctx):
        seen.append("x")

    r2 = SQSRouter(discriminator="action")

    @r2.route("y")
    async def handle_y(msg, ctx):
        seen.append("y")

    app = FastSQS()
    app.include_router(r1)
    app.include_router(r2)

    client = SQSTestClient(app)

    # {'type': 'x'} hits r1 only.
    res1 = client.send({"type": "x"})
    assert seen == ["x"]
    assert res1 == {"batchItemFailures": []}

    # {'action': 'y'} has no 'type' key: r1 finds no discriminator value and
    # declines (dispatch returns False), so r2 handles it.
    res2 = client.send({"action": "y"})
    assert seen == ["x", "y"]
    assert res2 == {"batchItemFailures": []}


# ---- app-level custom discriminator ----

def test_app_custom_discriminator_routes_pydantic_model():
    app = FastSQS(discriminator="kind")
    got = {}

    @app.route(OrderCreated)  # message_type == 'order_created'
    async def handle(msg: OrderCreated):
        got["order_id"] = msg.order_id

    result = SQSTestClient(app).send({"kind": "order_created", "order_id": "1"})

    assert got["order_id"] == "1"  # custom discriminator drives pydantic routing
    assert result == {"batchItemFailures": []}


def test_app_custom_discriminator_default_handler_on_missing_key():
    app = FastSQS(discriminator="kind")
    seen = []

    @app.route(OrderCreated)
    async def handle(msg: OrderCreated):
        seen.append("order")

    @app.default()
    async def fallback(msg, ctx):
        seen.append("default")

    # No 'kind' key -> discriminator value is None -> default path on main router.
    result = SQSTestClient(app).send({"order_id": "1"})

    assert seen == ["default"]
    assert result == {"batchItemFailures": []}

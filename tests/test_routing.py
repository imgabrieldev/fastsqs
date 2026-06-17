from fastsqs import FastSQS, SQSEvent
from fastsqs.testing import SQSTestClient


class OrderCreated(SQSEvent):
    order_id: str


class UserRegistered(SQSEvent):
    user_id: str


def test_routes_by_type_to_handler():
    seen = []
    app = FastSQS()

    @app.route(OrderCreated)
    async def handle(msg: OrderCreated):
        seen.append(msg.order_id)

    result = SQSTestClient(app).send({"type": "order_created", "order_id": "A1"})

    assert seen == ["A1"]
    assert result == {"batchItemFailures": []}


def test_flexible_matching_accepts_camel_case_type():
    seen = []
    app = FastSQS(flexible_matching=True)  # opt-in in v1

    @app.route(OrderCreated)
    async def handle(msg: OrderCreated):
        seen.append(msg.order_id)

    SQSTestClient(app).send({"type": "orderCreated", "order_id": "A2"})

    assert seen == ["A2"]


def test_flexible_matching_off_by_default():
    # v1 default: a camelCase type does NOT match the snake_case route.
    app = FastSQS()

    @app.route(OrderCreated)
    async def handle(msg: OrderCreated):
        pass

    r = SQSTestClient(app).send({"type": "orderCreated", "order_id": "A2"}, message_id="nx")
    assert r == {"batchItemFailures": [{"itemIdentifier": "nx"}]}


def test_routes_each_type_independently():
    seen = []
    app = FastSQS()

    @app.route(OrderCreated)
    async def handle_order(msg: OrderCreated):
        seen.append(("order", msg.order_id))

    @app.route(UserRegistered)
    async def handle_user(msg: UserRegistered):
        seen.append(("user", msg.user_id))

    client = SQSTestClient(app)
    client.send({"type": "order_created", "order_id": "A3"})
    client.send({"type": "user_registered", "user_id": "U9"})

    assert seen == [("order", "A3"), ("user", "U9")]


def test_default_handler_catches_unmatched():
    seen = []
    app = FastSQS()

    @app.route(OrderCreated)
    async def handle(msg: OrderCreated):
        seen.append(("order", msg.order_id))

    # default handler receives the parsed (base) SQSEvent; the raw message is
    # available via the `record` kwarg (selected by signature).
    @app.default()
    async def fallback(msg, record):
        seen.append(("default", type(msg).__name__, record["messageId"]))

    result = SQSTestClient(app).send({"type": "something_else", "x": 1}, message_id="m-7")

    assert seen == [("default", "SQSEvent", "m-7")]
    assert result == {"batchItemFailures": []}


def test_unmatched_without_default_fails_the_record():
    app = FastSQS()

    @app.route(OrderCreated)
    async def handle(msg: OrderCreated):
        pass

    result = SQSTestClient(app).send({"type": "nope"}, message_id="m-x")

    assert result == {"batchItemFailures": [{"itemIdentifier": "m-x"}]}

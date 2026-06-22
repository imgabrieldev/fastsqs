# Routers, subrouters, and defaults

Split routes across modules with `SQSRouter`, nest them with `subrouter(...)`, attach them to the app with `include_router(...)`, and catch every unmatched message with a default handler.

```python
from fastsqs import FastSQS, SQSRouter, SQSEvent

orders = SQSRouter()


class OrderCreated(SQSEvent):
    order_id: str
    amount: int


@orders.route(OrderCreated)
async def on_created(msg: OrderCreated):
    print("created", msg.order_id)


app = FastSQS()
app.include_router(orders)


@app.default()
async def fallback(payload: dict):
    print("unmatched", payload)


def handler(event, context):
    return app.handler(event, context)
```

## Group routes in a router

Define a `SQSRouter` in its own module, register routes on it, and import it where you build the app. A router supports the same routing styles as the app: pydantic routing with `@router.route(Model)` and key-value routing with `@router.route("value", model=...)`.

```python
# orders.py
from fastsqs import SQSRouter, SQSEvent

orders = SQSRouter()


class OrderCreated(SQSEvent):
    order_id: str
    amount: int


class OrderCancelled(SQSEvent):
    order_id: str


@orders.route(OrderCreated)                             # pydantic routing
async def on_created(msg: OrderCreated):
    ...


@orders.route("order_cancelled", model=OrderCancelled)  # key-value + validation
async def on_cancelled(msg: OrderCancelled):
    ...


@orders.route("ping")                                   # key-value, no model -> raw SQSEvent
async def on_ping(msg: SQSEvent):
    ...
```

A key-value route with no `model=` passes the raw `SQSEvent` to the handler. See [Route by message type](routing-by-type.md) and [Route by a key](routing-by-key.md) for the two styles.

## Attach a router to the app

Call `app.include_router(router)` to register a router. The app tries its own routes first, then each included router in registration order.

```python
from fastsqs import FastSQS

from orders import orders
from billing import billing

app = FastSQS()
app.include_router(orders)
app.include_router(billing)
```

A message that matches no app route and no included router raises `RouteNotFoundError` and becomes a batch failure, unless you register a default handler.

## Nest routers as subrouters

Use `subrouter(value, child)` to dispatch on a second discriminator once the parent has matched `value`. A subrouter reads its own discriminator key, so a parent keyed on `action` can hand off to a child keyed on `entity`.

```python
from fastsqs import FastSQS, SQSRouter, SQSEvent


class CreateUser(SQSEvent):
    name: str


class CreateOrder(SQSEvent):
    order_id: str


router = SQSRouter(discriminator="action")
create_router = SQSRouter(discriminator="entity")

router.subrouter("create", create_router)


@create_router.route("user", model=CreateUser)
async def handle_create_user(msg: CreateUser):
    print("create user", msg.name)


@create_router.route("order", model=CreateOrder)
async def handle_create_order(msg: CreateOrder):
    print("create order", msg.order_id)


app = FastSQS()
app.include_router(router)
```

A message `{"action": "create", "entity": "user", "name": "Ada"}` routes through `router` on `action=create`, then through `create_router` on `entity=user` to `handle_create_user`. Nest further by registering a subrouter on the child.

!!! note
    A subrouter inherits its parent's middleware by default. Construct it with `SQSRouter(inherit_middlewares=False)` to run only its own middleware. See [Middleware](middleware.md).

## Add a default handler

Register a catch-all with `@app.default()` (or `@router.default()`) for messages that match no route. The default handler runs under the same middleware chain as a routed handler. It can declare `msg`, `payload`, `record`, `context`, or `ctx` parameters; FastSQS matches them by name.

```python
@app.default()
async def fallback(payload: dict, ctx):
    print("unmatched", ctx.message_id, payload)
```

A router-level default catches messages that reach that router but match none of its routes:

```python
@router.default()
async def handle_unknown_action(payload: dict):
    print("unknown action", payload)
```

!!! note
    Without a default handler, an unmatched message raises `RouteNotFoundError`, which FastSQS reports as a batch failure, so SQS redelivers it. Register a default handler only when you want unmatched messages acknowledged instead.

## Resolution order

For each record, FastSQS resolves a handler in this order:

1. The app's own routes (those registered with `@app.route(...)`).
2. Each included router, in `include_router(...)` registration order; within a router, pydantic routes take precedence over key-value routes on the same discriminator value.
3. The default handler of the first router that reaches its no-match path, otherwise the app's default handler.
4. If nothing matches and no default handler exists, `RouteNotFoundError`.

!!! warning
    A single discriminator value may use only one routing style. Registering the same value as both a pydantic route and a key-value route raises `ValueError` at import, because the pydantic route would shadow the key-value handler.

## Full example

A runnable app with a parent router, two subrouters, direct routes, and a default handler lives in [`examples/nested_example`](https://github.com/fastsqs/fastsqs/tree/main/examples/nested_example).

For the underlying dispatch model, see [Routing](../concepts/routing.md). For the router API surface, see the [router reference](../reference/router.md).

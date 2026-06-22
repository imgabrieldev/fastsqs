# Route by a key value

Dispatch a message to a handler by matching a literal value in its payload, instead of by a pydantic model.

```python
from fastsqs import FastSQS, SQSRouter, SQSEvent
from pydantic import BaseModel

orders = SQSRouter()


class OrderCancelled(BaseModel):
    order_id: str
    reason: str


@orders.route("order_cancelled", model=OrderCancelled)  # key-value + validation
async def on_cancelled(msg: OrderCancelled):
    ...


@orders.route("ping")                                   # key-value, no model -> raw SQSEvent
async def on_ping(msg: SQSEvent):
    ...


app = FastSQS()
app.include_router(orders)
```

Each record routes by its discriminator value. The default discriminator key is `"type"`, so a body of `{"type": "order_cancelled", ...}` reaches `on_cancelled`, and `{"type": "ping"}` reaches `on_ping`.

## Register a key-value route

Call `@router.route(value)` with a string or integer. The decorated function becomes the handler for any message whose discriminator equals that value.

```python
@orders.route("order_cancelled")
async def on_cancelled(msg: SQSEvent):
    ...
```

Key-value routing lives on `SQSRouter`. The app-level `@app.route(...)` accepts a pydantic model only, so register key-value routes on a router and attach it with `app.include_router(router)`.

Pass several values to point them at one handler:

```python
@orders.route(["order_cancelled", "order_refunded"])
async def on_terminal(msg: SQSEvent):
    ...
```

## Validate with a model

Pass `model=` to validate the payload before the handler runs. The handler receives a parsed instance of that model.

```python
from pydantic import BaseModel


class OrderCancelled(BaseModel):
    order_id: str
    reason: str


@orders.route("order_cancelled", model=OrderCancelled)
async def on_cancelled(msg: OrderCancelled):
    print(msg.order_id, msg.reason)
```

Validation failure raises `InvalidMessageError` for that record. The record becomes a partial batch failure and SQS redelivers it; see [Handle partial batch failure](partial-batch-failure.md).

## Omit the model for raw access

Leave out `model=` to skip validation. The handler receives a raw `SQSEvent` carrying the parsed payload, with no field typing applied.

```python
@orders.route("ping")
async def on_ping(msg: SQSEvent):
    ...
```

Use this for messages that need no validation, such as health pings or pass-through events.

!!! note
    `SQSEvent` accepts both snake_case field names and their camelCase aliases. A payload may use either convention.

## One routing style per value

A single discriminator value uses one routing style. Registering the same value as both a pydantic route and a key-value route raises `ValueError` at import, because the key-value handler would be unreachable.

```python
@orders.route(OrderCreated)        # pydantic route for "order_created"

@orders.route("order_created")     # ValueError at import: value already routed by model
async def shadowed(msg: SQSEvent):
    ...
```

Registering the same value twice as a key-value route also raises `ValueError`.

## Catch unmatched values

A message whose discriminator matches no route raises `RouteNotFoundError` and becomes a batch failure. Register a default handler to catch those messages instead.

```python
@app.default()
async def fallback(msg, ctx):
    ...
```

A router takes its own default with `@router.default()`. See [Routers and default handlers](routers-and-defaults.md).

## Change the discriminator key

Both `FastSQS` and `SQSRouter` accept `discriminator=` to read a payload key other than `"type"`.

```python
orders = SQSRouter(discriminator="event")
```

With that router, `{"event": "order_cancelled", ...}` routes to the `"order_cancelled"` handler.

## Match loosely

Set `flexible_matching=True` on the app or router to also match the ClassName plus camelCase and kebab-case variants of a value. It is off by default. For pydantic vs key-value routing and how dispatch precedence works, see [How routing works](../concepts/routing.md).

## Next steps

- [Route by a model type](routing-by-type.md)
- [Split routes across routers](routers-and-defaults.md)
- [Test handlers](testing.md)
- See the [nested example](https://github.com/fastsqs/fastsqs/tree/main/examples/nested_example) for routers and subrouters.

# How routing works

This page explains how FastSQS turns a parsed SQS record into a handler call: the three routing styles, the discriminator, and snake_case matching.

```python
from fastsqs import FastSQS, SQSEvent, SQSRouter

app = FastSQS()  # discriminator defaults to "type"


class OrderCreated(SQSEvent):
    order_id: str
    amount: int


@app.route(OrderCreated)              # pydantic routing
async def on_created(msg: OrderCreated):
    ...


orders = SQSRouter()


@orders.route("order_cancelled", model=OrderCreated)  # key-value routing
async def on_cancelled(msg: OrderCreated):
    ...


@app.default()                        # catch-all for unmatched messages
async def fallback(msg, ctx):
    ...


app.include_router(orders)

# {"type": "order_created", "order_id": "1", "amount": 5} -> on_created
# {"type": "order_cancelled", ...}                         -> on_cancelled
# {"type": "anything_else", ...}                           -> fallback
```

## The discriminator

FastSQS reads a single field from each message body to decide where it goes. That field is the discriminator. Its key defaults to `"type"` and you set it per app or per router:

```python
app = FastSQS(discriminator="event")          # read body["event"] instead of body["type"]
router = SQSRouter(discriminator="event")
```

The discriminator's *value* is a string that names the route. FastSQS looks that value up in a table of registered routes. A body with no discriminator field, or one whose value matches no route, goes to the default handler if you registered one, and otherwise raises `RouteNotFoundError` (the record becomes a batch failure). See [How partial batch failure works](partial-batch-failure.md) for what happens to a failed record after that.

## Pydantic routing

Register a route by passing an `SQSEvent` subclass. The route value is the class name in snake_case, and the body is validated against that model before your handler runs:

```python
class OrderCreated(SQSEvent):
    order_id: str
    amount: int


@app.route(OrderCreated)
async def on_created(msg: OrderCreated):
    ...
```

`OrderCreated.get_message_type()` returns `"order_created"`, so a body of `{"type": "order_created", ...}` routes here. The conversion inserts an underscore before each interior capital and lowercases the result: `OrderCreated` becomes `order_created`, `HTTPRequest` becomes `h_t_t_p_request`. Name your event classes in PascalCase and the discriminator value is the snake_case form.

The handler receives a parsed, validated model instance. A body that is not valid JSON, is not a JSON object, or fails model validation raises `InvalidMessageError` and becomes a batch failure before the handler is reached.

### snake_case and camelCase fields

`SQSEvent` accepts each field under both its snake_case name and its camelCase alias. This is Pydantic alias generation with `populate_by_name`, not a bespoke normalizer:

```python
class OrderCreated(SQSEvent):
    order_id: str
    amount: int


# both bodies validate into the same model:
# {"type": "order_created", "order_id": "1", "amount": 5}
# {"type": "order_created", "orderId": "1",  "amount": 5}
```

!!! note
    The discriminator value (`order_created`) is matched in snake_case against the class name. Field aliasing (`order_id` / `orderId`) is a separate mechanism that applies *after* the route is chosen, when the body is validated. kebab-case field keys are not auto-mapped.

## Key-value routing

Register a route by passing a literal string instead of a model. The string is the discriminator value to match. This suits messages whose `type` is not derived from a class name:

```python
router = SQSRouter()


@router.route("order_cancelled", model=OrderCreated)  # validate against a model
async def on_cancelled(msg: OrderCreated):
    ...


@router.route("ping")                                 # no model -> raw SQSEvent
async def on_ping(msg: SQSEvent):
    ...
```

Pass `model=` to validate the body against an `SQSEvent` subclass, exactly as pydantic routing does. Omit it and the handler receives the raw body wrapped in a base `SQSEvent`, with no field validation. Key-value routing lives on `SQSRouter`; combine routers with the app through `include_router(...)`, covered in [Routers and default handlers](../guide/routers-and-defaults.md).

## One value, one style

A single discriminator value uses exactly one routing style. Registering the same value as both a pydantic route and a key-value route raises `ValueError` at decoration time, when the module imports:

```python
@app.route(OrderCreated)            # registers "order_created" (pydantic)
async def a(msg: OrderCreated):
    ...


@app.route("order_created")         # same value, key-value -> ValueError at import
async def b(msg: SQSEvent):
    ...
```

The error surfaces at import rather than at runtime, so a shadowed, unreachable handler cannot ship silently.

## The default handler

Register a catch-all with `@app.default()` (or `@router.default()`). It runs for any message whose discriminator value matches no route, including a message with no discriminator field at all:

```python
@app.default()
async def fallback(msg, ctx):
    ...
```

Without a default handler, an unmatched message raises `RouteNotFoundError` and is reported as a batch failure. With one, the message is handled instead of failed. A default handler does not validate the body against a model; it receives the raw body.

## Resolution order

FastSQS resolves a record in this order:

1. Read the discriminator value from the body.
2. Match it against the app's own routes (pydantic and key-value share one table).
3. If no app route matches, try each included router in registration order.
4. If still no match, call the default handler if one exists; otherwise raise `RouteNotFoundError`.

App routes are tried before included routers, so an app-level route takes precedence over a router route that registered the same value.

## Flexible matching

By default a body's discriminator value matches a route only by its exact snake_case form. Set `flexible_matching=True` on the app or a router to also match the class name and its camelCase and kebab-case variants:

```python
app = FastSQS(flexible_matching=True)   # default is False


@app.route(OrderCreated)
async def on_created(msg: OrderCreated):
    ...


# with flexible_matching=True, all of these route to on_created:
#   {"type": "order_created"}   (snake_case)
#   {"type": "OrderCreated"}    (class name)
#   {"type": "orderCreated"}    (camelCase)
#   {"type": "order-created"}   (kebab-case)
```

`SQSEvent.get_message_type_variants()` returns the set FastSQS matches against: the class name plus its snake_case, camelCase, and kebab-case forms. Leave `flexible_matching` off when you control the producers and want the discriminator value to be exact.

## Related

- [Routing by type](../guide/routing-by-type.md) — task guide for pydantic routing.
- [Routing by key](../guide/routing-by-key.md) — task guide for key-value routing.
- [Routers and default handlers](../guide/routers-and-defaults.md) — splitting and nesting routes.
- [The processing lifecycle](lifecycle.md) — where routing sits relative to validation and middleware.

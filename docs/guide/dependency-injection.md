# Inject dependencies

Declare `Depends(...)` parameters on a handler to receive resources resolved per invocation, without an `@inject` decorator.

```python
from fastsqs import FastSQS, SQSEvent, Depends


class OrderCreated(SQSEvent):
    order_id: str
    amount: int


def get_db():
    return Database(...)


app = FastSQS()


@app.route(OrderCreated)
async def handle(msg: OrderCreated, db=Depends(get_db)):
    await db.save(msg.order_id)
```

FastSQS detects the `Depends(...)` default on a handler parameter and wires the dependency through [fast-depends](https://lancetnik.github.io/FastDepends/). The handler still receives `msg` (the validated [SQSEvent](../reference/events.md)) by name; `db` arrives already resolved.

## Add a dependency

A dependency is any callable. Pass it to `Depends(...)` as the default value of a handler parameter. FastSQS calls it on each record and binds the return value to that parameter.

```python
from fastsqs import FastSQS, SQSEvent, Depends


class OrderCreated(SQSEvent):
    order_id: str


def get_settings():
    return {"region": "us-east-1"}


app = FastSQS()


@app.route(OrderCreated)
async def handle(msg: OrderCreated, settings=Depends(get_settings)):
    print(settings["region"], msg.order_id)
```

`Depends` is re-exported from `fastsqs`, so import it from there rather than from fast-depends directly.

!!! note
    A dependency referenced once per invocation resolves once. fast-depends caches each dependency by its callable for the duration of a single record, so two parameters that both `Depends(get_settings)` share one resolved value.

## Chain sub-dependencies

A dependency may itself declare `Depends(...)` parameters. fast-depends resolves the whole graph before the handler runs, so you compose narrow dependencies into wider ones.

```python
from fastsqs import FastSQS, SQSEvent, Depends


class OrderCreated(SQSEvent):
    order_id: str


def get_config():
    return {"region": "us-east-1"}


def get_client(config=Depends(get_config)):   # depends on another dependency
    return f"client@{config['region']}"


app = FastSQS()


@app.route(OrderCreated)
async def handle(msg: OrderCreated, client=Depends(get_client)):
    print(client)   # "client@us-east-1"
```

`get_client` declares `config=Depends(get_config)`. FastSQS resolves `get_config` first, passes its value into `get_client`, then binds `get_client`'s result to the handler's `client` parameter. The handler declares only what it consumes.

## Mix dependencies with the typed Context

Dependencies and the framework parameters coexist. Annotate a parameter `ctx: Context` for the typed [Context](../reference/context.md) alongside any `Depends(...)` parameters; FastSQS binds each by its role.

```python
from fastsqs import FastSQS, SQSEvent, Context, Depends


class OrderCreated(SQSEvent):
    order_id: str


def get_db():
    return Database(...)


app = FastSQS()


@app.route(OrderCreated)
async def handle(msg: OrderCreated, ctx: Context, db=Depends(get_db)):
    ctx.state.db_region = "us-east-1"
    await db.save(msg.order_id, message_id=ctx.message_id)
```

A handler with no `Depends(...)` parameter is left untouched. FastSQS only wires injection when at least one parameter carries a `Depends(...)` default, so existing name-based handlers keep their behavior.

## Raising inside a dependency

A dependency that raises fails the record like any other handler error. FastSQS reports the record in `batchItemFailures`, and SQS redelivers it under the queue's redrive policy. Use this to fail fast when a required resource is unavailable.

```python
from fastsqs import FastSQS, SQSEvent, Depends


class OrderCreated(SQSEvent):
    order_id: str


def get_db():
    conn = connect()
    if conn is None:
        raise RuntimeError("database unavailable")
    return conn


app = FastSQS()


@app.route(OrderCreated)
async def handle(msg: OrderCreated, db=Depends(get_db)):
    await db.save(msg.order_id)
```

See [Report partial batch failures](partial-batch-failure.md) for how reported failures translate into redelivery.

## Dependencies on routers and defaults

`Depends(...)` works on any handler, including those registered on an [SQSRouter](../reference/router.md), on key-value routes, and on the [default handler](routers-and-defaults.md). Declaration is identical wherever the handler lives.

```python
from fastsqs import FastSQS, SQSRouter, SQSEvent, Depends


class OrderCreated(SQSEvent):
    order_id: str


def get_db():
    return Database(...)


orders = SQSRouter()


@orders.route(OrderCreated)
async def handle(msg: OrderCreated, db=Depends(get_db)):
    await db.save(msg.order_id)


app = FastSQS()
app.include_router(orders)
```

## When to reach for middleware instead

`Depends(...)` supplies values to one handler. A resource that every handler needs, or one that must be released after the handler runs, belongs in [middleware](middleware.md): `before` acquires it and the balanced `after` releases it, even on failure. Use dependencies for per-handler inputs and middleware for cross-cutting setup and teardown.

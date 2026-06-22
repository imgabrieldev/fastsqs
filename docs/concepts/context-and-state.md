# Context vs State

This page explains why a `Context` carries two kinds of data — framework-owned fields and your own scratch — and why they live on separate surfaces.

```python
from fastsqs import FastSQS, Context

app = FastSQS()


@app.route(OrderCreated)
async def handle(msg: OrderCreated, ctx: Context):
    ctx.message_id             # str, set by the framework
    ctx.queue_type             # QueueType enum (.value for the string)
    ctx.fifo_info              # FifoInfo | None (.message_group_id, ...)

    ctx.state.tenant = "acme"  # your scratch, attribute access
    ctx.state["tenant"]        # item access works too
    ctx.state.get("missing")   # optional read; returns None if unset
```

FastSQS builds one `Context` per record and threads that same instance through the middleware stack and into the handler. It holds everything known about the record being processed. Annotate a handler or middleware param `ctx: Context` to receive it with full typing.

## Two surfaces, two owners

A `Context` separates the data FastSQS owns from the data you own.

Framework-owned fields are typed attributes on the `Context` itself: `ctx.message_id`, `ctx.record`, `ctx.lambda_context`, `ctx.queue_type`, `ctx.route_path`, `ctx.message_type`, `ctx.fifo_info`, and `ctx.handler_result`. FastSQS populates these as it parses and routes the record. They have a single, fixed read path and a stable type.

Your scratch goes in `ctx.state`, a separate `State` namespace. This is the only writable surface for arbitrary data. Middleware and handlers read and write it freely.

## Why the surfaces are separate

In a `dict`-shaped context, a write you make and a field the framework sets share one keyspace. A scratch key named `message_id` would overwrite the real message id, and a typo would silently create a key no one reads.

Splitting the two surfaces removes that class of bug. Scratch lives in `ctx.state` and cannot collide with or clobber a framework field, because the framework fields are not keys in `state` at all — they are attributes on the `Context`. A type checker also sees `ctx.message_id` as a `str` and `ctx.fifo_info` as `FifoInfo | None`, so a wrong access fails at check time rather than at runtime.

!!! note
    Before 1.0.0, `Context` was a `dict` subclass and you read fields with string keys such as `ctx["messageId"]`. That surface is gone. Framework fields are now snake_case typed attributes, and scratch belongs in `ctx.state`.

## Reading and writing State

`State` supports both attribute and item access, so use whichever reads better at the call site.

```python
ctx.state.attempt = 1          # attribute write
ctx.state["attempt"] += 1      # item access
ctx.state.setdefault("seen", []).append(ctx.message_id)
"attempt" in ctx.state         # membership test
```

A bare attribute read of an unset key raises `AttributeError`; the same applies to an unset item key with `KeyError`. For an optional read, use `.get()`, which returns `None` (or a default you pass) when the key is absent.

```python
trace_id = ctx.state.get("trace_id")          # None if unset
mode = ctx.state.get("mode", "default")        # explicit fallback
```

!!! tip
    Use `.get()` whenever a key may not have been set yet — for example reading in `after` a value that an earlier middleware writes only on some paths. A bare `ctx.state.missing` raises.

## How middleware and handlers share State

`State` is the channel between middleware and the handler. A middleware writes in `before`, and the handler or a later `after` reads it. Because every stage receives the same `Context` instance, a write is visible everywhere downstream.

```python
from fastsqs import Middleware

class Audit(Middleware):
    async def before(self, payload, record, context, ctx):
        ctx.state.t0 = monotonic()

    async def after(self, payload, record, context, ctx, error):
        elapsed = monotonic() - ctx.state.t0
        ...  # record elapsed, observe error
```

The built-in `TimingMiddleware` follows this pattern: it writes `duration_ms` into `ctx.state` under a key you can configure with `store_key_ms`. See [Built-in middleware](../guide/builtin-middleware.md) for the before/after contract.

## One instance per record

Each record in a batch gets its own `Context`, and FastSQS threads that single instance through the whole stack rather than copying it. Do not `deepcopy` a `Context`: `record` and the Lambda `lambda_context` it holds are not safely copyable. Pass the one instance along.

## See also

- [Context, State, FifoInfo](../reference/context.md) — the full API for these types
- [Lifecycle](lifecycle.md) — when the `Context` is built and how it flows through the stack
- [Write middleware](../guide/middleware.md) — the `Middleware` base class and the before/after stages that share `State`
- [Built-in middleware](../guide/builtin-middleware.md) — `TimingMiddleware` writing `duration_ms` into `State`

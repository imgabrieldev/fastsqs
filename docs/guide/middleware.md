# Write middleware

Middleware lets you run code around every record: acquire a resource or read context in `before`, observe the outcome in `after`. Subclass `Middleware`, override the hooks you need, and register the instance with `add_middleware`.

```python
import time

from fastsqs import FastSQS, SQSEvent, Middleware


class Timing(Middleware):
    async def before(self, payload, record, context, ctx):
        ctx.state.t0 = time.perf_counter()

    async def after(self, payload, record, context, ctx, error):
        elapsed_ms = (time.perf_counter() - ctx.state.t0) * 1000
        status = "failed" if error is not None else "ok"
        print(f"{ctx.message_id} {status} in {elapsed_ms:.1f}ms")


class OrderCreated(SQSEvent):
    order_id: str
    amount: int


app = FastSQS()
app.add_middleware(Timing())


@app.route(OrderCreated)
async def handle_order(msg: OrderCreated):
    print("processing", msg.order_id, msg.amount)


def handler(event, context):
    return app.handler(event, context)
```

## The two hooks

Override either or both. Both are `async`, both receive the same four arguments, and `after` receives one more:

- `before(self, payload, record, context, ctx)` runs before the handler.
- `after(self, payload, record, context, ctx, error)` runs after the handler.

The arguments:

- `payload`: the parsed message body (a `dict`).
- `record`: the raw SQS record (a `dict`), including `record["messageId"]` and FIFO system attributes under `record["attributes"]`.
- `context`: the Lambda context object passed to your handler.
- `ctx`: the per-record [`Context`](../concepts/context-and-state.md). Read framework fields like `ctx.message_id` and `ctx.queue_type`; stash your own data in `ctx.state`.
- `error`: in `after` only, the exception that the handler (or an earlier `before`) raised, or `None` on success.

```python
class Audit(Middleware):
    async def before(self, payload, record, context, ctx):
        ctx.state.received_at = record["attributes"].get("SentTimestamp")

    async def after(self, payload, record, context, ctx, error):
        if error is not None:
            log.warning("record %s failed: %s", ctx.message_id, error)
```

## Pass data from before to after

Use `ctx.state` to carry per-record data across the two hooks. `ctx.state` is a `State` scratch namespace scoped to one record; it never collides with a framework field. Use `.get()` for reads that may be absent.

```python
class Tenant(Middleware):
    async def before(self, payload, record, context, ctx):
        ctx.state.tenant = payload.get("tenant", "default")

    async def after(self, payload, record, context, ctx, error):
        tenant = ctx.state.get("tenant", "default")
        emit_metric("processed", tenant=tenant, ok=error is None)
```

!!! note
    Each record gets a fresh `ctx`. Do not store cross-record totals in `ctx.state`; keep aggregate counters in the middleware instance or an external store.

## Registration order and balanced unwind

You register middleware with `add_middleware`, and it runs as a stack. `before` hooks run in registration order; `after` hooks run in reverse. The hook order for two middlewares A then B is: `A.before`, `B.before`, handler, `B.after`, `A.after`.

```python
app.add_middleware(A())   # outermost
app.add_middleware(B())   # innermost
```

`after` runs for every middleware whose `before` completed, even when a later `before` or the handler raises. This balanced unwind keeps enter and exit symmetric, so a resource you acquire in `before` is always released in `after`:

```python
class Slot(Middleware):
    async def before(self, payload, record, context, ctx):
        ctx.state.slot = await pool.acquire()

    async def after(self, payload, record, context, ctx, error):
        await ctx.state.slot.release()   # runs even if the handler raised
```

## Abort a record from before

Raising from `before` aborts the record: the handler does not run, and the record becomes a [partial batch failure](partial-batch-failure.md). Middlewares already entered are still unwound through `after`, which receives the raised exception as `error`. Use this for cheap validation or guard checks before the handler:

```python
class RequireTenant(Middleware):
    async def before(self, payload, record, context, ctx):
        if "tenant" not in payload:
            raise ValueError("missing tenant")
```

## Errors in after are isolated

An exception raised inside an `after` hook is logged and swallowed: it neither aborts the remaining `after` hooks nor masks the handler's original error. The original error is re-raised after the unwind completes, so the record's failure status is preserved. Keep `after` resilient, but do not rely on it to change a record's outcome.

## Compose application concerns as middleware

Observability, idempotency, and PII masking are application concerns. Build each as its own middleware and register them in the order you want them to wrap the handler. For timing and structured logging, FastSQS ships `TimingMiddleware` and `LoggingMiddleware` so you do not have to write them; see [Use built-in middleware](builtin-middleware.md).

A full runnable sample (custom logging, error handling, and metrics middleware) lives in [`examples/custom_middleware_example`](https://github.com/fastsqs/fastsqs/tree/main/examples/custom_middleware_example).

## Test middleware in process

Drive your app with the [`SQSTestClient`](testing.md) to assert that a middleware runs and that an aborting `before` produces a reported failure:

```python
from fastsqs.testing import SQSTestClient

client = SQSTestClient(app)

result = client.send({"type": "order_created", "order_id": "1", "amount": 5})
assert result == {"batchItemFailures": []}
```

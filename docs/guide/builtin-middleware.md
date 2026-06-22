# Use built-in middleware

This page shows how to attach the two middleware that ship with FastSQS: `LoggingMiddleware` for structured per-record logs, and `TimingMiddleware` for processing duration.

```python
from fastsqs import FastSQS, SQSEvent, LoggingMiddleware, TimingMiddleware


class OrderPlaced(SQSEvent):
    order_id: str

    @classmethod
    def get_message_type(cls) -> str:
        return "order_placed"


app = FastSQS()
app.add_middleware(LoggingMiddleware())
app.add_middleware(TimingMiddleware())


@app.route(OrderPlaced)
async def handle_order(msg: OrderPlaced):
    return {"order_id": msg.order_id}


def handler(event, context):
    return app.handler(event, context)
```

`add_middleware` registers each instance once. Both run on every record the app processes.

## Order and unwind

Middleware runs in registration order on the way in, and in reverse on the way out. `before` fires top to bottom; `after` fires bottom to top for every middleware whose `before` completed.

In the snippet above `LoggingMiddleware.before` runs first, then `TimingMiddleware.before`. On the way out `TimingMiddleware.after` runs first and writes the duration, then `LoggingMiddleware.after` runs and can read it. Register `TimingMiddleware` after `LoggingMiddleware` when you want the logged duration to cover the inner middleware and the handler.

## LoggingMiddleware

`LoggingMiddleware` emits one JSON line on `before_processing` and one on `after_processing` for each record. The default logger prints JSON to stdout, which CloudWatch ingests without extra configuration.

Both lines carry the message id and the resolved route path. The `before_processing` line also carries the queue type and the Lambda request id. The `after_processing` line carries the duration (when `TimingMiddleware` is registered), the handler result type, and, on failure, the error type, message, and traceback.

Control what each line includes with the constructor toggles:

```python
app.add_middleware(
    LoggingMiddleware(
        level="INFO",
        include_payload=True,   # the parsed message body
        include_record=False,   # the raw SQS record
        include_context=False,  # repr of the Lambda context
        verbose=True,           # also log ctx.state keys
    )
)
```

The defaults are `include_payload=True`, `include_record=False`, `include_context=False`, and `verbose=True`.

!!! warning
    `include_payload=True` writes the parsed message body into the log line. If the body holds passwords, tokens, or PII, those values reach CloudWatch in clear text. Set `include_payload=False`, or write your own middleware to mask fields before `LoggingMiddleware` writes them. Field masking is an application concern; see [Use custom middleware](middleware.md).

To route logs through your own sink, pass a `logger` callable. It receives the structured `dict` for each line:

```python
import logging

_log = logging.getLogger("orders")


def emit(entry: dict) -> None:
    _log.info(entry)


app.add_middleware(LoggingMiddleware(logger=emit))
```

## TimingMiddleware

`TimingMiddleware` records a start time in `ctx.state` during `before`, then writes the elapsed milliseconds back to `ctx.state` during `after`. Any later middleware or your handler can read the value.

```python
from fastsqs import FastSQS, TimingMiddleware, Context

app = FastSQS()
app.add_middleware(TimingMiddleware())


@app.route(OrderPlaced)
async def handle_order(msg: OrderPlaced, ctx: Context):
    # Available in `after`; read it here only if an earlier middleware set it.
    return {"order_id": msg.order_id}
```

The duration lands under `ctx.state["duration_ms"]`. Change the key with `store_key_ms`:

```python
app.add_middleware(TimingMiddleware(store_key_ms="latency_ms"))
```

`TimingMiddleware.after` reads the start time defensively. If `before` did not run for this middleware during an unwind, it skips the calculation rather than failing.

!!! note
    `LoggingMiddleware` reports `duration_ms` only when a `TimingMiddleware` using the default `store_key_ms="duration_ms"` ran before it on the way out. Keep that key, and register the timing middleware after the logging middleware.

## Combine them

Register both to get timed, structured logs per record. The handler stays unchanged.

```python
from fastsqs import FastSQS, SQSEvent, LoggingMiddleware, TimingMiddleware


class UserLogin(SQSEvent):
    user_id: str

    @classmethod
    def get_message_type(cls) -> str:
        return "login"


app = FastSQS(discriminator="action")
app.add_middleware(LoggingMiddleware(include_payload=False))
app.add_middleware(TimingMiddleware())


@app.route(UserLogin)
async def handle_login(msg: UserLogin):
    return {"status": "ok", "user_id": msg.user_id}


def handler(event, context):
    return app.handler(event, context)
```

The runnable version of this setup, with masking of sensitive fields, lives in the [middleware_example](https://github.com/fastsqs/fastsqs/tree/main/examples/middleware_example) on GitHub.

## Next steps

- Write your own `before`/`after` hooks in [Use custom middleware](middleware.md).
- Read what `ctx.state` holds across a record in [Context and state](../concepts/context-and-state.md).
- See the full constructor surface in the [middleware reference](../reference/middleware.md).

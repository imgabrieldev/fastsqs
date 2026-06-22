# Troubleshooting and FAQ

This page diagnoses the common failure modes when running FastSQS on Lambda and points you at the fix.

Start every diagnosis from the test client, which reproduces the failure in-process with no AWS:

```python
from fastsqs.testing import SQSTestClient

client = SQSTestClient(app)
result = client.send({"type": "order_created", "order_id": "1", "amount": 5})
print(result)  # {"batchItemFailures": [...]} tells you exactly which records failed
```

## Failed messages are never retried

A handler raises, the invocation succeeds anyway, and the message is gone. The event source mapping is not reading your partial response.

FastSQS only reports failures. Redelivery and dead-lettering belong to the queue: its visibility timeout, `maxReceiveCount`, and redrive policy. For SQS to honour the per-record response, the event source mapping must enable `ReportBatchItemFailures`.

```yaml
# AWS SAM / CloudFormation: the event source mapping on the function
FunctionResponseTypes:
  - ReportBatchItemFailures
```

!!! warning
    Without `FunctionResponseTypes: ["ReportBatchItemFailures"]` on the event source mapping, SQS ignores the `batchItemFailures` response. A clean invocation deletes every message in the batch, including the ones your handler failed. See the [AWS docs on reporting batch item failures](https://docs.aws.amazon.com/lambda/latest/dg/services-sqs-errorhandling.html).

To confirm the app side is correct, assert the reported failures with the test client:

```python
result = client.send("{not json", message_id="bad")
assert result == {"batchItemFailures": [{"itemIdentifier": "bad"}]}
```

If the test client reports the failure but production still drops it, the gap is the mapping configuration, not the app.

## A message type matches no route

An unmatched message raises `RouteNotFoundError` and becomes a batch failure, so SQS redelivers it until it dead-letters. This is by design: a message you cannot route is not a message you should silently drop.

Two fixes, depending on intent.

Register a default handler to absorb unmatched messages:

```python
@app.default()
async def fallback(msg, ctx):
    ...  # log, forward, or discard deliberately
```

Or confirm the discriminator value matches the route. Routing is by the discriminator (default key `"type"`), matched to the event model name in snake_case. `OrderCreated` is reached by `{"type": "order_created", ...}`.

!!! note
    `flexible_matching=True` (on `FastSQS` or `SQSRouter`, default `False`) also matches the ClassName plus camelCase and kebab-case variants of the discriminator value. A single discriminator value may use only one routing style; registering it as both a pydantic and a key-value route raises `ValueError` at import.

For the matching rules in full, see [Routing by type](guide/routing-by-type.md) and [Routing by key](guide/routing-by-key.md).

## A FIFO message group is stuck

One message in a `MessageGroupId` fails, and every later message in that group stops processing. This is correct FIFO behaviour, not a bug: the default `fifo_failure_mode` is `"isolate_groups"`, which blocks the failed group's tail to preserve order. Other groups continue.

The usual cause of a *permanently* stuck group is a poison message that fails every redelivery. Fix the message or its handler, or attach a dead-letter queue so `maxReceiveCount` eventually drains the poison message and unblocks the group.

The most common silent cause is a malformed test event. SQS exposes FIFO system attributes in PascalCase, and dropping that detail collapses every message into one group.

!!! warning
    SQS exposes system attributes (`MessageGroupId`, `MessageDeduplicationId`) in **PascalCase** under `record["attributes"]`, unlike the camelCase record-level keys. Keep raw test events faithful, or FIFO grouping silently collapses into a single group. `SQSTestClient` already emits PascalCase, so prefer it for FIFO tests.

```python
from fastsqs.testing import SQSTestClient, RecordSpec

client = SQSTestClient(app)

# two distinct groups: a failure in g1 never blocks g2
client.send_batch([
    RecordSpec({"type": "order_created", "order_id": "1", "amount": 1}, group_id="g1"),
    RecordSpec({"type": "order_created", "order_id": "2", "amount": 2}, group_id="g2"),
])
```

If you instead want the whole batch to stop at the first failure, set `fifo_failure_mode="halt_batch"`.

!!! warning
    `halt_batch` re-reports records that already succeeded before the failure, so SQS redelivers them. Your handlers must be idempotent under `halt_batch`.

See [FIFO failure modes](guide/fifo-failure-modes.md) and [FIFO ordering](concepts/fifo-ordering.md).

## Which record in the batch failed

Read the returned `batchItemFailures`: each entry's `itemIdentifier` is the failing record's `messageId`.

```python
result = app.handler(event, context)
for item in result["batchItemFailures"]:
    print("failed:", item["itemIdentifier"])
```

If you see an `itemIdentifier` of `"UNKNOWN"`, the failing record carried a missing or empty `messageId`. FastSQS coalesces an absent, empty-string, or `None` identifier to the `"UNKNOWN"` sentinel so the response never contains an empty or null `itemIdentifier`.

!!! note
    SQS and EventBridge read an empty or `null` `itemIdentifier` as a *whole-batch* failure, which would redeliver every message. The `"UNKNOWN"` sentinel keeps the failure scoped to the one record. A real `messageId` is always preferred; `"UNKNOWN"` means the source record lacked one.

To attribute a failure to its payload during a handler run, read the typed `Context`:

```python
from fastsqs import FastSQS, SQSEvent, Context

@app.route(OrderCreated)
async def handle(msg: OrderCreated, ctx: Context):
    ctx.message_id   # the record's messageId, also used as the failure itemIdentifier
```

See [Partial batch failure](guide/partial-batch-failure.md) and [Context and state](concepts/context-and-state.md).

## A Pipes (bare-list) event is not recognised

An EventBridge Pipes SQS-source target delivers a **bare list** of records, not the `{"Records": [...]}` envelope an event source mapping sends. `app.handler` accepts both shapes unchanged, so a Pipe and a mapping run the same handler.

```python
def handler(event, context):
    return app.handler(event, context)  # accepts {"Records": [...]} and a bare list
```

If a single Lambda serves both SQS and non-SQS traffic (for example API Gateway), dispatch by shape with `is_sqs_event`, which returns `True` for a bare list or a `Records` dict:

```python
from fastsqs import is_sqs_event

def handler(event, context):
    if is_sqs_event(event):
        return app.handler(event, context)
    return http_handler(event, context)  # e.g. an API Gateway proxy event
```

!!! note
    A bare-list event containing a **non-dict element** (a malformed enrichment array item such as a JSON string, number, or `null`) does not crash the batch. FastSQS reports that element as its own batch-item failure and processes its siblings normally.

See [EventBridge Pipes](guide/eventbridge-pipes.md) and [Multiplexing](guide/multiplexing.md).

## A valid-looking message fails validation

A handler never runs and you get a batch failure with no obvious cause. The body failed parsing or pydantic validation, which raises `InvalidMessageError`: a non-JSON body, a non-object body, or a field that does not satisfy the model.

Field names accept both snake_case and their camelCase aliases (`order_id` or `orderId`) via Pydantic alias generation. kebab-case keys are not auto-mapped. Confirm the body matches the model:

```python
class OrderCreated(SQSEvent):
    order_id: str
    amount: int

# routes and validates: {"type": "order_created", "order_id": "1", "amount": 5}
# fails validation: {"type": "order_created", "order_id": "1", "amount": "five"}
```

A malformed body becomes a reported failure, not an unhandled exception, so SQS can redeliver and eventually dead-letter it.

## Exception reference

All errors derive from `FastSQSError`:

- `RouteNotFoundError`: a message matched no route and no default handler is registered.
- `InvalidMessageError`: a non-JSON body, a non-object body, or a pydantic validation failure.
- `BatchFailedError`: raised when `partial_batch_failure=False` and any record fails; `.failures` holds the failed item ids.

By default `partial_batch_failure=True` reports per-record failures. Set it `False` to fail the entire batch instead, which raises `BatchFailedError` so SQS redelivers every message in the batch.

See the [exceptions reference](reference/exceptions.md) and [Partial batch failure](concepts/partial-batch-failure.md).

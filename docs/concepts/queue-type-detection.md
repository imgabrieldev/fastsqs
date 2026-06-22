# Queue-type detection

Queue-type detection is how FastSQS decides whether a batch came from a standard or a FIFO queue, and so which processing path to run.

```python
from fastsqs import FastSQS, QueueType

# AUTO (the default): infer the queue type from each batch's eventSourceARN.
app = FastSQS()

# Equivalent, written out:
app = FastSQS(queue_type=QueueType.AUTO)

# Force a type, skipping inference:
app = FastSQS(queue_type=QueueType.STANDARD)
app = FastSQS(queue_type=QueueType.FIFO)
```

`QueueType` has three members. `AUTO` is the default and infers the type at runtime. `STANDARD` and `FIFO` pin it.

## Why the type matters

The resolved type selects the processing path, and the two paths differ in ways you can observe.

A standard batch runs records concurrently, up to `max_concurrent_messages`. Order is not preserved, because SQS standard queues do not promise order.

A FIFO batch runs records in arrival order and groups them by `messageGroupId`. `max_concurrent_messages` does not apply. The `fifo_failure_mode` setting (`"isolate_groups"` or `"halt_batch"`) governs what happens to a group after a record in it fails. See [FIFO ordering](fifo-ordering.md) and [FIFO failure modes](../guide/fifo-failure-modes.md).

The resolved type is also visible to your code as `ctx.queue_type`, and FIFO system attributes surface on `ctx.fifo_info`. See [Context and State](context-and-state.md).

## How AUTO infers the type

FastSQS reads the `eventSourceARN` of the batch and checks its suffix. A FIFO queue's ARN ends in `.fifo`; a standard queue's does not.

```text
arn:aws:sqs:us-east-1:111122223333:orders.fifo   -> QueueType.FIFO
arn:aws:sqs:us-east-1:111122223333:orders         -> QueueType.STANDARD
```

The check is per batch, not per application. FastSQS resolves the type from the records present in the event it is handling, so the same app handles a standard event source mapping and a FIFO one without reconfiguration.

When `queue_type` is set to `STANDARD` or `FIFO`, FastSQS honors that value and does not inspect the ARN.

!!! note
    AUTO became the default in 1.0.0. Before that the default was `STANDARD`, which silently ran a FIFO queue on the concurrent standard path and broke ordering. AUTO removes that footgun: a `.fifo` source is recognized and run on the ordered path.

## When AUTO falls back to STANDARD

AUTO infers `FIFO` only from a `.fifo` suffix. Every other case resolves to `STANDARD`:

- The first record's `eventSourceARN` does not end in `.fifo`.
- The `eventSourceARN` is absent or empty.
- The batch carries no records.

A bare list of records (the EventBridge Pipes target shape) is inspected the same way: each element should carry an `eventSourceARN`, and the suffix drives the result. See [EventBridge Pipes](../guide/eventbridge-pipes.md).

!!! warning
    A FIFO event whose records lack a `.fifo` ARN resolves to `STANDARD` under AUTO and runs on the concurrent, unordered path. If a source does not deliver the ARN, pin the type with `queue_type=QueueType.FIFO` rather than relying on inference.

## Pinning the type

Set `queue_type` at construction when you know the queue type and want to skip inference, or when the event source does not carry a reliable ARN.

```python
from fastsqs import FastSQS, QueueType

# A FIFO app: always run the ordered path, regardless of the ARN.
app = FastSQS(
    queue_type=QueueType.FIFO,
    fifo_failure_mode="isolate_groups",
)
```

`queue_type` is keyword-only and set once, at construction. There is no method to mutate it on a warm app.

## Related

- [Queue type reference](../reference/queue-type.md) — the `QueueType` enum.
- [Application reference](../reference/app.md) — the `FastSQS` constructor and its parameters.
- [FIFO ordering](fifo-ordering.md) — how the FIFO path orders and groups records.
- [AWS SQS FIFO queues](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queues.html) — the `.fifo` naming requirement.

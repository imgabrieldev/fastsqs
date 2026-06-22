# Test handlers in-process

`SQSTestClient` dispatches a synthetic SQS event to your app in-process, so you can assert on the result without hand-building the event envelope or standing up a queue.

```python
from pydantic import BaseModel
from fastsqs import FastSQS
from fastsqs.testing import SQSTestClient

class OrderCreated(BaseModel):
    type: str
    order_id: str
    amount: int

app = FastSQS()

@app.route(OrderCreated)
async def handle(msg: OrderCreated) -> None:
    ...

client = SQSTestClient(app)

result = client.send({"type": "order_created", "order_id": "1", "amount": 5})
assert result == {"batchItemFailures": []}
```

`SQSTestClient` is the SQS analog of `fastapi.testclient.TestClient`. It wraps a `FastSQS` app, builds the raw `{"Records": [...]}` envelope for you, and calls `app.handler`. The return value is exactly what your Lambda returns: `{"batchItemFailures": [...]}`.

It lives in `fastsqs.testing`, not the package root. Import it from there.

## Send one message

`send` takes a body and returns the handler result. Pass a `dict` to have it JSON-encoded, or a raw `str`/`bytes` to control the wire body verbatim.

```python
result = client.send({"type": "order_created", "order_id": "1", "amount": 5})
assert result == {"batchItemFailures": []}
```

The default `event_source_arn` is a standard-queue ARN, so a `QueueType.AUTO` app infers a standard queue. To exercise FIFO behavior, set a `group_id` (covered below) or pass an explicit `event_source_arn`.

## Send a FIFO batch with message groups

`send_batch` takes a list of `RecordSpec` items. Give each record a distinct `group_id` to model multiple FIFO message groups in one batch.

```python
from fastsqs.testing import SQSTestClient, RecordSpec

client = SQSTestClient(app)

result = client.send_batch([
    RecordSpec({"type": "order_created", "order_id": "1", "amount": 1}, group_id="g1"),
    RecordSpec({"type": "order_created", "order_id": "2", "amount": 2}, group_id="g2"),
])
assert result == {"batchItemFailures": []}
```

When you set a `group_id` (and no explicit `event_source_arn`), the client uses a `.fifo` ARN, so a `QueueType.AUTO` app infers FIFO. This is what lets you test [FIFO failure modes](fifo-failure-modes.md) such as `isolate_groups` and `halt_batch`.

!!! note
    SQS exposes FIFO system attributes in PascalCase. The client emits `MessageGroupId` and `MessageDeduplicationId` under `record["attributes"]`, identical to what the event source mapping delivers. Read them through `ctx.fifo_info` rather than parsing `record["attributes"]` yourself.

A `RecordSpec` also accepts `message_id`, `deduplication_id`, and `message_attributes`. When you omit `message_id`, the client assigns `m0`, `m1`, and so on by position.

## Send a malformed body

A raw `str` or `bytes` passes through unchanged, so you can reach the malformed-body path. An unparseable body becomes a reported failure, not a raised exception.

```python
result = client.send("{not json", message_id="bad")
assert result == {"batchItemFailures": [{"itemIdentifier": "bad"}]}
```

The `itemIdentifier` matches the record's `message_id`. Use a known `message_id` so your assertion can name the exact failed record. This is the [partial batch failure](partial-batch-failure.md) protocol: the bad record is reported for redelivery while the rest of the batch succeeds.

!!! note
    `partial_batch_failure` defaults to `True`, so failures surface in `batchItemFailures`. If you construct the app with `partial_batch_failure=False`, a failed record raises `BatchFailedError` instead, and your test should assert on the exception.

## Assert on a partial batch

Mix a valid record and an invalid one to confirm only the bad record is reported.

```python
result = client.send_batch([
    RecordSpec({"type": "order_created", "order_id": "1", "amount": 1}, message_id="ok"),
    RecordSpec("{not json", message_id="bad"),
])
assert result == {"batchItemFailures": [{"itemIdentifier": "bad"}]}
```

## Build events by hand

For cases the client does not cover, `fastsqs.testing` exports `make_record(body, ...)` and `make_event(records)`. Use them to assemble a raw event and pass it straight to `app.handler`.

```python
from fastsqs.testing import make_record, make_event

event = make_event([
    make_record({"type": "order_created", "order_id": "1", "amount": 1}, message_id="a"),
    make_record({"type": "order_created", "order_id": "2", "amount": 2}, message_id="b"),
])
result = app.handler(event, None)
assert result == {"batchItemFailures": []}
```

`make_record` maps snake_case kwargs (`message_id`, `group_id`, `deduplication_id`, `message_attributes`, `event_source_arn`, `attributes`) to the camelCase SQS wire keys. As with the client, passing `group_id` without an explicit `event_source_arn` selects a `.fifo` ARN.

## Run the tests

The client is synchronous, so it works under any test runner. Wrap the calls in plain test functions.

```python
def test_order_created_succeeds():
    result = client.send({"type": "order_created", "order_id": "1", "amount": 5})
    assert result == {"batchItemFailures": []}

def test_malformed_body_is_reported():
    result = client.send("{not json", message_id="bad")
    assert result == {"batchItemFailures": [{"itemIdentifier": "bad"}]}
```

## Related

- [Partial batch failure](partial-batch-failure.md) — what `batchItemFailures` means and how to assert on it.
- [FIFO failure modes](fifo-failure-modes.md) — `isolate_groups` and `halt_batch`, tested with per-record `group_id`.
- [Testing reference](../reference/testing.md) — the full `SQSTestClient`, `RecordSpec`, `make_record`, and `make_event` signatures.
- [fastsqs examples on GitHub](https://github.com/lafayettegabe/fastsqs/tree/main/examples)

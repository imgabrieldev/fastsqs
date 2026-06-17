# Integration tests — real Lambda runtime + SQS events

These tests run the fastsqs handler in the **real AWS Lambda Python runtime**
(`public.ecr.aws/lambda/python:3.13`, which bundles the **Runtime Interface
Emulator**) and inject the SQS event fixtures from [`../events/`](../events) —
exactly the `{"Records":[...]}` envelope the SQS event-source mapping delivers
in production.

**Docker-only. No cloud account, no credentials.** Runs the same locally and in
CI.

## Run

```bash
# fast unit suite only (integration auto-skipped)
pytest

# + Docker integration tests (real Lambda runtime)
pytest --run-integration tests/integration
# or
RUN_INTEGRATION=1 pytest
```

Integration tests auto-skip when Docker is unavailable.

## How it works

```
docker build -f tests/integration/Dockerfile -t fastsqs-rie:test .
docker run -d --rm -p 9000:8080 fastsqs-rie:test
curl -XPOST http://localhost:9000/2015-03-31/functions/function/invocations \
     -d @tests/events/sqs_standard_batch.json
# -> {"batchItemFailures": [{"itemIdentifier": "..."}]}
```

[`rie_app.py`](rie_app.py) is the handler under test (a small fastsqs app that
flips to FIFO mode when the event carries `messageGroupId`).

## Event fixtures

Faithful to the official AWS SQS Lambda event shape
(<https://docs.aws.amazon.com/lambda/latest/dg/with-sqs.html>). Seed new ones
with the SAM CLI (installed):

```bash
sam local generate-event sqs receive-message --body '{"type":"task","task_id":"x"}' \
  > tests/events/my_case.json
```

- `sqs_standard_batch.json` — standard queue, one OK + one failing record.
- `sqs_fifo_batch.json` — FIFO group where the middle record fails (the failure
  + the blocked tail must come back as `batchItemFailures`).

## CI

Any runner with Docker (GitHub Actions `ubuntu-latest`, Bitbucket with the
Docker service, etc.):

```yaml
- run: pip install -e . pytest
- run: pytest --run-integration
```

## Next layer (optional): LocalStack — real SQS + event-source mapping

The RIE approach injects the event directly. To also exercise the **SQS → Lambda
wiring** (send a message to a real queue and have the ESM trigger the function),
run **LocalStack community** (open-source Docker image, free, offline):

```bash
docker run --rm -p 4566:4566 localstack/localstack
# create queue + deploy this image as a Lambda + create an event-source mapping,
# then: awslocal sqs send-message ...  and assert the handler ran.
```

This is heavier (deploy + poll) and validates the queue/ESM rather than the
router/middleware logic the unit + RIE tests already cover.

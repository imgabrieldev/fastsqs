"""Minimal fastsqs Lambda handler used by the Docker RIE integration test.

Runs inside the real AWS Lambda Python runtime image
(public.ecr.aws/lambda/python). The RIE invoke endpoint receives a synthetic
SQS event ({"Records":[...]}) exactly as the SQS event-source mapping delivers
it in production, and `lambda_handler` returns the partial-batch-failure result.
"""

from fastsqs import FastSQS, SQSEvent


class Task(SQSEvent):
    task_id: str


app = FastSQS()  # QueueType.AUTO infers FIFO from a .fifo event-source ARN


@app.route(Task)
async def handle(msg: Task):
    # task_id == "boom" fails the record so it shows up in batchItemFailures.
    if msg.task_id == "boom":
        raise ValueError(f"boom on {msg.task_id}")
    return {"ok": msg.task_id}


def lambda_handler(event, context):
    # FIFO fixtures carry messageGroupId but no real ARN; stamp a .fifo
    # event-source ARN so QueueType.AUTO exercises the FIFO ordering path.
    records = event.get("Records", [])
    if any((r.get("attributes", {}) or {}).get("messageGroupId") for r in records):
        for r in records:
            r.setdefault(
                "eventSourceARN", "arn:aws:sqs:us-east-1:000000000000:rie.fifo"
            )
    return app.handler(event, context)

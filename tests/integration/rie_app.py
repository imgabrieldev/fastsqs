"""Minimal fastsqs Lambda handler used by the Docker RIE integration test.

Runs inside the real AWS Lambda Python runtime image
(public.ecr.aws/lambda/python). The RIE invoke endpoint receives a synthetic
SQS event ({"Records":[...]}) exactly as the SQS event-source mapping delivers
it in production, and `lambda_handler` returns the partial-batch-failure result.
"""

from fastsqs import FastSQS, SQSEvent, QueueType


class Task(SQSEvent):
    task_id: str


app = FastSQS()


@app.route(Task)
async def handle(msg: Task):
    # task_id == "boom" fails the record so it shows up in batchItemFailures.
    if msg.task_id == "boom":
        raise ValueError(f"boom on {msg.task_id}")
    return {"ok": msg.task_id}


def lambda_handler(event, context):
    # FIFO fixtures carry messageGroupId; flip the queue type so the FIFO
    # ordering path is exercised when those events are injected.
    records = event.get("Records", [])
    is_fifo = any(
        (r.get("attributes", {}) or {}).get("messageGroupId") for r in records
    )
    app.set_queue_type(QueueType.FIFO if is_fifo else QueueType.STANDARD)
    return app.handler(event, context)

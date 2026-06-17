"""Handler deployed to a real Lambda by the Tier 2 e2e harness (conftest.py).

Routes Task events; fails task ids starting with "boom"; sleeps for
"sleep-<n>" (to exercise Lambda timeout). Queue type is inferred from the real
event-source ARN (QueueType.AUTO). Returns ReportBatchItemFailures.
"""

import asyncio

from fastsqs import FastSQS, SQSEvent


class Task(SQSEvent):
    task_id: str


app = FastSQS()  # QueueType.AUTO infers FIFO from the .fifo event-source ARN


@app.route(Task)
async def handle(msg: Task):
    if msg.task_id.startswith("sleep-"):
        await asyncio.sleep(int(msg.task_id.split("-", 1)[1]))
    if msg.task_id.startswith("boom"):
        raise ValueError(f"boom {msg.task_id}")
    return {"ok": msg.task_id}


def lambda_handler(event, context):
    return app.handler(event, context)

"""Handler deployed to a real Lambda by the Tier 2 e2e harness (conftest.py).

Routes Task events; fails task ids starting with "boom"; sleeps for
"sleep-<n>" (to exercise Lambda timeout). Flips to FIFO mode when records carry
a messageGroupId. Returns ReportBatchItemFailures.
"""

import asyncio

from fastsqs import FastSQS, SQSEvent, QueueType


class Task(SQSEvent):
    task_id: str


app = FastSQS()


@app.route(Task)
async def handle(msg: Task):
    if msg.task_id.startswith("sleep-"):
        await asyncio.sleep(int(msg.task_id.split("-", 1)[1]))
    if msg.task_id.startswith("boom"):
        raise ValueError(f"boom {msg.task_id}")
    return {"ok": msg.task_id}


def lambda_handler(event, context):
    records = event.get("Records", [])
    is_fifo = any((r.get("attributes", {}) or {}).get("messageGroupId") for r in records)
    app.set_queue_type(QueueType.FIFO if is_fifo else QueueType.STANDARD)
    return app.handler(event, context)

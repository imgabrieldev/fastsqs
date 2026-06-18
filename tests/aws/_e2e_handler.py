"""Handler deployed to a real Lambda by the Tier 2 e2e harness (conftest.py).

Drives the v1 feature surface AND exposes a "results echo" side channel so the
e2e tests can observe SUCCESS positively (not only via absence-from-DLQ).

App config is selected by environment variables (one zip, many deployed
functions): FASTSQS_PARTIAL ("0" -> partial_batch_failure=False),
FASTSQS_FIFO_MODE ("halt_batch"), FASTSQS_MAX_CONCURRENT (int), FASTSQS_CORRUPT
("1" -> return a structurally defective batchItemFailures to test the response
contract).

Results echo: if a record carries a "ResultsQueue" messageAttribute, the handler
sends a small JSON receipt (message_id, queue_type, message_group_id, sequence
number, ApproximateReceiveCount, system/message attributes, body length, cold/
warm + invocation_seq + id(app), enter/exit timestamps) to that queue BEFORE the
pass/fail decision. Tests drain their own per-test results queue.

Task id protocol (Task.task_id):
- "boom*"            -> fail
- "sleep-<n>"        -> await n seconds (Lambda timeout tests)
- "di-check"         -> fail unless Depends() injected
- "ctx-std-check"    -> fail unless AUTO->STANDARD and fifo_info is None
- "ctx-fifo-check"   -> fail unless AUTO->FIFO and fifo_info.message_group_id set
- "echo-fail-*"      -> echo, then ALWAYS fail (receive-count progression -> DLQ)
- "flaky-<n>-*"      -> fail while ApproximateReceiveCount < n, then succeed
- "fail-once-*"      -> fail on the first receive, succeed afterwards
- "conc-*"           -> sleep ~2s (concurrency overlap probe)
- anything else      -> succeed
"""

import asyncio
import json
import os
import time

import boto3

from fastsqs import Context, Depends, FastSQS, QueueType, SQSEvent

_PARTIAL = os.environ.get("FASTSQS_PARTIAL", "1") != "0"
_FIFO_MODE = os.environ.get("FASTSQS_FIFO_MODE", "isolate_groups")
_MAX_CONCURRENT = int(os.environ.get("FASTSQS_MAX_CONCURRENT", "10"))
_CORRUPT = os.environ.get("FASTSQS_CORRUPT") == "1"

_sqs = None  # lazy boto3 client for results echo (boto3 is in the Lambda runtime)

# Module-scope markers persist across warm invocations (cold-start / reuse tests).
_STATE = {"invocation_seq": 0, "cold": True}


def _client():
    global _sqs
    if _sqs is None:
        _sqs = boto3.client("sqs")
    return _sqs


class Task(SQSEvent):
    task_id: str


class Order(SQSEvent):
    order_id: str


def get_marker() -> str:
    return "INJECTED"


app = FastSQS(
    partial_batch_failure=_PARTIAL,
    fifo_failure_mode=_FIFO_MODE,
    max_concurrent_messages=_MAX_CONCURRENT,
)


def _echo(ctx: Context, identifier: str, enter_ts: float, exit_ts: float, extra=None):
    """Send a receipt to the per-record ResultsQueue messageAttribute, if present."""
    attrs = ctx.record.get("messageAttributes") or {}
    rq = attrs.get("ResultsQueue")
    if not rq:
        return
    url = rq.get("stringValue") or rq.get("StringValue")
    if not url:
        return
    sys_attrs = ctx.record.get("attributes") or {}
    receipt = {
        "identifier": identifier,
        "message_id": ctx.message_id,
        "queue_type": ctx.queue_type.value,
        "message_group_id": ctx.fifo_info.message_group_id if ctx.fifo_info else None,
        "message_dedup_id": ctx.fifo_info.message_deduplication_id if ctx.fifo_info else None,
        "sequence_number": sys_attrs.get("SequenceNumber"),
        "approx_receive_count": sys_attrs.get("ApproximateReceiveCount"),
        "sent_timestamp": sys_attrs.get("SentTimestamp"),
        "first_receive_ts": sys_attrs.get("ApproximateFirstReceiveTimestamp"),
        "sender_id": sys_attrs.get("SenderId"),
        "message_attributes": {k: v for k, v in attrs.items() if k != "ResultsQueue"},
        "body_len": len(ctx.record.get("body") or ""),
        "aws_request_id": getattr(ctx.lambda_context, "aws_request_id", None),
        "app_id": id(app),
        "invocation_seq": _STATE["invocation_seq"],
        "cold": extra.pop("cold", None) if extra else None,
        "enter_ts": enter_ts,
        "exit_ts": exit_ts,
    }
    if extra:
        receipt.update(extra)
    try:
        _client().send_message(QueueUrl=url, MessageBody=json.dumps(receipt))
    except Exception:
        pass  # never let an echo failure change the record's pass/fail outcome


def _recv_count(ctx: Context) -> int:
    try:
        return int((ctx.record.get("attributes") or {}).get("ApproximateReceiveCount", "1"))
    except (TypeError, ValueError):
        return 1


@app.route(Task)
async def handle(msg: Task, ctx: Context, marker: str = Depends(get_marker)):
    enter_ts = time.time()
    tid = msg.task_id

    if tid.startswith("sleep-"):
        await asyncio.sleep(int(tid.split("-", 1)[1]))
    if tid.startswith("conc-"):
        await asyncio.sleep(2)  # create overlap so the semaphore bound is observable

    exit_ts = time.time()
    _echo(ctx, tid, enter_ts, exit_ts, extra={"cold": _STATE["cold_for_invocation"]})

    if tid == "di-check" and marker != "INJECTED":
        raise ValueError("DI did not resolve in the real runtime")
    if tid == "ctx-std-check":
        if ctx.queue_type != QueueType.STANDARD or ctx.fifo_info is not None:
            raise ValueError(f"expected STANDARD ctx, got {ctx.queue_type}/{ctx.fifo_info}")
    if tid == "ctx-fifo-check":
        if ctx.queue_type != QueueType.FIFO or not (ctx.fifo_info and ctx.fifo_info.message_group_id):
            raise ValueError(f"expected FIFO ctx, got {ctx.queue_type}/{ctx.fifo_info}")

    if tid.startswith("echo-fail-"):
        raise ValueError(f"intentional fail for receive-count probe {tid}")
    if tid.startswith("flaky-"):
        threshold = int(tid.split("-")[1])
        if _recv_count(ctx) < threshold:
            raise ValueError(f"transient failure {tid} (receive {_recv_count(ctx)} < {threshold})")
    if tid.startswith("fail-once"):
        if _recv_count(ctx) == 1:
            raise ValueError(f"fail-once first delivery {tid}")

    if tid.startswith("boom"):
        raise ValueError(f"boom {tid}")
    return {"ok": tid}


@app.route(Order)
async def handle_order(msg: Order, ctx: Context):
    enter_ts = time.time()
    _echo(ctx, msg.order_id, enter_ts, time.time(), extra={"cold": _STATE["cold_for_invocation"]})
    if msg.order_id.startswith("boom"):
        raise ValueError(f"boom order {msg.order_id}")
    return {"ok": msg.order_id}


def lambda_handler(event, context):
    _STATE["invocation_seq"] += 1
    _STATE["cold_for_invocation"] = _STATE["cold"]
    _STATE["cold"] = False
    if _CORRUPT:
        # Structurally defective response: an empty itemIdentifier makes the ESM
        # treat the WHOLE batch as failed regardless of actual outcomes.
        return {"batchItemFailures": [{"itemIdentifier": ""}]}
    return app.handler(event, context)

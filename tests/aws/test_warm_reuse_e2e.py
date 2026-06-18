"""Real-AWS e2e: module-scope app object reuse across warm Lambda invocations
(opt-in: ``pytest --run-aws``).

This can't be proven in-process: it asserts that the module-level
``app = FastSQS()`` object built at import time survives across warm invocations
of a deployed python3.13 Lambda, and that the runtime really does reuse one
execution environment for spaced, low-concurrency deliveries. The deployed
handler keeps module-scope markers (``_STATE["invocation_seq"]`` bumped once per
``lambda_handler`` call, and a ``cold`` flag flipped False after the first ever
invocation) and echoes them — together with ``id(app)`` — into each results-queue
receipt BEFORE the pass/fail decision.

We fire N plain echo messages SPACED apart (each sent one-at-a-time with a small
gap so it lands in its OWN invocation rather than co-batching, ``invocation_seq``
advancing one per message) and drain the receipts:
- ``app_id`` is identical across every receipt: the same module-scope FastSQS()
  object served every warm invocation (guards the per-request-state-on-app leak
  surface — fastsqs keeps per-record scratch in ctx.state, not on ``app``).
- ``invocation_seq`` is strictly increasing across the spaced sends (warm reuse,
  not N cold starts each resetting the counter).
- at most ONE receipt has ``cold == True`` (a single cold start for the env).
- none of the success messages dead-letter; a paired poison control does.

If the platform happens to spin a second execution environment, we relax to the
weaker-but-still-meaningful invariant: ``app_id`` is stable WITHIN each container
and ``invocation_seq`` is monotonic with at most one cold start PER container —
i.e. each container still reused one module-scope FastSQS() across its warm
invocations.

A green run also implicitly proves the linux-x86_64 pydantic/fast-depends/fastsqs
deployment zip imports cleanly at cold start (any import error would fail every
message into the DLQ). Harness in conftest.py.
"""

import json
import time
import uuid

import pytest

pytestmark = pytest.mark.aws


@pytest.mark.slow
def test_module_scope_app_reused_across_warm_invocations(aws, pipeline, drain, drain_full):
    """N spaced echo messages land on warm reuses of one execution environment:
    every receipt reports the same ``id(app)`` (module-scope FastSQS() persists),
    ``invocation_seq`` advances monotonically, and at most one receipt is a cold
    start. A paired poison control dead-letters; none of the echo messages do."""
    sqs = aws["sqs"]
    # Default pipeline (max_concurrent=10 on the fn). We serialize sends with a small
    # gap so each message lands in its OWN invocation on the SAME warm sandbox ->
    # invocation_seq advances by one per message rather than several records sharing
    # one invocation. This is the OPPOSITE of the batch tests: no co-batching here.
    main_url, dlq_url, results_url = pipeline(
        fifo=False, max_receive_count=2, visibility=10, results=True
    )

    run = uuid.uuid4().hex[:8]
    n = 5
    ids = [f"warm-{run}-{i}" for i in range(n)]

    def send(task_id):
        sqs.send_message(
            QueueUrl=main_url,
            MessageBody=json.dumps({"type": "task", "task_id": task_id}),
            MessageAttributes={
                "ResultsQueue": {"DataType": "String", "StringValue": results_url}
            },
        )

    # Spaced sends: ~2s apart, comfortably past the ~plain handler runtime, so each
    # lands on a distinct (warm) invocation of the reused execution environment
    # instead of co-batching into a single invocation.
    for tid in ids:
        send(tid)
        time.sleep(2)

    # Poison control: anchors the DLQ path and proves the redrive machinery works.
    send("boom-control")

    # Collect ALL echo receipts for this run in one generous window. min_count is set
    # absurdly high so drain waits the whole timeout (it never reaches the count), and
    # we filter to this run's ids — at-least-once may duplicate a receipt, so we keep
    # the FIRST receipt seen per identifier.
    prefix = f"warm-{run}-"
    by_id: dict[str, dict] = {}

    def _keep(m):
        try:
            body = json.loads(m["Body"])
        except (KeyError, ValueError):
            return False
        ident = body.get("identifier")
        if not (isinstance(ident, str) and ident.startswith(prefix)):
            return False
        by_id.setdefault(ident, body)
        return True

    drain_full(results_url, timeout=180, min_count=10_000, predicate=_keep)

    missing = [tid for tid in ids if tid not in by_id]
    assert not missing, f"missing results-queue receipt(s) for {missing}"

    # Receipts in send order (one per echo message, FIRST receipt kept per id).
    receipts = [by_id[tid] for tid in ids]

    # Group receipts by the execution environment that served them. Normally there is
    # exactly one (warm reuse), but the platform may occasionally spin a second.
    by_app: dict[int, list[dict]] = {}
    for r in receipts:
        by_app.setdefault(int(r["app_id"]), []).append(r)

    app_ids = set(by_app)
    seqs = [int(r["invocation_seq"]) for r in receipts]

    if len(app_ids) == 1:
        # Strong invariant: one module-scope FastSQS() object served every warm
        # invocation (no per-request rebuild, no app-scope state leak).
        assert seqs == sorted(seqs) and len(set(seqs)) == len(seqs), (
            f"invocation_seq must increase monotonically across warm reuses, got {seqs}"
        )
        cold_count = sum(1 for r in receipts if r.get("cold") is True)
        assert cold_count <= 1, f"expected at most one cold start, saw {cold_count}: {seqs}"
    else:
        # Relaxed invariant (a second container appeared): WITHIN each container the
        # same id(app) was reused, invocation_seq is monotonic, and at most one cold
        # start happened per container. Each container still reused one FastSQS().
        for app_id, group in by_app.items():
            g_seqs = [int(r["invocation_seq"]) for r in group]
            assert g_seqs == sorted(g_seqs) and len(set(g_seqs)) == len(g_seqs), (
                f"invocation_seq must be monotonic within app_id {app_id}, got {g_seqs}"
            )
            g_cold = sum(1 for r in group if r.get("cold") is True)
            assert g_cold <= 1, (
                f"expected at most one cold start in app_id {app_id}, saw {g_cold}"
            )

    # DLQ: only the poison control redrives; none of the echo messages do.
    moved = drain(dlq_url, timeout=180, min_count=1)
    dlq_ids = {json.loads(b).get("task_id") for b in moved}
    assert "boom-control" in dlq_ids, "poison control must reach the DLQ"
    assert not (set(ids) & dlq_ids), f"echo messages must not dead-letter, saw {dlq_ids}"

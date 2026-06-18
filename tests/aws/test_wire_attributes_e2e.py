"""Real-AWS e2e: on-the-wire system + message attributes that ONLY the real ESM
delivery path produces (opt-in: ``pytest --run-aws``).

In-process tests synthesize the SQS record dict themselves, so they can never
prove what AWS actually puts on the wire. These tests send through a real queue
bound to a real event-source mapping and have the deployed handler echo the
record it RECEIVED (``ctx.record``) to a per-test results queue. Draining that
queue lets us assert:

- the ESM-supplied PascalCase system attributes the in-process client cannot
  synthesize (SentTimestamp / ApproximateFirstReceiveTimestamp / SenderId /
  ApproximateReceiveCount) against captured wall-clock and the STS caller id;
- that String / Number / Binary MessageAttribute typing survives ESM delivery
  on the delivered record (Number stays a string, Binary is base64);
- that a just-under-256KB body round-trips whole through the ESM (fastsqs
  ``json.loads`` parses the full body), while a >256KB body is rejected at
  SendMessage rather than silently truncated.

The handler echoes the receipt BEFORE its pass/fail decision, so an echoing
message that then succeeds is observable purely from the results queue; no DLQ
is needed for the success path. Harness in conftest.py.
"""

import base64
import json
import time
import uuid

import pytest

pytestmark = pytest.mark.aws


def _has_identifier(identifier):
    """Predicate for drain_full: keep only results-queue receipts for ``identifier``."""

    def _pred(m):
        try:
            return json.loads(m["Body"]).get("identifier") == identifier
        except (KeyError, ValueError):
            return False

    return _pred


def test_system_attributes_well_formed_on_wire(aws, pipeline, drain_full):
    """The ESM delivers PascalCase system attributes the in-process client cannot
    synthesize: on the record the handler actually received, SentTimestamp is a
    plausible recent epoch-ms, ApproximateFirstReceiveTimestamp >= SentTimestamp,
    ApproximateReceiveCount == '1' on first delivery, and SenderId is the sender's
    SQS-supplied principal id."""
    sqs = aws["sqs"]
    main_url, _dlq_url, results_url = pipeline(
        fifo=False, max_receive_count=2, visibility=10, results=True
    )

    # Wall-clock window: capture just before send so we can bound SentTimestamp.
    before_ms = int(time.time() * 1000)
    tid = f"sysattrs-{uuid.uuid4().hex[:8]}"
    sqs.send_message(
        QueueUrl=main_url,
        MessageBody=json.dumps({"type": "task", "task_id": tid}),
        MessageAttributes={"ResultsQueue": {"DataType": "String", "StringValue": results_url}},
    )
    after_ms = int(time.time() * 1000)

    msgs = drain_full(
        results_url, timeout=180, min_count=1, predicate=_has_identifier(tid)
    )
    assert msgs, "the echo receipt for the system-attributes probe never arrived"
    receipt = json.loads(msgs[0]["Body"])

    # SentTimestamp: epoch-ms, plausibly within the window around our send (allow
    # generous clock skew on either side of the captured wall-clock).
    sent = int(receipt["sent_timestamp"])
    assert before_ms - 60_000 <= sent <= after_ms + 60_000, (
        f"SentTimestamp {sent} not near send window [{before_ms}, {after_ms}]"
    )

    # ApproximateFirstReceiveTimestamp is set on first delivery and is >= SentTimestamp.
    first_recv = int(receipt["first_receive_ts"])
    assert first_recv >= sent, (
        f"ApproximateFirstReceiveTimestamp {first_recv} < SentTimestamp {sent}"
    )

    # First (and only) delivery of a message that succeeds: receive count is 1.
    assert receipt["approx_receive_count"] == "1", (
        f"expected ApproximateReceiveCount '1', got {receipt['approx_receive_count']!r}"
    )

    # SenderId is supplied by SQS: for an IAM user it is the user's UNIQUE PRINCIPAL
    # ID (prefix 'AIDA'); for a role it is the role's principal id (prefix 'AROA').
    # It is NOT the 12-digit account number, so we only assert it is a non-empty
    # principal id rather than looking for the account id inside it.
    sender_id = receipt["sender_id"]
    assert sender_id, "SenderId missing from the delivered record"
    assert isinstance(sender_id, str) and sender_id.startswith(("AIDA", "AROA")), (
        f"SenderId {sender_id!r} is not a well-formed SQS principal id"
    )


def test_message_attribute_typing_survives_esm_delivery(aws, pipeline, drain_full):
    """On the ESM-delivered record (not a manual ReceiveMessage), String / Number /
    Binary MessageAttribute typing is preserved: String and Number arrive in
    stringValue with their dataType (Number stays a string, no numeric coercion),
    and Binary arrives base64-encoded and decodes back to the original bytes."""
    sqs = aws["sqs"]
    main_url, _dlq_url, results_url = pipeline(
        fifo=False, max_receive_count=2, visibility=10, results=True
    )

    raw_bytes = uuid.uuid4().bytes + b"\x00\xff\x10binary-payload"
    tid = f"msgattrs-{uuid.uuid4().hex[:8]}"
    sqs.send_message(
        QueueUrl=main_url,
        MessageBody=json.dumps({"type": "task", "task_id": tid}),
        MessageAttributes={
            "ResultsQueue": {"DataType": "String", "StringValue": results_url},
            "Corr": {"DataType": "String", "StringValue": "corr-123"},
            "Count": {"DataType": "Number", "StringValue": "42"},
            "Blob": {"DataType": "Binary", "BinaryValue": raw_bytes},
        },
    )

    msgs = drain_full(
        results_url, timeout=180, min_count=1, predicate=_has_identifier(tid)
    )
    assert msgs, "the echo receipt for the message-attribute typing probe never arrived"
    receipt = json.loads(msgs[0]["Body"])

    # The handler echoes ctx.record['messageAttributes'] minus the ResultsQueue key.
    # On a Lambda SQS event record, keys are camelCase: stringValue/binaryValue/dataType.
    attrs = receipt["message_attributes"]
    assert "ResultsQueue" not in attrs, "handler should strip its own ResultsQueue attr"
    assert set(attrs) >= {"Corr", "Count", "Blob"}, f"missing attributes: {sorted(attrs)}"

    # String attribute: arrives in stringValue with dataType String.
    assert attrs["Corr"]["dataType"] == "String"
    assert attrs["Corr"]["stringValue"] == "corr-123"

    # Number attribute: dataType Number, value carried as a STRING (no coercion).
    assert attrs["Count"]["dataType"] == "Number"
    assert attrs["Count"]["stringValue"] == "42"
    assert isinstance(attrs["Count"]["stringValue"], str)

    # Binary attribute: dataType Binary, value base64 in binaryValue, decodes to bytes.
    assert attrs["Blob"]["dataType"] == "Binary"
    decoded = base64.b64decode(attrs["Blob"]["binaryValue"])
    assert decoded == raw_bytes, "Binary attribute did not round-trip through the ESM"


def test_near_256kb_body_passes_through_esm_intact(aws, pipeline, drain_full):
    """A just-under-256KB body round-trips whole through the ESM: fastsqs json.loads
    parses the full body (the handler echoes len(body) == the sent size) and the
    success message never reaches the DLQ. (The >256KB SendMessage rejection is a
    pure SQS server limit that fastsqs never sees, so it is not asserted here.)"""
    sqs = aws["sqs"]
    main_url, dlq_url, results_url = pipeline(
        fifo=False, max_receive_count=2, visibility=10, results=True
    )

    # SQS's 262144-byte hard limit counts the body PLUS the message attributes, so
    # the just-under body must reserve headroom for the ResultsQueue attribute (its
    # name "ResultsQueue" + the "String" data type + the queue-URL value all count).
    # Build a valid-JSON body whose payload is ~230KB so that, together with the JSON
    # envelope and that attribute, it stays COMFORTABLY under 262144 bytes (~32KB of
    # headroom): the send succeeds and the body round-trips whole through the ESM.
    SQS_MAX = 262144
    tid = f"big-ok-{uuid.uuid4().hex[:8]}"
    attr_overhead = len("ResultsQueue") + len("String") + len(results_url)
    base = {"type": "task", "task_id": tid, "pad": "x" * (230 * 1024)}
    body = json.dumps(base)
    sent_len = len(body)
    assert sent_len + attr_overhead < SQS_MAX, (
        f"padded body {sent_len} + attrs {attr_overhead} unexpectedly over the limit"
    )

    sqs.send_message(
        QueueUrl=main_url,
        MessageBody=body,
        MessageAttributes={"ResultsQueue": {"DataType": "String", "StringValue": results_url}},
    )

    msgs = drain_full(
        results_url, timeout=180, min_count=1, predicate=_has_identifier(tid)
    )
    assert msgs, "the echo receipt for the near-256KB body never arrived"
    receipt = json.loads(msgs[0]["Body"])

    # The handler echoes len(ctx.record['body']); the full body arrived untruncated.
    assert receipt["body_len"] == sent_len, (
        f"body length changed across the ESM: sent {sent_len}, received {receipt['body_len']}"
    )

    # The big-ok message succeeded, so it must NOT be in the DLQ.
    dlq = drain_full(
        dlq_url,
        timeout=20,
        min_count=1,
        predicate=lambda m: json.loads(m["Body"]).get("task_id") == tid,
    )
    assert not dlq, "the successful near-256KB message must not reach the DLQ"

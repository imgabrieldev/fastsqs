"""Body-parsing edge cases & realistic SQS-attribute passthrough.

Covers the body-decode branches of ``_handle_record``:

- valid JSON that is NOT an object (list / scalar / string literal) hits the
  distinct ``isinstance(payload, dict)`` branch -> InvalidMessageError
  ("Message body must be a JSON object"), separate from the JSONDecodeError
  branch exercised by truly non-JSON bodies elsewhere;
- an empty/whitespace-only body short-circuits to ``{}`` (the ``if body_str``
  guard), so it routes as an empty payload (no discriminator).

It also pins the MEMORY contract that redelivered messages route identically
(ApproximateReceiveCount / SentTimestamp are plain attributes the router never
special-cases) and that a single malformed record fails alone in a standard
batch (poison-pill isolation), without crashing the whole batch.
"""

import pytest

from fastsqs import FastSQS, SQSEvent
from fastsqs.testing import RecordSpec, SQSTestClient


class Task(SQSEvent):
    task_id: str = "x"


# ---- non-dict JSON bodies hit the isinstance(payload, dict) branch ----
# (distinct from the JSONDecodeError branch: these ARE valid JSON, just not an
# object, so InvalidMessageError carries "must be a JSON object")

@pytest.mark.parametrize("body,mid", [
    ("[1,2,3]", "arr"),            # valid JSON list, not a dict
    ("42", "num"),                # valid JSON number scalar
    ('"just a string"', "str"),   # valid JSON string literal
    ("true", "bool"),             # valid JSON boolean scalar
    ("null", "nul"),              # valid JSON null
])
def test_non_dict_json_body_fails_record(body, mid):
    app = FastSQS()

    @app.route(Task)
    async def h(msg: Task):  # pragma: no cover - must never run for non-dict body
        raise AssertionError("handler should not run for a non-dict body")

    r = SQSTestClient(app).send(body, message_id=mid)
    assert r == {"batchItemFailures": [{"itemIdentifier": mid}]}


def test_json_array_body_fails_record():
    app = FastSQS()

    @app.route(Task)
    async def h(msg: Task):  # pragma: no cover - body is not a dict
        raise AssertionError("handler should not run")

    r = SQSTestClient(app).send("[1,2,3]", message_id="arr")
    assert r == {"batchItemFailures": [{"itemIdentifier": "arr"}]}


def test_json_scalar_number_body_fails_record():
    app = FastSQS()

    @app.route(Task)
    async def h(msg: Task):  # pragma: no cover - body is not a dict
        raise AssertionError("handler should not run")

    r = SQSTestClient(app).send("42", message_id="num")
    assert r == {"batchItemFailures": [{"itemIdentifier": "num"}]}


def test_json_string_literal_body_fails_record():
    app = FastSQS()

    @app.route(Task)
    async def h(msg: Task):  # pragma: no cover - body is not a dict
        raise AssertionError("handler should not run")

    r = SQSTestClient(app).send('"just a string"', message_id="str")
    assert r == {"batchItemFailures": [{"itemIdentifier": "str"}]}


def test_non_dict_branch_is_invalid_message_error():
    """Direct unit call: the non-dict branch raises InvalidMessageError with the
    'must be a JSON object' message (NOT a JSONDecodeError-derived message)."""
    import asyncio

    from fastsqs.exceptions import InvalidMessageError

    app = FastSQS()

    @app.route(Task)
    async def h(msg: Task):  # pragma: no cover - body is not a dict
        raise AssertionError("handler should not run")

    record = {"messageId": "x", "body": "[1,2,3]"}
    with pytest.raises(InvalidMessageError) as exc:
        asyncio.run(app._handle_record(record, None))
    assert "must be a JSON object" in str(exc.value)
    # this branch is NOT a JSON decode failure -> no chained JSONDecodeError
    assert exc.value.__cause__ is None


# ---- empty / whitespace bodies short-circuit to {} ----

def test_empty_string_body_routes_as_empty_payload_to_default():
    """Empty body -> ``{}`` (the ``if body_str`` guard). With no discriminator
    the default handler catches it; the record succeeds."""
    app = FastSQS()
    seen = []

    @app.route(Task)
    async def h(msg: Task):  # pragma: no cover - empty payload has no type
        seen.append("task")

    @app.default()
    async def d(msg, ctx):
        seen.append("default")

    r = SQSTestClient(app).send("", message_id="e1")
    assert r == {"batchItemFailures": []}
    assert seen == ["default"]


def test_empty_string_body_without_default_fails_route_not_found():
    """Empty body -> ``{}`` -> no discriminator -> no match and no default
    handler -> RouteNotFoundError fails the record."""
    app = FastSQS()

    @app.route(Task)
    async def h(msg: Task):  # pragma: no cover - empty payload has no type
        raise AssertionError("handler should not run")

    r = SQSTestClient(app).send("", message_id="e2")
    assert r == {"batchItemFailures": [{"itemIdentifier": "e2"}]}


def test_whitespace_only_body_is_truthy_and_fails_as_invalid_json():
    """A whitespace-only body is a non-empty (truthy) string, so it is NOT
    short-circuited to ``{}`` -- it is fed to json.loads and fails with a
    JSONDecodeError -> InvalidMessageError. (Adjacent edge: documents that only
    a *falsy* body becomes ``{}``; whitespace does not.)"""
    app = FastSQS()

    @app.route(Task)
    async def h(msg: Task):  # pragma: no cover - body is not valid JSON
        raise AssertionError("handler should not run")

    r = SQSTestClient(app).send("   ", message_id="ws")
    assert r == {"batchItemFailures": [{"itemIdentifier": "ws"}]}


# ---- ApproximateReceiveCount / SentTimestamp passthrough (redelivery) ----
# MEMORY contract: redelivered messages route identically; the router never
# special-cases these attributes. Consumers read them off ctx.record.

def test_redelivered_message_with_receive_count_routes_identically():
    app = FastSQS()
    seen = {}

    @app.route(Task)
    async def h(msg: Task, ctx):
        seen["task_id"] = msg.task_id
        seen["recv"] = ctx.record["attributes"]["ApproximateReceiveCount"]

    r = SQSTestClient(app).send(
        {"type": "task", "task_id": "1"},
        message_id="r1",
        attributes={"ApproximateReceiveCount": "3"},
    )
    assert r == {"batchItemFailures": []}
    # routed/validated normally AND the handler observes the raw count
    assert seen == {"task_id": "1", "recv": "3"}


def test_sent_timestamp_attribute_readable_in_handler():
    app = FastSQS()
    seen = {}

    @app.route(Task)
    async def h(msg: Task, ctx):
        seen["sent"] = ctx.record["attributes"]["SentTimestamp"]

    r = SQSTestClient(app).send(
        {"type": "task", "task_id": "1"},
        message_id="s1",
        attributes={"SentTimestamp": "1700000000000"},
    )
    # processes normally; the router does not block on staleness -- a consumer
    # could self-skip stale work, but that is the consumer's call.
    assert r == {"batchItemFailures": []}
    assert seen == {"sent": "1700000000000"}


# ---- poison-pill isolation on a standard batch ----

def test_poison_pill_one_malformed_record_fails_alone_in_batch():
    """A standard batch with one good record and one non-JSON ('poison') record:
    only the poison id is reported, the good record succeeds, no batch crash."""
    app = FastSQS()
    processed = []

    @app.route(Task)
    async def h(msg: Task):
        processed.append(msg.task_id)

    r = SQSTestClient(app).send_batch([
        RecordSpec({"type": "task", "task_id": "1"}, message_id="good"),
        RecordSpec("not json", message_id="poison"),
    ])
    assert r == {"batchItemFailures": [{"itemIdentifier": "poison"}]}
    assert processed == ["1"]  # the good record ran


def test_mixed_routable_and_unroutable_types_isolate_failure():
    """Standard batch: a routable record runs; an unknown-type record with no
    default handler fails with RouteNotFoundError. Only the unroutable id is
    reported."""
    app = FastSQS()
    processed = []

    @app.route(Task)
    async def h(msg: Task):
        processed.append(msg.task_id)

    r = SQSTestClient(app).send_batch([
        RecordSpec({"type": "task", "task_id": "1"}, message_id="ok"),
        RecordSpec({"type": "nope", "task_id": "2"}, message_id="bad"),
    ])
    assert r == {"batchItemFailures": [{"itemIdentifier": "bad"}]}
    assert processed == ["1"]  # the routable handler ran

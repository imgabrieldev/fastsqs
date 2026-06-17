"""Public surface of the shipped test client.

``fastsqs.testing`` (SQSTestClient / RecordSpec / make_record / make_event) is
public API but elsewhere it is only *used* to drive other tests, never
asserted. These tests pin its own behaviour: body encoding (dict -> JSON;
str/bytes verbatim, reaching InvalidMessageError), group_id -> ``.fifo`` ARN
inference (and explicit-ARN override), message_attributes/attributes
passthrough, ``context`` becoming ``ctx.lambda_context``, send_batch id
assignment (auto ``m{index}`` vs per-spec, mixed with bare bodies), RecordSpec
defaults, and the raw make_record/make_event builders. No AWS.
"""

import json

import pytest

from fastsqs import Context, FastSQS, SQSEvent
from fastsqs.testing import RecordSpec, SQSTestClient, make_event, make_record


class Task(SQSEvent):
    task_id: str = "x"


def _app_with_route():
    app = FastSQS()

    @app.route(Task)
    async def handle(msg: Task):
        pass

    return app


def _failing_app():
    app = FastSQS()

    @app.route(Task)
    async def handle(msg: Task):
        raise ValueError("boom")

    return app


# ---- body encoding -> InvalidMessageError on non-JSON str/bytes ----

def test_client_send_str_body_hits_invalid_message():
    # A raw str is passed verbatim; json.loads fails -> InvalidMessageError ->
    # the record is reported as a batch-item failure.
    app = _app_with_route()
    r = SQSTestClient(app).send("not json", message_id="s1")
    assert r == {"batchItemFailures": [{"itemIdentifier": "s1"}]}


def test_client_send_bytes_body_hits_invalid_message():
    # bytes are decoded then fail JSON parse, same failure path.
    app = _app_with_route()
    r = SQSTestClient(app).send(b"also not json", message_id="b1")
    assert r == {"batchItemFailures": [{"itemIdentifier": "b1"}]}


def test_client_send_dict_body_is_json_encoded():
    rec = make_record({"type": "task"})
    assert isinstance(rec["body"], str)
    assert json.loads(rec["body"]) == {"type": "task"}


# ---- group_id -> .fifo ARN inference, and explicit-ARN override ----

def test_client_group_id_sets_fifo_arn():
    rec = make_record({"type": "t"}, group_id="g1")
    assert rec["eventSourceARN"].endswith(".fifo")
    assert rec["attributes"]["messageGroupId"] == "g1"


def test_client_explicit_event_source_arn_overrides_group_id_default():
    rec = make_record({"type": "t"}, group_id="g1", event_source_arn="arn:custom")
    assert rec["eventSourceARN"] == "arn:custom"
    # group_id is still recorded as an attribute even when the ARN is overridden.
    assert rec["attributes"]["messageGroupId"] == "g1"


# ---- message_attributes / attributes passthrough onto the wire keys ----

def test_client_message_attributes_and_attributes_passthrough():
    rec = make_record(
        {"type": "t"},
        message_attributes={"k": {"StringValue": "v"}},
        attributes={"X": "1"},
    )
    assert rec["messageAttributes"] == {"k": {"StringValue": "v"}}
    assert rec["attributes"]["X"] == "1"


# ---- context flows through as ctx.lambda_context AND the `context` kwarg ----

def test_client_context_passed_as_lambda_context():
    app = FastSQS()
    captured = {}
    sentinel = object()

    @app.route(Task)
    async def handle(msg: Task, ctx: Context, context):
        captured["lambda_context"] = ctx.lambda_context
        captured["context_kwarg"] = context

    SQSTestClient(app).send({"type": "task", "task_id": "1"}, context=sentinel)
    assert captured["lambda_context"] is sentinel
    assert captured["context_kwarg"] is sentinel


# ---- send_batch id assignment ----

def test_send_batch_autogenerates_message_ids():
    # Bare bodies get index-based auto ids m0, m1; force both to fail and assert
    # the failure ids preserve arrival order.
    app = _failing_app()
    r = SQSTestClient(app).send_batch(
        [{"type": "task", "task_id": "0"}, {"type": "task", "task_id": "1"}]
    )
    assert [f["itemIdentifier"] for f in r["batchItemFailures"]] == ["m0", "m1"]


def test_send_batch_respects_per_spec_message_id():
    app = _failing_app()
    r = SQSTestClient(app).send_batch(
        [RecordSpec({"type": "task", "task_id": "0"}, message_id="custom-7")]
    )
    assert r["batchItemFailures"] == [{"itemIdentifier": "custom-7"}]


def test_send_batch_mixes_recordspec_and_bare_body():
    # The spec keeps its explicit id; the bare body (index 1) gets auto id m1.
    app = _failing_app()
    r = SQSTestClient(app).send_batch(
        [
            RecordSpec({"type": "task", "task_id": "0"}, message_id="r0"),
            {"type": "task", "task_id": "1"},
        ]
    )
    failed = {f["itemIdentifier"] for f in r["batchItemFailures"]}
    assert failed == {"r0", "m1"}


# ---- raw builders + RecordSpec defaults ----

def test_make_event_wraps_records_and_recordspec_defaults():
    r1 = make_record({"type": "task", "task_id": "1"})
    r2 = make_record({"type": "task", "task_id": "2"})
    assert make_event([r1, r2]) == {"Records": [r1, r2]}

    spec = RecordSpec(body="x")
    assert spec.message_id is None
    assert spec.group_id is None
    assert spec.deduplication_id is None
    assert spec.message_attributes is None


@pytest.mark.parametrize(
    "kwargs,wire_key,expected",
    [
        ({"message_id": "mid-9"}, "messageId", "mid-9"),
        ({"deduplication_id": "d1"}, "attributes", {"messageDeduplicationId": "d1"}),
    ],
)
def test_make_record_kwargs_map_to_wire_keys(kwargs, wire_key, expected):
    rec = make_record({"type": "t"}, **kwargs)
    if wire_key == "attributes":
        assert rec["attributes"] == expected
    else:
        assert rec[wire_key] == expected


def test_make_record_default_message_id_and_standard_arn():
    rec = make_record({"type": "t"})
    assert rec["messageId"] == "test-1"
    # No group_id -> standard (non-.fifo) ARN and no attributes key.
    assert not rec["eventSourceARN"].endswith(".fifo")
    assert "attributes" not in rec
    assert "messageAttributes" not in rec

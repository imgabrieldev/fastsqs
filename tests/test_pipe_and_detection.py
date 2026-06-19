"""EventBridge Pipes support: ``is_sqs_event`` detection + handler accepting a
bare list of records (the shape a Pipes target receives), not only
``{"Records": [...]}``."""

import json

from fastsqs import FastSQS, SQSEvent, is_sqs_event


class Ping(SQSEvent):
    ping_id: str


def _record(ping_id: str) -> dict:
    return {"messageId": f"m-{ping_id}", "body": json.dumps({"type": "ping", "ping_id": ping_id})}


# --- is_sqs_event -----------------------------------------------------------

def test_is_sqs_event_true_for_list():
    assert is_sqs_event([]) is True
    assert is_sqs_event([{"messageId": "1"}]) is True


def test_is_sqs_event_true_for_records_dict():
    # Presence of the key matters, not truthiness: an empty batch is still SQS.
    assert is_sqs_event({"Records": []}) is True
    assert is_sqs_event({"Records": [{"messageId": "1"}]}) is True


def test_is_sqs_event_false_for_non_sqs():
    # API Gateway proxy event and other dicts have no "Records" key.
    assert is_sqs_event({"requestContext": {}, "httpMethod": "GET"}) is False
    assert is_sqs_event({}) is False
    assert is_sqs_event(None) is False
    assert is_sqs_event("nope") is False
    assert is_sqs_event(123) is False


# --- handler accepts a bare list (EventBridge Pipes) ------------------------

def test_handler_accepts_bare_list():
    seen = []
    app = FastSQS()

    @app.route(Ping)
    async def handle(msg: Ping):
        seen.append(msg.ping_id)

    # A Pipes target gets the batch as a list, not {"Records": [...]}.
    result = app.handler([_record("P1"), _record("P2")], None)

    assert seen == ["P1", "P2"]
    assert result == {"batchItemFailures": []}


def test_handler_list_and_records_are_equivalent():
    app = FastSQS()
    got = []

    @app.route(Ping)
    async def handle(msg: Ping):
        got.append(msg.ping_id)

    rec = _record("X")
    assert app.handler([rec], None) == {"batchItemFailures": []}
    assert app.handler({"Records": [rec]}, None) == {"batchItemFailures": []}
    assert got == ["X", "X"]


def test_handler_empty_list_reports_no_failures():
    app = FastSQS()
    assert app.handler([], None) == {"batchItemFailures": []}


# --- regression: malformed batches must never poison the whole batch ---------

def _unrouted(message_id) -> dict:
    # Routes nowhere (no handler for "nope", no default) -> RouteNotFoundError.
    return {"messageId": message_id, "body": json.dumps({"type": "nope"})}


def test_non_dict_list_element_fails_only_itself():
    # A Pipe/enrichment that emits a non-dict array element (str/int/None) must
    # NOT crash the whole batch; each bad element fails on its own, the valid
    # sibling still processes. (Regression for the AttributeError-out-of-handler bug.)
    seen = []
    app = FastSQS()

    @app.route(Ping)
    async def handle(msg: Ping):
        seen.append(msg.ping_id)

    result = app.handler([_record("P1"), "malformed", 42, None], None)

    assert seen == ["P1"]  # valid record unaffected
    ids = [f["itemIdentifier"] for f in result["batchItemFailures"]]
    assert ids == ["UNKNOWN", "UNKNOWN", "UNKNOWN"]  # 3 non-dict elements, no crash


def test_empty_or_none_messageid_never_emits_blank_identifier():
    # AWS reads an empty-string or null itemIdentifier as a WHOLE-batch failure.
    # A failing record with a present-but-blank messageId must coalesce to a safe
    # non-empty sentinel, never "" or None. (Regression for the blank-id bug.)
    app = FastSQS()  # no routes/default -> every record fails routing

    for bad_id in ("", None):
        out = app.handler([{"messageId": bad_id, "body": json.dumps({"type": "x"})}], None)
        ids = [f["itemIdentifier"] for f in out["batchItemFailures"]]
        assert ids == ["UNKNOWN"], (bad_id, out)
        assert "" not in ids and None not in ids


def test_bare_list_and_records_equivalent_with_mixed_pass_fail():
    # The SAME records, as a bare list and as {"Records": [...]}, produce identical
    # batchItemFailures (only the unroutable record's messageId).
    def build():
        app = FastSQS()

        @app.route(Ping)
        async def handle(msg: Ping):
            pass

        return app

    recs = [_record("ok1"), _unrouted("bad1"), _record("ok2")]
    out_list = build().handler(recs, None)
    out_records = build().handler({"Records": recs}, None)

    assert out_list == out_records
    assert out_list == {"batchItemFailures": [{"itemIdentifier": "bad1"}]}

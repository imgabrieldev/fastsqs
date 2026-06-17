"""LoggingMiddleware battle tests: structured before/after entries, verbose
scratch-only state-key logging, include_* toggles, error reporting, and the
app._log routing contract.

All assertions reflect REAL v1 behavior, verified against the source. A few
documented expectations were adjusted to match reality and are noted inline:

  * A registered LoggingMiddleware's ``logger`` callable also receives every
    internal ``app._log(...)`` line emitted during record processing (those
    entries carry a ``message`` key but NO ``stage``/``middleware`` key). So
    "every captured entry has stage/middleware" is only true once filtered to
    the middleware's own before/after entries. We assert the TRUE behavior:
    a uniform ts+lvl on every entry, and stage/middleware only on the
    middleware-stage entries.
"""

from fastsqs import FastSQS, SQSEvent, LoggingMiddleware
from fastsqs.middleware import Middleware
from fastsqs.testing import SQSTestClient


class Task(SQSEvent):
    task_id: str = "x"


def _stage_entries(captured, stage):
    """The LoggingMiddleware before/after entries for the given stage.

    (The captured list also holds internal app._log lines, which have no
    ``stage`` key; this filters them out.)
    """
    return [e for e in captured if e.get("stage") == stage]


def _only(seq):
    assert len(seq) == 1, f"expected exactly one entry, got {len(seq)}"
    return seq[0]


# --------------------------------------------------------------------------- #
# before / after entries
# --------------------------------------------------------------------------- #

def test_logging_middleware_emits_before_and_after_entries():
    captured = []
    app = FastSQS()
    app.add_middleware(LoggingMiddleware(logger=captured.append))

    @app.route(Task)
    async def handle(msg: Task):
        return {"ok": True}

    r = SQSTestClient(app).send({"type": "task", "task_id": "1"}, message_id="mid-1")
    assert r == {"batchItemFailures": []}

    stages = [e["stage"] for e in captured if "stage" in e]
    assert stages == ["before_processing", "after_processing"]

    before = _only(_stage_entries(captured, "before_processing"))
    after = _only(_stage_entries(captured, "after_processing"))
    assert before["msg_id"] == "mid-1"
    assert after["msg_id"] == "mid-1"
    assert before["middleware"] == "LoggingMiddleware"
    assert after["middleware"] == "LoggingMiddleware"


def test_logging_verbose_logs_only_scratch_state_keys_not_framework_fields():
    captured = []
    app = FastSQS()

    class Setter(Middleware):
        async def before(self, payload, record, context, ctx):
            ctx.state.foo = "bar"

    # Setter is registered first so its before() runs before the logging
    # middleware's before(); the scratch key is therefore present in both
    # the before and after entries.
    app.add_middleware(Setter())
    app.add_middleware(LoggingMiddleware(logger=captured.append, verbose=True))

    @app.route(Task)
    async def handle(msg: Task):
        return {"ok": True}

    SQSTestClient(app).send({"type": "task", "task_id": "1"}, message_id="s1")

    after = _only(_stage_entries(captured, "after_processing"))
    assert after["state_keys"] == ["foo"]            # scratch only == list(ctx.state)
    for framework_field in ("message_id", "queue_type", "record", "handler_result"):
        assert framework_field not in after["state_keys"]


def test_logging_custom_logger_callable_receives_dict():
    captured = []
    app = FastSQS()
    app.add_middleware(LoggingMiddleware(logger=lambda d: captured.append(d)))

    @app.route(Task)
    async def handle(msg: Task):
        return {"ok": True}

    SQSTestClient(app).send({"type": "task", "task_id": "1"}, message_id="c1")

    # Every emitted entry is a dict with a uniform ts + lvl envelope.
    assert captured, "logger received nothing"
    for entry in captured:
        assert isinstance(entry, dict)
        assert "ts" in entry and "lvl" in entry

    # The middleware's own stage entries additionally carry stage + middleware.
    stage_entries = [e for e in captured if "stage" in e]
    assert stage_entries, "no before/after stage entries captured"
    for entry in stage_entries:
        assert "ts" in entry and "lvl" in entry and "stage" in entry
        assert entry["middleware"] == "LoggingMiddleware"


# --------------------------------------------------------------------------- #
# include_* toggles
# --------------------------------------------------------------------------- #

def test_logging_include_payload_toggle():
    cap_on = []
    app_on = FastSQS()
    app_on.add_middleware(LoggingMiddleware(logger=cap_on.append, include_payload=True))

    @app_on.route(Task)
    async def handle_on(msg: Task):
        pass

    SQSTestClient(app_on).send({"type": "task", "task_id": "1"}, message_id="p1")
    before_on = _only(_stage_entries(cap_on, "before_processing"))
    assert before_on["payload"] == {"type": "task", "task_id": "1"}

    cap_off = []
    app_off = FastSQS()
    app_off.add_middleware(LoggingMiddleware(logger=cap_off.append, include_payload=False))

    @app_off.route(Task)
    async def handle_off(msg: Task):
        pass

    SQSTestClient(app_off).send({"type": "task", "task_id": "1"}, message_id="p2")
    before_off = _only(_stage_entries(cap_off, "before_processing"))
    assert "payload" not in before_off


def test_logging_include_record_and_context_toggles():
    class LambdaCtx:
        def __repr__(self):
            return "<LAMBDACTX>"

    lambda_ctx = LambdaCtx()

    cap_on = []
    app_on = FastSQS()
    app_on.add_middleware(
        LoggingMiddleware(logger=cap_on.append, include_record=True, include_context=True)
    )

    @app_on.route(Task)
    async def handle_on(msg: Task):
        pass

    SQSTestClient(app_on).send(
        {"type": "task", "task_id": "1"}, message_id="r1", context=lambda_ctx
    )
    before_on = _only(_stage_entries(cap_on, "before_processing"))
    assert isinstance(before_on["record"], dict)
    assert before_on["record"]["messageId"] == "r1"
    assert before_on["context_repr"] == repr(lambda_ctx) == "<LAMBDACTX>"

    cap_off = []
    app_off = FastSQS()
    app_off.add_middleware(LoggingMiddleware(logger=cap_off.append))  # both False (default)

    @app_off.route(Task)
    async def handle_off(msg: Task):
        pass

    SQSTestClient(app_off).send({"type": "task", "task_id": "1"}, message_id="r2")
    before_off = _only(_stage_entries(cap_off, "before_processing"))
    assert "record" not in before_off
    assert "context_repr" not in before_off


# --------------------------------------------------------------------------- #
# after: error level / error_details / handler_result_type
# --------------------------------------------------------------------------- #

def test_logging_after_sets_error_level_and_error_details_on_failure():
    cap_fail = []
    app_fail = FastSQS()
    app_fail.add_middleware(LoggingMiddleware(logger=cap_fail.append))

    @app_fail.route(Task)
    async def boom(msg: Task):
        raise RuntimeError("boom")

    r = SQSTestClient(app_fail).send({"type": "task", "task_id": "1"}, message_id="e1")
    assert r == {"batchItemFailures": [{"itemIdentifier": "e1"}]}

    after = _only(_stage_entries(cap_fail, "after_processing"))
    assert after["lvl"] == "ERROR"
    details = after["error_details"]
    assert details["error_type"] == "RuntimeError"
    assert "boom" in details["error_message"]
    assert isinstance(details["traceback"], str) and details["traceback"]

    # Passing case: no error_details, lvl is the configured level.
    cap_ok = []
    app_ok = FastSQS()
    mw = LoggingMiddleware(logger=cap_ok.append)
    app_ok.add_middleware(mw)

    @app_ok.route(Task)
    async def ok(msg: Task):
        pass

    SQSTestClient(app_ok).send({"type": "task", "task_id": "1"}, message_id="e2")
    after_ok = _only(_stage_entries(cap_ok, "after_processing"))
    assert "error_details" not in after_ok
    assert after_ok["lvl"] == mw.level


def test_logging_after_handler_result_type_recorded():
    cap_dict = []
    app_dict = FastSQS()
    app_dict.add_middleware(LoggingMiddleware(logger=cap_dict.append))

    @app_dict.route(Task)
    async def returns_dict(msg: Task):
        return {"k": "v"}

    SQSTestClient(app_dict).send({"type": "task", "task_id": "1"}, message_id="h1")
    after_dict = _only(_stage_entries(cap_dict, "after_processing"))
    assert after_dict["processing_results"]["handler_result_type"] == "dict"

    cap_none = []
    app_none = FastSQS()
    app_none.add_middleware(LoggingMiddleware(logger=cap_none.append))

    @app_none.route(Task)
    async def returns_none(msg: Task):
        return None

    SQSTestClient(app_none).send({"type": "task", "task_id": "1"}, message_id="h2")
    after_none = _only(_stage_entries(cap_none, "after_processing"))
    assert after_none["processing_results"]["handler_result_type"] is None


# --------------------------------------------------------------------------- #
# app._log routing
# --------------------------------------------------------------------------- #

def test_app_internal_log_routed_through_logging_middleware():
    captured = []
    app = FastSQS()
    app.add_middleware(LoggingMiddleware(logger=captured.append))

    app._log("info", "hello", msg_id="x")

    assert len(captured) == 1
    entry = captured[0]
    assert entry["message"] == "hello"
    assert entry["lvl"] == "INFO"          # level is upper-cased by LoggingMiddleware.log
    assert entry["msg_id"] == "x"
    assert "ts" in entry
    # app._log forwards to the middleware's log(), which does not stamp a stage.
    assert "stage" not in entry


def test_app_internal_log_no_logging_middleware_is_silent(capsys):
    app = FastSQS()  # no LoggingMiddleware registered

    # Does not raise and emits no output (the for-loop finds nothing, returns).
    app._log("info", "hi")
    app._log("error", "still nothing", code=500)

    out = capsys.readouterr()
    assert out.out == ""
    assert out.err == ""

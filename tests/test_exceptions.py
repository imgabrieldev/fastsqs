"""Exception hierarchy + BatchFailedError.failures + InvalidMessageError cause chaining.

All fastsqs exceptions subclass :class:`FastSQSError`, so a caller can catch any
framework error with a single ``except FastSQSError``. ``BatchFailedError`` carries
the failed item identifiers on ``.failures`` and accepts an optional custom message
(otherwise it composes a default mentioning the failure count and that
``partial_batch_failure`` is disabled). ``InvalidMessageError`` is raised via
``raise ... from e`` for both a JSON-decode failure (non-JSON body) and a pydantic
validation failure (per-route ``model=``), so its ``__cause__`` preserves the real
cause.

These mix direct exception construction with behavioral assertions driven through
``partial_batch_failure=False`` (app.handler) and direct ``app._handle_record``
calls. No AWS.

real-behavior notes (verified against source, no library changes):
- BatchFailedError default message text is exactly
  ``f"{len(failures)} record(s) failed and partial_batch_failure is False; failing
  the whole batch"`` (exceptions.py), so it contains the count and the literal
  substring ``partial_batch_failure``.
- BatchFailedError is raised by ``_handle_event`` (processing.py) only when
  ``partial_batch_failure`` is False AND ``result["batchItemFailures"]`` is
  non-empty; ``.failures`` is the list of failed ``itemIdentifier`` strings.
- InvalidMessageError for a non-JSON body chains the ``json.JSONDecodeError``
  (processing.py: ``raise InvalidMessageError(...) from e``).
- InvalidMessageError for a per-route ``model=`` validation failure chains the
  pydantic ``ValidationError`` (router.py: ``raise InvalidMessageError(...) from e``).
- RouteNotFoundError message embeds the unmatched discriminator value via
  ``{self.discriminator}={discriminator_value!r}`` (processing.py).
"""

import asyncio
import json

import pytest
from pydantic import ValidationError

from fastsqs import FastSQS, SQSEvent, SQSRouter
from fastsqs.exceptions import (
    BatchFailedError,
    FastSQSError,
    InvalidMessageError,
    RouteNotFoundError,
)
from fastsqs.testing import SQSTestClient, RecordSpec


class Task(SQSEvent):
    task_id: str = "x"


class RequiredModel(SQSEvent):
    amount: int  # required: missing -> pydantic ValidationError


# ---- hierarchy ----------------------------------------------------------------

def test_all_exceptions_subclass_fastsqserror():
    assert issubclass(RouteNotFoundError, FastSQSError)
    assert issubclass(InvalidMessageError, FastSQSError)
    assert issubclass(BatchFailedError, FastSQSError)

    # A single ``except FastSQSError`` catches an instance of each.
    for exc in (
        RouteNotFoundError("no route"),
        InvalidMessageError("bad body"),
        BatchFailedError(["a"]),
    ):
        try:
            raise exc
        except FastSQSError as caught:
            assert caught is exc
        else:  # pragma: no cover - defensive
            pytest.fail(f"{type(exc).__name__} was not caught by except FastSQSError")


@pytest.mark.parametrize(
    "exc_cls",
    [RouteNotFoundError, InvalidMessageError],
)
def test_simple_exceptions_are_plain_fastsqserror_subclasses(exc_cls):
    inst = exc_cls("boom")
    assert isinstance(inst, FastSQSError)
    assert str(inst) == "boom"


# ---- BatchFailedError.failures + message --------------------------------------

def test_batchfailed_error_exposes_failures_list():
    err = BatchFailedError(["a", "b"])
    assert err.failures == ["a", "b"]


def test_batchfailed_error_default_message_mentions_count():
    msg = str(BatchFailedError(["a", "b"]))
    assert "2" in msg
    assert "partial_batch_failure" in msg


def test_batchfailed_error_custom_message_overrides_default():
    err = BatchFailedError(["a"], message="boom")
    assert str(err) == "boom"
    assert err.failures == ["a"]


# ---- partial_batch_failure=False -> BatchFailedError with exact failed ids -----

def test_partial_disabled_raises_batchfailed_with_failed_ids():
    app = FastSQS(partial_batch_failure=False)

    @app.route(Task)
    async def handle(msg: Task):
        if msg.task_id == "explode":
            raise ValueError("boom")

    client = SQSTestClient(app)

    # One passing record and one failing record (failing record's id == 'bad').
    with pytest.raises(BatchFailedError) as ei:
        client.send_batch(
            [
                RecordSpec({"type": "task", "task_id": "ok"}, message_id="good"),
                RecordSpec({"type": "task", "task_id": "explode"}, message_id="bad"),
            ]
        )

    # Exactly the failed itemIdentifiers, not the whole batch.
    assert ei.value.failures == ["bad"]
    assert isinstance(ei.value, FastSQSError)


# ---- InvalidMessageError chains json.JSONDecodeError --------------------------

def test_invalid_message_error_chains_json_decode_cause():
    app = FastSQS()

    @app.route(Task)
    async def handle(msg: Task):
        pass

    record = {"messageId": "m", "body": "{not json"}

    with pytest.raises(InvalidMessageError) as ei:
        asyncio.run(app._handle_record(record, None))

    assert isinstance(ei.value.__cause__, json.JSONDecodeError)


# ---- InvalidMessageError chains pydantic ValidationError ----------------------

def test_invalid_message_error_chains_validation_cause():
    router = SQSRouter(discriminator="type")

    @router.route("needy", model=RequiredModel)
    async def handle(msg):
        pass

    app = FastSQS()
    app.include_router(router)

    # Missing the required ``amount`` field -> model_validate raises
    # ValidationError, wrapped as InvalidMessageError via ``raise ... from e``.
    record = {"messageId": "m", "body": json.dumps({"type": "needy"})}

    with pytest.raises(InvalidMessageError) as ei:
        asyncio.run(app._handle_record(record, None))

    assert isinstance(ei.value.__cause__, ValidationError)


# ---- RouteNotFoundError is a FastSQSError and names the value -----------------

def test_route_not_found_is_fastsqserror_instance():
    app = FastSQS()

    @app.route(Task)  # one route, no default
    async def handle(msg: Task):
        pass

    record = {"messageId": "z", "body": json.dumps({"type": "unmatched"})}

    with pytest.raises(RouteNotFoundError) as ei:
        asyncio.run(app._handle_record(record, None))

    exc = ei.value
    assert isinstance(exc, FastSQSError)
    assert "unmatched" in str(exc)  # the unmatched discriminator value

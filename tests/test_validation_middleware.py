"""Validation, normalization, masking & error-handling battle tests."""

from fastsqs import FastSQS, SQSEvent
from fastsqs.middleware import Middleware, ErrorHandlingMiddleware, CircuitBreaker
from fastsqs.middleware.error_handling import CircuitBreakerState
from fastsqs.testing import SQSTestClient
from fastsqs.utils import shallow_mask, deep_mask


class Task(SQSEvent):
    task_id: str


# ---- validation surfaces as InvalidMessage and preserves messageId ----

class Strict(SQSEvent):
    amount: int


def test_validation_failure_preserves_messageid():
    app = FastSQS()

    @app.route(Strict)
    async def h(msg: Strict):
        pass

    r = SQSTestClient(app).send({"type": "strict"}, message_id="bad-7")  # amount missing
    assert r == {"batchItemFailures": [{"itemIdentifier": "bad-7"}]}


class Inner(SQSEvent):
    n: int


class Outer(SQSEvent):
    inner: Inner


def test_nested_model_validation_failure_fails_record():
    app = FastSQS()

    @app.route(Outer)
    async def h(msg: Outer):
        pass

    # inner.n is not an int -> nested validation error -> clean record failure
    r = SQSTestClient(app).send({"type": "outer", "inner": {"n": "notint"}}, message_id="n1")
    assert r == {"batchItemFailures": [{"itemIdentifier": "n1"}]}


def test_invalid_json_body_fails_record():
    app = FastSQS()

    @app.route(Task)
    async def h(msg: Task):
        pass

    r = app.handler({"Records": [{"messageId": "j1", "body": "{not json"}]}, None)
    assert r == {"batchItemFailures": [{"itemIdentifier": "j1"}]}


# ---- field-name normalization ----

class Customer(SQSEvent):
    first_name: str


def test_camelcase_normalized_to_snake():
    app = FastSQS()
    got = {}

    @app.route(Customer)
    async def h(msg: Customer):
        got["fn"] = msg.first_name

    SQSTestClient(app).send({"type": "customer", "firstName": "Ada"})
    assert got["fn"] == "Ada"


def test_message_type_derivation():
    assert Customer.get_message_type() == "customer"
    variants = Customer.get_message_type_variants()
    assert "customer" in variants and "Customer" in variants


# ---- handler error vs after-hook error: handler error wins ----

def test_handler_error_not_masked_by_after_hook_error():
    seen = []

    class Boom(Middleware):
        async def after(self, payload, record, context, ctx, error):
            raise RuntimeError("after boom")

    class Recorder(Middleware):
        async def after(self, payload, record, context, ctx, error):
            seen.append(error)

    app = FastSQS()
    app.add_middleware(Recorder())  # after runs reversed: Boom first, then Recorder
    app.add_middleware(Boom())

    @app.route(Task)
    async def h(msg: Task):
        raise ValueError("handler fail")

    r = SQSTestClient(app).send({"type": "task", "task_id": "1"}, message_id="h1")
    assert r == {"batchItemFailures": [{"itemIdentifier": "h1"}]}
    assert len(seen) == 1 and isinstance(seen[0], ValueError)


# ---- circuit breaker transitions ----

def test_circuit_breaker_opens_at_threshold_and_resets():
    cb = CircuitBreaker(failure_threshold=2, recovery_timeout=60.0)
    assert cb.should_allow_request() is True
    cb.record_failure(ValueError())
    assert cb.state == CircuitBreakerState.CLOSED
    cb.record_failure(ValueError())
    assert cb.state == CircuitBreakerState.OPEN
    assert cb.should_allow_request() is False
    cb.record_success()
    assert cb.state == CircuitBreakerState.CLOSED
    assert cb.should_allow_request() is True


def test_error_handler_dlq_sync_and_async():
    # sync dlq
    sync_seen = []

    def sync_dlq(payload, record, error):
        sync_seen.append(type(error).__name__)

    app = FastSQS()
    app.add_middleware(ErrorHandlingMiddleware(dead_letter_handler=sync_dlq))

    @app.route(Task)
    async def h(msg: Task):
        raise ConnectionError("x")

    SQSTestClient(app).send({"type": "task", "task_id": "1"})
    assert sync_seen == ["ConnectionError"]

    # async dlq
    async_seen = []

    async def async_dlq(payload, record, error):
        async_seen.append(type(error).__name__)

    app2 = FastSQS()
    app2.add_middleware(ErrorHandlingMiddleware(dead_letter_handler=async_dlq))

    @app2.route(Task)
    async def h2(msg: Task):
        raise ConnectionError("y")

    SQSTestClient(app2).send({"type": "task", "task_id": "1"})
    assert async_seen == ["ConnectionError"]


# ---- masking: shallow vs deep (D4) ----

def test_shallow_mask_top_level_only():
    out = shallow_mask({"cpf": "123", "nested": {"cpf": "456"}}, ["cpf"])
    assert out["cpf"] == "***"
    assert out["nested"]["cpf"] == "456"  # shallow leaves nested


def test_deep_mask_recurses_into_nested_and_lists():
    payload = {
        "cpf": "111",
        "customer": {"cpf": "222", "name": "Ada"},
        "items": [{"cpf": "333"}, {"ok": 1}],
    }
    out = deep_mask(payload, ["cpf"])
    assert out["cpf"] == "***"
    assert out["customer"]["cpf"] == "***"
    assert out["customer"]["name"] == "Ada"
    assert out["items"][0]["cpf"] == "***"
    assert out["items"][1] == {"ok": 1}
    # input not mutated
    assert payload["customer"]["cpf"] == "222"


# ---- D3: positional-only params are not injected (documented) ----

def test_positional_only_param_is_not_injected():
    app = FastSQS()

    @app.route(Task)
    async def h(msg: Task, /):  # positional-only
        pass

    # select_kwargs only injects by keyword; a positional-only param receives
    # nothing -> the call fails -> record fails. Documents the limitation.
    r = SQSTestClient(app).send({"type": "task", "task_id": "1"}, message_id="p1")
    assert r == {"batchItemFailures": [{"itemIdentifier": "p1"}]}

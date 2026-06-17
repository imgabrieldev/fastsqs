"""Typed Context (C2): typed framework attributes + a separate ctx.state scratch."""

import pytest

from fastsqs import FastSQS, SQSEvent, Context, State, QueueType, Depends
from fastsqs.middleware import Middleware
from fastsqs.testing import SQSTestClient


class Task(SQSEvent):
    task_id: str


def test_framework_fields_are_typed_attributes():
    seen = {}
    app = FastSQS()

    @app.route(Task)
    async def handle(msg: Task, ctx: Context):
        seen["mid"] = ctx.message_id          # str
        seen["qtype"] = ctx.queue_type        # QueueType enum
        seen["mtype"] = ctx.message_type
        seen["route"] = ctx.route_path

    SQSTestClient(app).send({"type": "task", "task_id": "1"}, message_id="abc")
    assert seen["mid"] == "abc"
    assert seen["qtype"] is QueueType.STANDARD
    assert seen["mtype"] == "task"
    assert isinstance(seen["route"], list)


def test_scratch_lives_in_ctx_state():
    captured = {}
    app = FastSQS()

    class MW(Middleware):
        async def before(self, payload, record, context, ctx):
            ctx.state.custom = "X"                  # attribute write
            ctx.state["acc"] = [1]                  # item write
            ctx.state.setdefault("seen", []).append(1)

        async def after(self, payload, record, context, ctx, error):
            captured["custom"] = ctx.state.custom   # attribute read
            captured["acc"] = ctx.state["acc"]      # item read
            captured["seen"] = ctx.state.get("seen")

    app.add_middleware(MW())

    @app.route(Task)
    async def handle(msg: Task, ctx: Context):
        pass

    SQSTestClient(app).send({"type": "task", "task_id": "1"})
    assert captured == {"custom": "X", "acc": [1], "seen": [1]}


def test_scratch_cannot_clobber_framework_fields():
    seen = {}
    app = FastSQS()

    class MW(Middleware):
        async def before(self, payload, record, context, ctx):
            ctx.state.message_id = "SCRATCH"   # writing scratch named like a field

    app.add_middleware(MW())

    @app.route(Task)
    async def handle(msg: Task, ctx: Context):
        seen["framework"] = ctx.message_id        # still the real value
        seen["scratch"] = ctx.state.message_id    # the scratch value

    SQSTestClient(app).send({"type": "task", "task_id": "1"}, message_id="real")
    assert seen == {"framework": "real", "scratch": "SCRATCH"}


def test_typed_context_with_di_together():
    def get_svc():
        return "SVC"

    out = {}
    app = FastSQS()

    @app.route(Task)
    async def handle(msg: Task, ctx: Context, svc=Depends(get_svc)):
        out["mid"] = ctx.message_id
        out["svc"] = svc

    SQSTestClient(app).send({"type": "task", "task_id": "1"}, message_id="m7")
    assert out == {"mid": "m7", "svc": "SVC"}


def test_state_unit_semantics():
    st = State({"a": 1})
    assert st.a == 1 == st["a"]
    st.b = 2
    assert st["b"] == 2 and "b" in st
    st["c"] = 3
    assert st.c == 3
    assert st.get("missing") is None
    assert st.setdefault("d", 4) == 4 and st.d == 4
    assert set(st) == {"a", "b", "c", "d"}
    with pytest.raises(AttributeError):
        _ = st.nope          # attribute miss raises (unlike .get)

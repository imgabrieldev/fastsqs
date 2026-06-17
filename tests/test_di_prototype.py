"""Prototype: passive dependency injection via fast-depends.

The user never writes @inject — `@app.route(...)` applies it under the hood
when the handler declares Depends(...) params. Handlers without Depends are
untouched (backward compatible).
"""

from fastsqs import FastSQS, SQSEvent, Depends
from fastsqs.testing import SQSTestClient


class Task(SQSEvent):
    task_id: str


def test_passive_di_injects_dependency_without_inject_decorator():
    calls = {"n": 0}

    def get_service():
        calls["n"] += 1
        return "SERVICE"

    seen = []
    app = FastSQS()

    @app.route(Task)  # NO @inject — passive
    async def handle(msg: Task, svc=Depends(get_service)):
        seen.append((msg.task_id, svc))

    result = SQSTestClient(app).send({"type": "task", "task_id": "1"})

    assert result == {"batchItemFailures": []}
    assert seen == [("1", "SERVICE")]   # dependency injected
    assert calls["n"] == 1              # factory resolved once


def test_backward_compat_plain_handler_unchanged():
    got = []
    app = FastSQS()

    @app.route(Task)
    async def handle(msg: Task, ctx):   # no Depends -> name-based path, untouched
        got.append((msg.task_id, ctx.message_id))

    SQSTestClient(app).send({"type": "task", "task_id": "2"}, message_id="m9")
    assert got == [("2", "m9")]


def test_sub_dependency_graph_resolves():
    def get_config():
        return {"region": "us-east-1"}

    def get_client(config=Depends(get_config)):   # dep that depends on a dep
        return f"client@{config['region']}"

    out = []
    app = FastSQS()

    @app.route(Task)
    async def handle(msg: Task, client=Depends(get_client)):
        out.append(client)

    SQSTestClient(app).send({"type": "task", "task_id": "3"})
    assert out == ["client@us-east-1"]   # sub-dependency resolved (free from fast-depends)

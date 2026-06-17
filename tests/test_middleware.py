from fastsqs import FastSQS, SQSEvent
from fastsqs.middleware import Middleware
from fastsqs.testing import SQSTestClient


class Task(SQSEvent):
    task_id: str


def test_middleware_before_and_after_wrap_the_handler():
    events = []

    class Recorder(Middleware):
        async def before(self, payload, record, context, ctx):
            events.append("before")

        async def after(self, payload, record, context, ctx, error):
            events.append(f"after:{error is None}")

    app = FastSQS()
    app.add_middleware(Recorder())

    @app.route(Task)
    async def handle(msg: Task):
        events.append("handler")

    SQSTestClient(app).send({"type": "task", "task_id": "1"})
    assert events == ["before", "handler", "after:True"]


def test_after_receives_error_on_handler_failure():
    errors = []

    class Recorder(Middleware):
        async def after(self, payload, record, context, ctx, error):
            errors.append(error)

    app = FastSQS()
    app.add_middleware(Recorder())

    @app.route(Task)
    async def handle(msg: Task):
        raise ValueError("boom")

    SQSTestClient(app).send({"type": "task", "task_id": "1"})
    assert len(errors) == 1
    assert isinstance(errors[0], ValueError)

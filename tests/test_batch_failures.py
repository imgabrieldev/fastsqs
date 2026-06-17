from fastsqs import FastSQS, SQSEvent
from fastsqs.testing import SQSTestClient


class Task(SQSEvent):
    task_id: str


def test_all_success_returns_empty_failures():
    app = FastSQS()

    @app.route(Task)
    async def handle(msg: Task):
        pass

    result = SQSTestClient(app).send_batch([
        {"type": "task", "task_id": "1"},
        {"type": "task", "task_id": "2"},
    ])
    assert result == {"batchItemFailures": []}


def test_partial_batch_failure_isolates_bad_record():
    app = FastSQS()

    @app.route(Task)
    async def handle(msg: Task):
        if msg.task_id == "bad":
            raise ValueError("boom")

    result = SQSTestClient(app).send_batch([
        {"type": "task", "task_id": "ok1"},   # m0
        {"type": "task", "task_id": "bad"},   # m1 -> fails
        {"type": "task", "task_id": "ok2"},   # m2
    ])
    assert result == {"batchItemFailures": [{"itemIdentifier": "m1"}]}

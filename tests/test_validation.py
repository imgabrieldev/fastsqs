from fastsqs import FastSQS, SQSEvent
from fastsqs.testing import SQSTestClient


class Order(SQSEvent):
    order_id: str


def test_invalid_json_body_fails_the_record():
    app = FastSQS()

    @app.route(Order)
    async def handle(msg: Order):
        pass

    # body is not valid JSON -> InvalidMessage -> record fails
    event = {"Records": [{"messageId": "m-bad-json", "body": "not json{"}]}
    result = app.handler(event, None)
    assert result == {"batchItemFailures": [{"itemIdentifier": "m-bad-json"}]}


def test_missing_required_field_fails_the_record():
    app = FastSQS()

    @app.route(Order)
    async def handle(msg: Order):
        pass

    # routes to Order by type, but order_id is missing -> validation error
    result = SQSTestClient(app).send({"type": "order"}, message_id="m-invalid")
    assert result == {"batchItemFailures": [{"itemIdentifier": "m-invalid"}]}

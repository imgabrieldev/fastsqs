from fastsqs import FastSQS, SQSEvent
from fastsqs.testing import SQSTestClient


class Order(SQSEvent):
    order_id: str
    customer_name: str


def _route_capturing(app):
    captured = {}

    @app.route(Order)
    async def handle(msg: Order):
        captured["msg"] = msg

    return captured


def test_snake_case_fields_bind():
    app = FastSQS()
    captured = _route_capturing(app)
    SQSTestClient(app).send({"type": "order", "order_id": "1", "customer_name": "Ana"})
    assert captured["msg"].order_id == "1"
    assert captured["msg"].customer_name == "Ana"


def test_camel_case_normalized_to_snake():
    app = FastSQS()
    captured = _route_capturing(app)
    SQSTestClient(app).send({"type": "order", "orderId": "2", "customerName": "Bia"})
    assert captured["msg"].order_id == "2"
    assert captured["msg"].customer_name == "Bia"


def test_kebab_case_normalized_to_snake():
    app = FastSQS()
    captured = _route_capturing(app)
    SQSTestClient(app).send({"type": "order", "order-id": "3", "customer-name": "Cid"})
    assert captured["msg"].order_id == "3"
    assert captured["msg"].customer_name == "Cid"

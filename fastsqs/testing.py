"""In-process test client for FastSQS apps.

The SQS analog of ``fastapi.testclient.TestClient``: lets you "send" a message
(or a batch) to a FastSQS app without hand-building the raw SQS event envelope.
Kept in a separate submodule (not exported from the package root), mirroring
``fastapi.testclient``.

    from fastsqs import FastSQS, SQSEvent
    from fastsqs.testing import SQSTestClient

    app = FastSQS()

    @app.route(OrderCreated)
    async def handle(msg: OrderCreated): ...

    client = SQSTestClient(app)
    result = client.send({"type": "order_created", "order_id": "1"})
    assert result == {"batchItemFailures": []}
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from .app import FastSQS


class SQSTestClient:
    """Wraps a FastSQS app and dispatches synthetic SQS events to it."""

    def __init__(self, app: FastSQS) -> None:
        self.app = app

    def send(
        self,
        body: dict,
        *,
        message_id: str = "test-1",
        attributes: Optional[dict] = None,
        context: Any = None,
    ) -> Dict[str, Any]:
        """Send a single message. Returns ``app.handler``'s result, i.e.
        ``{"batchItemFailures": [...]}``."""
        record: Dict[str, Any] = {"messageId": message_id, "body": json.dumps(body)}
        if attributes is not None:
            record["attributes"] = attributes
        return self.app.handler({"Records": [record]}, context)

    def send_batch(
        self,
        bodies: List[dict],
        *,
        message_group_id: Optional[str] = None,
        context: Any = None,
    ) -> Dict[str, Any]:
        """Send a batch (one record per body). Pass ``message_group_id`` to
        exercise FIFO grouping."""
        records: List[Dict[str, Any]] = []
        for index, body in enumerate(bodies):
            record: Dict[str, Any] = {"messageId": f"m{index}", "body": json.dumps(body)}
            if message_group_id is not None:
                record["attributes"] = {"messageGroupId": message_group_id}
            records.append(record)
        return self.app.handler({"Records": records}, context)

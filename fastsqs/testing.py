"""In-process test client for FastSQS apps.

The SQS analog of ``fastapi.testclient.TestClient``: lets you "send" a message
(or a batch) to a FastSQS app without hand-building the raw SQS event envelope.
Kept in a separate submodule (not exported from the package root), mirroring
``fastapi.testclient``.

    from fastsqs import FastSQS, SQSEvent
    from fastsqs.testing import SQSTestClient, RecordSpec

    app = FastSQS()

    @app.route(OrderCreated)
    async def handle(msg: OrderCreated): ...

    client = SQSTestClient(app)
    result = client.send({"type": "order_created", "order_id": "1"})
    assert result == {"batchItemFailures": []}

    # FIFO: per-record groups (a ``.fifo`` ARN is set so AUTO infers FIFO)
    client.send_batch([
        RecordSpec({"type": "t", "id": "1"}, group_id="g1"),
        RecordSpec({"type": "t", "id": "2"}, group_id="g2"),
    ])
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Union

from .app import FastSQS

Body = Union[dict, str, bytes]

# Synthetic event-source ARNs so QueueType.AUTO infers the right path in tests.
_FIFO_ARN = "arn:aws:sqs:us-east-1:000000000000:test-queue.fifo"
_STANDARD_ARN = "arn:aws:sqs:us-east-1:000000000000:test-queue"


def _body_str(body: Body) -> str:
    """Serialize a message body. ``dict`` -> JSON; ``str``/``bytes`` pass through
    verbatim (so malformed-body / non-JSON paths are reachable)."""
    if isinstance(body, (bytes, bytearray)):
        return body.decode()
    if isinstance(body, str):
        return body
    return json.dumps(body)


def make_record(
    body: Body,
    *,
    message_id: str = "test-1",
    group_id: Optional[str] = None,
    deduplication_id: Optional[str] = None,
    message_attributes: Optional[dict] = None,
    event_source_arn: Optional[str] = None,
    attributes: Optional[dict] = None,
) -> Dict[str, Any]:
    """Build one synthetic SQS record.

    Snake_case kwargs map to the camelCase SQS wire keys. When ``group_id`` is
    given (and no explicit ``event_source_arn``), a ``.fifo`` ARN is set so a
    ``QueueType.AUTO`` app infers FIFO.
    """
    record: Dict[str, Any] = {"messageId": message_id, "body": _body_str(body)}

    attrs: Dict[str, Any] = dict(attributes or {})
    if group_id is not None:
        attrs.setdefault("messageGroupId", group_id)
    if deduplication_id is not None:
        attrs.setdefault("messageDeduplicationId", deduplication_id)
    if attrs:
        record["attributes"] = attrs

    if event_source_arn is None:
        event_source_arn = _FIFO_ARN if group_id is not None else _STANDARD_ARN
    record["eventSourceARN"] = event_source_arn

    if message_attributes is not None:
        record["messageAttributes"] = message_attributes

    return record


def make_event(records: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    """Wrap records in an SQS event envelope."""
    return {"Records": list(records)}


@dataclass
class RecordSpec:
    """One record in a :meth:`SQSTestClient.send_batch` call.

    Use distinct ``group_id`` values to model multiple FIFO message groups in a
    single batch.
    """

    body: Body
    message_id: Optional[str] = None
    group_id: Optional[str] = None
    deduplication_id: Optional[str] = None
    message_attributes: Optional[dict] = None


class SQSTestClient:
    """Wraps a FastSQS app and dispatches synthetic SQS events to it."""

    def __init__(self, app: FastSQS) -> None:
        self.app = app

    def send(
        self,
        body: Body,
        *,
        message_id: str = "test-1",
        group_id: Optional[str] = None,
        deduplication_id: Optional[str] = None,
        message_attributes: Optional[dict] = None,
        attributes: Optional[dict] = None,
        event_source_arn: Optional[str] = None,
        context: Any = None,
    ) -> Dict[str, Any]:
        """Send a single message. Returns ``app.handler``'s result, i.e.
        ``{"batchItemFailures": [...]}``. ``body`` may be a ``dict`` (JSON-encoded)
        or a raw ``str``/``bytes`` (passed through, e.g. to test malformed bodies)."""
        record = make_record(
            body,
            message_id=message_id,
            group_id=group_id,
            deduplication_id=deduplication_id,
            message_attributes=message_attributes,
            attributes=attributes,
            event_source_arn=event_source_arn,
        )
        return self.app.handler(make_event([record]), context)

    def send_batch(
        self,
        specs: Iterable[Union[RecordSpec, Body]],
        *,
        context: Any = None,
    ) -> Dict[str, Any]:
        """Send a batch. Each item is a :class:`RecordSpec` (full control, incl.
        per-record FIFO ``group_id``) or a bare body for the simple case."""
        records: List[Dict[str, Any]] = []
        for index, spec in enumerate(specs):
            if not isinstance(spec, RecordSpec):
                spec = RecordSpec(body=spec)
            records.append(
                make_record(
                    spec.body,
                    message_id=spec.message_id or f"m{index}",
                    group_id=spec.group_id,
                    deduplication_id=spec.deduplication_id,
                    message_attributes=spec.message_attributes,
                )
            )
        return self.app.handler(make_event(records), context)


__all__ = ["SQSTestClient", "RecordSpec", "make_record", "make_event"]

"""Record and batch processing for FastSQS.

Split out of app.py as a mixin: these methods rely on the FastSQS instance for
its router(s), middleware chain, logging and queue configuration.
"""

from __future__ import annotations

import asyncio
import json
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from .exceptions import RouteNotFound, InvalidMessage, BatchFailedError
from .middleware import run_middleware_stack
from .utils import group_records_by_message_group

if TYPE_CHECKING:
    from .middleware import Middleware
    from .routing import SQSRouter
    from .types import QueueType


class RecordProcessingMixin:
    """SQS record/batch processing for FastSQS (mixed into the app class).

    These methods read state owned by the concrete FastSQS class. That state is
    declared below (under TYPE_CHECKING) so the contract is explicit and
    type-checkable, without introducing a runtime base-class dependency. ``ctx``
    keys are documented by ``ProcessingContext`` in ``types.py``.
    """

    if TYPE_CHECKING:
        _main_router: "SQSRouter"
        _routers: List["SQSRouter"]
        _middlewares: List["Middleware"]
        message_type_key: str
        queue_type: "QueueType"
        debug: bool
        max_concurrent_messages: int
        enable_partial_batch_failure: bool
        skip_group_on_error: bool

        def _log(self, level: str, message: str, **data: Any) -> None: ...
        def is_fifo_queue(self) -> bool: ...

    async def _handle_record(self, record: dict, context: Any) -> Optional[Any]:
        """Handle a single SQS record.

        Raises:
            InvalidMessage: If the message body is not a JSON object.
            RouteNotFound: If no handler matches the message.
        """
        body_str = record.get("body", "")
        msg_id = record.get("messageId") or record.get("message_id") or "UNKNOWN"

        self._log("info", "Starting record processing", msg_id=msg_id)
        self._log(
            "debug",
            "Raw body",
            msg_id=msg_id,
            body=body_str[:500] + ("..." if len(body_str) > 500 else ""),
        )

        try:
            payload = json.loads(body_str) if body_str else {}
            if not isinstance(payload, dict):
                raise InvalidMessage("Message body must be a JSON object")
            self._log("debug", "Parsed payload", msg_id=msg_id, payload=payload)
        except json.JSONDecodeError as e:
            self._log("error", "JSON decode error", msg_id=msg_id, error=str(e))
            raise InvalidMessage(f"Invalid JSON in message body: {e}")

        ctx: Dict[str, Any] = {
            "messageId": msg_id,
            "record": record,
            "context": context,
            "route_path": [],
            "queueType": self.queue_type.value,
        }

        if self.is_fifo_queue():
            attributes = record.get("attributes", {})
            ctx["fifoInfo"] = {
                "messageGroupId": attributes.get("messageGroupId"),
                "messageDeduplicationId": attributes.get("messageDeduplicationId"),
                "queueType": "fifo",
            }
            self._log("debug", "FIFO info", msg_id=msg_id, fifo_info=ctx["fifoInfo"])

        async def _route() -> Any:
            # Try main router first
            self._log("debug", "Trying main router", msg_id=msg_id)
            if await self._main_router.dispatch(
                payload, record, context, ctx, root_payload=payload
            ):
                self._log("debug", "Main router handled the message", msg_id=msg_id)
                return ctx.get("handler_result")

            if self._routers:
                self._log(
                    "debug",
                    "Trying routers",
                    msg_id=msg_id,
                    router_count=len(self._routers),
                )
                for i, router in enumerate(self._routers):
                    self._log(
                        "debug",
                        f"Trying router {i}",
                        msg_id=msg_id,
                        router_key=router.key,
                    )
                    if await router.dispatch(
                        payload, record, context, ctx, root_payload=payload
                    ):
                        self._log(
                            "debug", f"Router {i} handled the message", msg_id=msg_id
                        )
                        return ctx.get("handler_result")
                    self._log(
                        "debug",
                        f"Router {i} did not handle the message",
                        msg_id=msg_id,
                    )

            available_routes = list(self._main_router._pydantic_routes.keys())
            available_routers = [r.key for r in self._routers]
            discriminator = payload.get(self.message_type_key)
            error_msg = (
                f"No handler found for message "
                f"({self.message_type_key}={discriminator!r}). "
                f"Available FastSQS routes: {available_routes}, "
                f"Available router keys: {available_routers}"
            )
            self._log(
                "error",
                error_msg,
                msg_id=msg_id,
                available_routes=available_routes,
                available_routers=available_routers,
            )
            raise RouteNotFound(error_msg)

        # before -> route -> after, with balanced cleanup: a before-hook raising
        # still unwinds the middlewares that already entered (release slots,
        # cancel monitors), and after-hook errors never mask the real failure.
        result = await run_middleware_stack(
            self._middlewares, payload, record, context, ctx, _route
        )

        self._log("info", "Record processing completed successfully", msg_id=msg_id)
        return result

    async def _handle_event(self, event: dict, context: Any) -> dict:
        """Handle an SQS event with multiple records."""
        records = event.get("Records") or []
        if not isinstance(records, list) or not records:
            return {"batchItemFailures": []}

        if self.debug:
            queue_info = f"queue_type={self.queue_type.value}, records={len(records)}"
            self._log("info", "Processing event", queue_info=queue_info)

        if self.is_fifo_queue():
            result = await self._handle_fifo_event(records, context)
        else:
            result = await self._handle_standard_event(records, context)

        # When partial batch failure is disabled, ReportBatchItemFailures is not
        # in play: any failure must fail the WHOLE batch so SQS redelivers every
        # message. Returning empty failures here would tell SQS everything
        # succeeded -> silent data loss.
        if not self.enable_partial_batch_failure and result["batchItemFailures"]:
            raise BatchFailedError(
                f"{len(result['batchItemFailures'])} record(s) failed and "
                "enable_partial_batch_failure is False; failing the whole batch"
            )
        return result

    async def _handle_standard_event(self, records: List[dict], context: Any) -> dict:
        """Handle records for a standard (non-FIFO) queue."""
        failures: List[Dict[str, str]] = []

        self._log(
            "info",
            "Processing records in standard queue mode",
            record_count=len(records),
        )

        semaphore = asyncio.Semaphore(self.max_concurrent_messages)

        async def process_with_semaphore(record):
            async with semaphore:
                return await self._handle_record_safe(record, context)

        tasks = [asyncio.create_task(process_with_semaphore(rec)) for rec in records]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for i, result in enumerate(results):
            if isinstance(result, Exception):
                msg_id = records[i].get("messageId", "UNKNOWN")
                self._log(
                    "error",
                    "Record failed",
                    msg_id=msg_id,
                    error_type=type(result).__name__,
                    error=str(result),
                )
                if self.debug:
                    self._log(
                        "debug", "Record failed", msg_id=msg_id, error=str(result)
                    )
                failures.append({"itemIdentifier": msg_id})
            else:
                msg_id = records[i].get("messageId", "UNKNOWN")
                self._log("debug", "Record succeeded", msg_id=msg_id)

        self._log(
            "info",
            "Batch processing completed",
            succeeded=len(records) - len(failures),
            failed=len(failures),
        )

        return {"batchItemFailures": failures}

    async def _handle_fifo_event(self, records: List[dict], context: Any) -> dict:
        """Handle records for a FIFO queue with message-group ordering."""
        if not self.skip_group_on_error:
            return await self._handle_fifo_halt_batch(records, context)

        failures: List[Dict[str, str]] = []

        message_groups = group_records_by_message_group(records)

        if self.debug:
            self._log(
                "info",
                "FIFO processing",
                record_count=len(records),
                group_count=len(message_groups),
            )

        async def process_group(group_id: str, group_records: List[dict]):
            group_failures: List[Dict[str, str]] = []
            if self.debug:
                self._log(
                    "debug",
                    "Processing group",
                    group_id=group_id,
                    record_count=len(group_records),
                )

            for idx, rec in enumerate(group_records):
                try:
                    await self._handle_record(rec, context)
                except Exception as e:
                    msg_id = rec.get("messageId", "UNKNOWN")
                    if self.debug:
                        self._log(
                            "error",
                            "FIFO record failed; halting group to preserve ordering",
                            msg_id=msg_id,
                            group_id=group_id,
                            error=str(e),
                        )
                    # FIFO ordering: a failed message blocks the rest of its
                    # group. Stop here and report this record plus every record
                    # after it as failures so SQS redelivers the tail in order.
                    group_failures.extend(
                        {"itemIdentifier": later.get("messageId", "UNKNOWN")}
                        for later in group_records[idx:]
                    )
                    break

            return group_failures

        group_tasks = [
            asyncio.create_task(process_group(group_id, group_records))
            for group_id, group_records in message_groups.items()
        ]

        group_results = await asyncio.gather(*group_tasks, return_exceptions=True)

        for result in group_results:
            if isinstance(result, list):
                failures.extend(result)
            elif isinstance(result, Exception):
                if self.debug:
                    self._log(
                        "error", "Message group processing failed", error=str(result)
                    )

        return {"batchItemFailures": failures}

    async def _handle_fifo_halt_batch(
        self, records: List[dict], context: Any
    ) -> dict:
        """FIFO with skip_group_on_error=False: process the batch in arrival
        order and halt at the first failure, reporting that record and every
        record after it so SQS redelivers the unprocessed tail (matching AWS
        Powertools' default behaviour)."""
        failures: List[Dict[str, str]] = []
        halted = False
        for rec in records:
            if halted:
                failures.append({"itemIdentifier": rec.get("messageId", "UNKNOWN")})
                continue
            try:
                await self._handle_record(rec, context)
            except Exception as e:
                halted = True
                msg_id = rec.get("messageId", "UNKNOWN")
                if self.debug:
                    self._log(
                        "error",
                        "FIFO batch halted on failure",
                        msg_id=msg_id,
                        error=str(e),
                    )
                failures.append({"itemIdentifier": msg_id})
        return {"batchItemFailures": failures}

    async def _handle_record_safe(self, record: dict, context: Any) -> None:
        """Handle a record, logging and re-raising any failure."""
        msg_id = record.get("messageId", "UNKNOWN")
        try:
            await self._handle_record(record, context)
        except Exception as e:
            self._log(
                "error",
                "Record processing failed",
                msg_id=msg_id,
                error_type=type(e).__name__,
                error=str(e),
            )
            raise

from __future__ import annotations

import asyncio
import json
from typing import Any, Callable, Dict, List, Optional, Type

from .events import SQSEvent
from .types import QueueType, Handler
from .exceptions import RouteNotFound, InvalidMessage
from .middleware import Middleware, run_middlewares
from .middleware.logging import LoggingMiddleware
from .routing import SQSRouter
from .utils import group_records_by_message_group
from .presets import MiddlewarePreset
from .concurrency.concurrency import ThreadPoolManager


class FastSQS:
    """Main FastSQS application class for handling AWS SQS messages.
    
    This class provides a FastAPI-style interface for routing and processing
    SQS messages with support for middleware, validation, and concurrency.
    """

    def __init__(
        self,
        title: str = "FastSQS App",
        description: str = "",
        version: str = "1.0.0",
        debug: bool = False,
        queue_type: QueueType = QueueType.STANDARD,
        message_type_key: str = "type",
        flexible_matching: bool = True,
        max_concurrent_messages: int = 10,
        enable_partial_batch_failure: bool = True,
    ):
        """Initialize FastSQS application.
        
        Args:
            title: Application title
            description: Application description
            version: Application version
            debug: Enable debug mode
            queue_type: SQS queue type (STANDARD or FIFO)
            message_type_key: Key to identify message type in payload
            flexible_matching: Enable flexible message type matching
            max_concurrent_messages: Maximum concurrent message processing
            enable_partial_batch_failure: Enable partial batch failure handling
        """
        self.title = title
        self.description = description
        self.version = version
        self.debug = debug
        self.queue_type = queue_type
        self.message_type_key = message_type_key
        self.flexible_matching = flexible_matching
        self.max_concurrent_messages = max_concurrent_messages
        self.enable_partial_batch_failure = enable_partial_batch_failure

        self._main_router = SQSRouter(
            key=self.message_type_key,
            message_type_key=self.message_type_key,
            flexible_matching=self.flexible_matching,
        )

        self._routers: List[SQSRouter] = []
        self._middlewares: List[Middleware] = []

    def route(
        self,
        event_model: Type[SQSEvent],
        *,
        middlewares: Optional[List[Middleware]] = None,
    ) -> Callable[[Handler], Handler]:
        """Register a route for a specific SQS event model.
        
        Args:
            event_model: Pydantic model class for the event
            middlewares: Optional list of middlewares to apply
            
        Returns:
            Decorator function for the handler
        """
        return self._main_router.route(event_model, middlewares=middlewares)

    def default(self) -> Callable[[Handler], Handler]:
        """Register a default handler for unmatched messages.
        
        Returns:
            Decorator function for the default handler
        """
        return self._main_router.route(None)

    def include_router(self, router: SQSRouter) -> None:
        """Include an external router in the application.
        
        Args:
            router: SQSRouter instance to include
        """
        self._routers.append(router)

    def add_middleware(self, middleware: Middleware) -> None:
        """Add a middleware to the application.
        
        Args:
            middleware: Middleware instance to add
        """
        middleware._app = self
        self._middlewares.append(middleware)

    def use(self, middleware: Middleware) -> None:
        """Alias for add_middleware.
        
        Args:
            middleware: Middleware instance to add
        """
        self.add_middleware(middleware)

    def _log(self, level: str, message: str, **data) -> None:
        """Internal logging method that routes through LoggingMiddleware.
        
        Args:
            level: Log level (info, debug, error, etc.)
            message: Log message
            **data: Additional log data
        """
        for middleware in self._middlewares:
            if isinstance(middleware, LoggingMiddleware) and hasattr(middleware, "log"):
                middleware.log(level, message, **data)
                return

    def use_preset(self, preset: str, **kwargs) -> None:
        """Apply a predefined middleware preset.
        
        Args:
            preset: Preset name (production, development, minimal)
            **kwargs: Additional preset configuration
            
        Raises:
            ValueError: If preset name is unknown
        """
        if preset == "production":
            middlewares = MiddlewarePreset.production(**kwargs)
        elif preset == "development":
            middlewares = MiddlewarePreset.development(**kwargs)
        elif preset == "minimal":
            middlewares = MiddlewarePreset.minimal(**kwargs)
        else:
            raise ValueError(
                f"Unknown preset: {preset}. Available: production, development, minimal"
            )

        for middleware in middlewares:
            self.add_middleware(middleware)

    def set_queue_type(self, queue_type: QueueType) -> None:
        """Set the SQS queue type.
        
        Args:
            queue_type: Queue type (STANDARD or FIFO)
        """
        self.queue_type = queue_type
        if self.debug:
            self._log("info", f"Queue type set to: {queue_type.value}")

    def is_fifo_queue(self) -> bool:
        """Check if the current queue type is FIFO.
        
        Returns:
            True if queue type is FIFO, False otherwise
        """
        return self.queue_type == QueueType.FIFO

    async def _handle_record(self, record: dict, context: Any) -> Optional[Any]:
        """Handle a single SQS record.
        
        Args:
            record: SQS record dictionary
            context: Lambda context object
            
        Returns:
            Handler result if successful, None otherwise
            
        Raises:
            InvalidMessage: If message format is invalid
            RouteNotFound: If no handler found for message
        """
        body_str = record.get("body", "")
        msg_id = record.get("messageId") or record.get("message_id") or "UNKNOWN"

        self._log("info", f"Starting record processing", msg_id=msg_id)
        self._log(
            "debug",
            f"Raw body",
            msg_id=msg_id,
            body=body_str[:500] + ("..." if len(body_str) > 500 else ""),
        )

        try:
            payload = json.loads(body_str) if body_str else {}
            if not isinstance(payload, dict):
                raise InvalidMessage("Message body must be a JSON object")
            self._log("debug", f"Parsed payload", msg_id=msg_id, payload=payload)
        except json.JSONDecodeError as e:
            self._log("error", f"JSON decode error", msg_id=msg_id, error=str(e))
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
            self._log("debug", f"FIFO info", msg_id=msg_id, fifo_info=ctx["fifoInfo"])

        err: Optional[Exception] = None
        result: Any = None

        self._log("debug", f"Running 'before' middleware chain", msg_id=msg_id)
        await run_middlewares(
            self._middlewares, "before", payload, record, context, ctx
        )
        self._log("debug", f"'before' middleware chain completed", msg_id=msg_id)

        try:
            handled = False

            # Try main router first
            self._log("debug", f"Trying main router", msg_id=msg_id)
            if await self._main_router.dispatch(
                payload, record, context, ctx, root_payload=payload
            ):
                self._log("debug", f"Main router handled the message", msg_id=msg_id)
                handled = True
                result = ctx.get("handler_result")

            if not handled and self._routers:
                self._log(
                    "debug",
                    f"Trying routers",
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
                        handled = True
                        result = ctx.get("handler_result")
                        break
                    else:
                        self._log(
                            "debug",
                            f"Router {i} did not handle the message",
                            msg_id=msg_id,
                        )

            if not handled:
                available_routes = list(self._main_router._pydantic_routes.keys())
                available_routers = [r.key for r in self._routers]
                error_msg = (
                    f"No handler found for message. "
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

        except Exception as e:
            self._log(
                "error",
                f"Handler error",
                msg_id=msg_id,
                error_type=type(e).__name__,
                error=str(e),
            )
            err = e
            raise
        finally:
            self._log("debug", f"Running 'after' middleware chain", msg_id=msg_id)
            await run_middlewares(
                self._middlewares, "after", payload, record, context, ctx, err
            )
            self._log("debug", f"'after' middleware chain completed", msg_id=msg_id)

        self._log("info", f"Record processing completed successfully", msg_id=msg_id)
        return result

    def _cleanup_resources(self) -> None:
        """Clean up background tasks."""
        ThreadPoolManager().wait_for_completion()

    async def _handle_event(self, event: dict, context: Any) -> dict:
        """Handle SQS event with multiple records.
        
        Args:
            event: SQS event dictionary containing records
            context: Lambda context object
            
        Returns:
            Dictionary with batch failure information
        """
        records = event.get("Records", [])
        if not records:
            self._cleanup_resources()
            return {"batchItemFailures": []}

        if self.debug:
            queue_info = f"queue_type={self.queue_type.value}, records={len(records)}"
            self._log("info", f"Processing event", queue_info=queue_info)

        if self.is_fifo_queue():
            return await self._handle_fifo_event(records, context)
        else:
            return await self._handle_standard_event(records, context)

    async def _handle_standard_event(self, records: List[dict], context: Any) -> dict:
        """Handle records for standard (non-FIFO) queue.
        
        Args:
            records: List of SQS records
            context: Lambda context object
            
        Returns:
            Dictionary with batch failure information
        """
        failures: List[Dict[str, str]] = []

        self._log(
            "info",
            f"Processing records in standard queue mode",
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
                    f"Record failed",
                    msg_id=msg_id,
                    error_type=type(result).__name__,
                    error=str(result),
                )
                if self.debug:
                    self._log(
                        "debug", f"Record failed", msg_id=msg_id, error=str(result)
                    )
                if self.enable_partial_batch_failure:
                    failures.append({"itemIdentifier": msg_id})
            else:
                msg_id = records[i].get("messageId", "UNKNOWN")
                self._log("debug", f"Record succeeded", msg_id=msg_id)

        self._log(
            "info",
            f"Batch processing completed",
            succeeded=len(records) - len(failures),
            failed=len(failures),
        )
        
        self._cleanup_resources()
        
        return {"batchItemFailures": failures}

    async def _handle_fifo_event(self, records: List[dict], context: Any) -> dict:
        """Handle records for FIFO queue with message group ordering.
        
        Args:
            records: List of SQS records
            context: Lambda context object
            
        Returns:
            Dictionary with batch failure information
        """
        failures: List[Dict[str, str]] = []

        message_groups = group_records_by_message_group(records)

        if self.debug:
            self._log(
                "info",
                f"FIFO processing",
                record_count=len(records),
                group_count=len(message_groups),
            )

        async def process_group(group_id: str, group_records: List[dict]):
            group_failures = []
            if self.debug:
                self._log(
                    "debug",
                    f"Processing group",
                    group_id=group_id,
                    record_count=len(group_records),
                )

            for rec in group_records:
                try:
                    await self._handle_record(rec, context)
                except Exception as e:
                    msg_id = rec.get("messageId", "UNKNOWN")
                    if self.debug:
                        self._log(
                            "error",
                            f"FIFO record failed",
                            msg_id=msg_id,
                            group_id=group_id,
                            error=str(e),
                        )
                    if self.enable_partial_batch_failure:
                        group_failures.append({"itemIdentifier": msg_id})

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
                        "error", f"Message group processing failed", error=str(result)
                    )

        self._cleanup_resources()
        
        return {"batchItemFailures": failures}

    async def _handle_record_safe(self, record: dict, context: Any) -> None:
        """Safely handle a record with error logging.
        
        Args:
            record: SQS record dictionary
            context: Lambda context object
            
        Raises:
            Exception: Re-raises any exception from record handling
        """
        msg_id = record.get("messageId", "UNKNOWN")
        try:
            await self._handle_record(record, context)
        except Exception as e:
            self._log(
                "error",
                f"Record processing failed",
                msg_id=msg_id,
                error_type=type(e).__name__,
                error=str(e),
            )
            raise

    def handler(self, event: dict, context: Any) -> dict:
        """Main synchronous handler entry point for Lambda.
        
        Args:
            event: SQS event dictionary
            context: Lambda context object
            
        Returns:
            Dictionary with batch failure information
            
        Raises:
            RuntimeError: If called from within an event loop
        """
        try:
            asyncio.get_running_loop()
            raise RuntimeError(
                "FastSQS handler called from within event loop. Use async_handler() for testing."
            )
        except RuntimeError as e:
            if "no running event loop" in str(e):
                return asyncio.run(self._handle_event(event, context))
            else:
                raise

    async def async_handler(self, event: dict, context: Any) -> dict:
        """Asynchronous handler entry point for testing.
        
        Args:
            event: SQS event dictionary
            context: Lambda context object
            
        Returns:
            Dictionary with batch failure information
        """
        return await self._handle_event(event, context)

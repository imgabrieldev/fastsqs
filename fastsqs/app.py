from __future__ import annotations

import asyncio
from typing import Any, Callable, List, Literal, Optional, Type

from .events import SQSEvent
from .types import QueueType, Handler
from .middleware import Middleware
from .middleware.logging import LoggingMiddleware
from .routing import SQSRouter
from .processing import RecordProcessingMixin


class FastSQS(RecordProcessingMixin):
    """Main FastSQS application class for handling AWS SQS messages.

    FastAPI-style interface for routing and processing SQS messages with
    support for middleware, validation, and concurrency. Record/batch
    processing lives in RecordProcessingMixin (processing.py).
    """

    def __init__(
        self,
        *,
        debug: bool = False,
        queue_type: QueueType = QueueType.AUTO,
        discriminator: str = "type",
        flexible_matching: bool = False,
        max_concurrent_messages: int = 10,
        partial_batch_failure: bool = True,
        fifo_failure_mode: Literal["isolate_groups", "halt_batch"] = "isolate_groups",
    ):
        """Initialize FastSQS application.

        Args:
            debug: Enable debug logging.
            queue_type: ``AUTO`` (default) infers FIFO vs standard from each
                batch's ``eventSourceARN``; ``STANDARD``/``FIFO`` force it.
            discriminator: Payload key used to route messages (default ``"type"``).
            flexible_matching: Allow fuzzy message-type matching (off by default).
            max_concurrent_messages: Max concurrent records (STANDARD queues only).
            partial_batch_failure: Report per-record failures (ReportBatchItemFailures).
                When False, any failure fails the whole batch so SQS redelivers all.
            fifo_failure_mode: FIFO only. ``"isolate_groups"`` (default): a failed
                message blocks only the rest of its own messageGroupId; other groups
                run independently. ``"halt_batch"``: the first failure halts the whole
                batch (that record and every record after it are reported), matching
                AWS Powertools' default.
        """
        self.debug = debug
        self.queue_type = queue_type
        self.discriminator = discriminator
        self.flexible_matching = flexible_matching
        self.max_concurrent_messages = max_concurrent_messages
        self.partial_batch_failure = partial_batch_failure
        self.fifo_failure_mode = fifo_failure_mode

        self._main_router = SQSRouter(
            discriminator=self.discriminator,
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
        return self._main_router.default()

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
        self._middlewares.append(middleware)

    def _log(self, level: str, message: str, **data) -> None:
        """Route an internal log line through a registered LoggingMiddleware, if any.

        Args:
            level: Log level (info, debug, error, etc.)
            message: Log message
            **data: Additional log data
        """
        for middleware in self._middlewares:
            if isinstance(middleware, LoggingMiddleware):
                middleware.log(level, message, **data)
                return

    def _resolve_queue_type(self, records: List[dict]) -> QueueType:
        """Resolve the effective queue type for a batch.

        When ``queue_type`` is ``AUTO``, infer FIFO from the record's
        ``eventSourceARN`` (``.fifo`` suffix); otherwise honor the explicit type.
        """
        if self.queue_type != QueueType.AUTO:
            return self.queue_type
        if records:
            arn = records[0].get("eventSourceARN", "") or ""
            if arn.endswith(".fifo"):
                return QueueType.FIFO
        return QueueType.STANDARD

    def handler(self, event: dict, context: Any) -> dict:
        """Main synchronous handler entry point for Lambda.

        Args:
            event: SQS event dictionary
            context: Lambda context object

        Returns:
            Dictionary with batch failure information

        Raises:
            RuntimeError: If called from within a running event loop
        """
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(self._handle_event(event, context))
        raise RuntimeError(
            "FastSQS.handler() called inside a running event loop; use async_handler() instead."
        )

    async def async_handler(self, event: dict, context: Any) -> dict:
        """Asynchronous handler entry point for testing.

        Args:
            event: SQS event dictionary
            context: Lambda context object

        Returns:
            Dictionary with batch failure information
        """
        return await self._handle_event(event, context)

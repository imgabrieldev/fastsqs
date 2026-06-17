from __future__ import annotations

import asyncio
from typing import Any, Callable, List, Optional, Type

from .events import SQSEvent
from .types import QueueType, Handler
from .middleware import Middleware
from .middleware.logging import LoggingMiddleware
from .routing import SQSRouter
from .presets import MiddlewarePreset
from .processing import RecordProcessingMixin


class FastSQS(RecordProcessingMixin):
    """Main FastSQS application class for handling AWS SQS messages.

    FastAPI-style interface for routing and processing SQS messages with
    support for middleware, validation, and concurrency. Record/batch
    processing lives in RecordProcessingMixin (processing.py).
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
        skip_group_on_error: bool = True,
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
            skip_group_on_error: FIFO only. True (default): a failed message
                blocks only the rest of its own messageGroupId; other groups
                run independently. False: the first failure halts the whole
                batch (the failed record and every record after it are reported
                as failures), matching AWS Powertools' default.
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
        self.skip_group_on_error = skip_group_on_error

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

    def use_preset(self, preset: str) -> None:
        """Apply a predefined middleware preset.

        Args:
            preset: Preset name (production, development, minimal)

        Raises:
            ValueError: If preset name is unknown
        """
        if preset == "production":
            middlewares = MiddlewarePreset.production()
        elif preset == "development":
            middlewares = MiddlewarePreset.development()
        elif preset == "minimal":
            middlewares = MiddlewarePreset.minimal()
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

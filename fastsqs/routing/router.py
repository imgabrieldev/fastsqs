from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, Dict, Iterable, List, Optional, Type, Union
from pydantic import BaseModel, ValidationError

from ..events import SQSEvent
from ..exceptions import InvalidMessageError
from ..types import Handler, RouteValue
from ..middleware import Middleware
from ..middleware.base import _run_middleware_stack
from ..utils import invoke_handler, maybe_inject
from .entry import _RouteEntry

if TYPE_CHECKING:
    from ..types import Context


class SQSRouter:
    """Router for handling SQS messages with multiple routing strategies.

    Supports both pydantic model-based routing and key-value routing, with
    optional flexible message-type matching and nested subrouters.
    """

    def __init__(
        self,
        base_event_class: Optional[Type[BaseModel]] = None,
        *,
        discriminator: str = "type",
        inherit_middlewares: bool = True,
        flexible_matching: bool = False,
    ):
        """Initialize SQS router.

        Args:
            base_event_class: Optional base event class for validation
            discriminator: Payload key used to route messages (default ``"type"``)
            inherit_middlewares: Whether to inherit parent middlewares
            flexible_matching: Enable fuzzy message-type matching (off by default)
        """
        self.base_event_class = base_event_class
        self.discriminator = discriminator
        self.inherit_middlewares = inherit_middlewares
        self.flexible_matching = flexible_matching

        self._routes: Dict[str, _RouteEntry] = {}
        self._middlewares: List[Middleware] = []
        self._default_handler: Optional[Handler] = None

        self._pydantic_routes: Dict[
            str, tuple[Type[BaseModel], Handler, List[Middleware]]
        ] = {}
        self._route_lookup: Dict[str, str] = {}

    def route(
        self,
        value: Union[RouteValue, Iterable[RouteValue], Type[BaseModel], None] = None,
        *,
        model: Optional[type[BaseModel]] = None,
        middlewares: Optional[List[Middleware]] = None,
    ) -> Callable[[Handler], Handler]:
        """Register a route handler.

        Args:
            value: Route value(s), a Pydantic model class, or None for the default
            model: Optional Pydantic model for validation (key-value routes)
            middlewares: Optional list of middlewares

        Returns:
            Decorator function for the handler

        Raises:
            ValueError: If event model is invalid or a duplicate/colliding route exists
        """
        # Handle pydantic model routing (like FastSQS.route)
        if (
            value is not None
            and isinstance(value, type)
            and issubclass(value, BaseModel)
        ):
            if not issubclass(value, SQSEvent):
                raise ValueError(
                    f"event_model must be a subclass of SQSEvent, got {value}"
                )

            # If a base event class was specified, validate that the event_model
            # is a subclass
            if self.base_event_class is not None:
                if not issubclass(value, self.base_event_class):
                    raise ValueError(
                        f"event_model {value.__name__} must be a subclass "
                        f"of the router's base event class "
                        f"{self.base_event_class.__name__}"
                    )

            primary_type = value.get_message_type()

            def pydantic_decorator(handler: Handler) -> Handler:
                if primary_type in self._pydantic_routes:
                    raise ValueError(
                        f"Handler for message type '{primary_type}' already exists"
                    )
                # Pydantic routes take dispatch precedence over key-value routes,
                # so a key-value handler on the same discriminator value would be
                # unreachable dead code. Fail fast on the cross-registry collision.
                if primary_type in self._routes:
                    raise ValueError(
                        f"'{primary_type}' is already a key-value route; it cannot "
                        "also be a pydantic route (the key-value handler would be "
                        "shadowed). Use one routing style per discriminator value."
                    )

                self._pydantic_routes[primary_type] = (
                    value,
                    maybe_inject(handler),
                    list(middlewares or []),
                )

                if self.flexible_matching:
                    variants = value.get_message_type_variants()
                    for variant in variants:
                        existing = self._route_lookup.get(variant)
                        if existing is None:
                            self._route_lookup[variant] = primary_type
                        elif existing != primary_type:
                            raise ValueError(
                                f"fastsqs: message-type variant '{variant}' maps to "
                                f"both '{existing}' and '{primary_type}'; rename one "
                                "event class or disable flexible_matching"
                            )

                return handler

            return pydantic_decorator

        # Handle default route (no value)
        if value is None:
            return self.default(middlewares=middlewares)

        # Handle string/int value routing
        values = [value] if isinstance(value, (str, int)) else list(value)

        def value_decorator(fn: Handler) -> Handler:
            injected = maybe_inject(fn)
            for v in values:
                k = str(v)
                if k in self._pydantic_routes:
                    raise ValueError(
                        f"'{k}' is already a pydantic route; it cannot also be a "
                        "key-value route (the key-value handler would be shadowed). "
                        "Use one routing style per discriminator value."
                    )
                if k in self._routes:
                    existing = self._routes[k]
                    if existing.handler is not None:
                        raise ValueError(
                            f"Duplicate handler for {self.discriminator}={k}"
                        )
                    existing.handler = injected
                    existing.model = model
                    existing.middlewares = list(middlewares or [])
                else:
                    self._routes[k] = _RouteEntry(
                        handler=injected, model=model, middlewares=list(middlewares or [])
                    )
            return fn

        return value_decorator

    def default(
        self,
        *,
        middlewares: Optional[List[Middleware]] = None,
    ) -> Callable[[Handler], Handler]:
        """Register the default handler for messages that match no route.

        Args:
            middlewares: Optional list of middlewares (currently advisory; the
                default handler runs under the router/app middleware chain).

        Returns:
            Decorator function for the default handler.
        """
        def default_decorator(fn: Handler) -> Handler:
            self._default_handler = maybe_inject(fn)
            return fn

        return default_decorator

    def _find_pydantic_route(
        self, message_type: str
    ) -> Optional[tuple[Type[BaseModel], Handler, List[Middleware]]]:
        """Find a pydantic route by message type.

        Args:
            message_type: Message type to search for

        Returns:
            Tuple of (model_class, handler, middlewares) if found, None otherwise
        """
        if message_type in self._pydantic_routes:
            return self._pydantic_routes[message_type]

        if self.flexible_matching and message_type in self._route_lookup:
            primary_type = self._route_lookup[message_type]
            return self._pydantic_routes[primary_type]

        return None

    def subrouter(
        self,
        value: Union[RouteValue, Iterable[RouteValue]],
        router: Optional["SQSRouter"] = None,
    ) -> Union["SQSRouter", Callable[["SQSRouter"], "SQSRouter"]]:
        """Register a subrouter for nested routing.

        Args:
            value: Route value(s) to associate with subrouter
            router: Optional router instance

        Returns:
            Router instance or decorator function
        """
        values = [value] if isinstance(value, (str, int)) else list(value)

        if router is not None:
            for v in values:
                k = str(v)
                if k in self._routes:
                    self._routes[k].subrouter = router
                else:
                    self._routes[k] = _RouteEntry(subrouter=router)
            return router

        def decorator(
            router_or_fn: Union[SQSRouter, Callable[[], SQSRouter]],
        ) -> SQSRouter:
            if callable(router_or_fn) and not isinstance(router_or_fn, SQSRouter):
                router_instance = router_or_fn()
            else:
                router_instance = router_or_fn

            for v in values:
                k = str(v)
                if k in self._routes:
                    self._routes[k].subrouter = router_instance
                else:
                    self._routes[k] = _RouteEntry(subrouter=router_instance)
            return router_instance

        return decorator

    def add_middleware(self, mw: Middleware) -> None:
        """Add middleware to this router.

        Args:
            mw: Middleware instance to add
        """
        self._middlewares.append(mw)

    async def dispatch(
        self,
        payload: dict,
        record: dict,
        context: Any,
        ctx: "Context",
        root_payload: Optional[dict] = None,
        parent_middlewares: Optional[List[Middleware]] = None,
    ) -> bool:
        """Dispatch a message to the appropriate handler.

        Args:
            payload: Message payload dictionary
            record: SQS record dictionary
            context: Lambda context object
            ctx: Per-record processing Context
            root_payload: Original root payload
            parent_middlewares: Middlewares from parent routers

        Returns:
            True if message was handled, False otherwise

        Raises:
            InvalidMessageError: If message validation fails
        """
        if root_payload is None:
            root_payload = payload

        if parent_middlewares is None:
            parent_middlewares = []

        # First try pydantic-based routing (using the discriminator key).
        # Route through _execute_handler so router-level and per-route
        # middlewares run for pydantic routes exactly as they do for
        # key-value routes (validation + InvalidMessageError handling included).
        message_type = payload.get(self.discriminator)
        if message_type:
            pydantic_route = self._find_pydantic_route(message_type)
            if pydantic_route:
                event_model, handler, route_middlewares = pydantic_route
                ctx.message_type = message_type
                await self._execute_handler(
                    handler,
                    event_model,
                    route_middlewares,
                    payload,
                    record,
                    context,
                    ctx,
                    root_payload,
                    parent_middlewares,
                )
                return True

        # Then try key-value based routing
        key_value = payload.get(self.discriminator)
        if key_value is None:
            # Discriminator missing or explicitly null: let the default handler
            # (if any) catch it, mirroring the "no matching route" path below.
            if self._default_handler:
                await self._execute_handler(
                    self._default_handler,
                    None,
                    [],
                    payload,
                    record,
                    context,
                    ctx,
                    root_payload,
                    parent_middlewares,
                )
                return True
            return False

        str_value = str(key_value)

        route_path = ctx.route_path
        route_path.append(f"{self.discriminator}={str_value}")

        entry = self._routes.get(str_value)

        if entry is None:
            if self._default_handler:
                await self._execute_handler(
                    self._default_handler,
                    None,
                    [],
                    payload,
                    record,
                    context,
                    ctx,
                    root_payload,
                    parent_middlewares,
                )
                return True
            route_path.pop()
            return False

        if entry.is_nested and entry.subrouter:
            if self.inherit_middlewares:
                combined_mws = (
                    parent_middlewares + self._middlewares + entry.middlewares
                )
            else:
                combined_mws = entry.middlewares

            handled = await entry.subrouter.dispatch(
                payload, record, context, ctx, root_payload, combined_mws
            )
            if handled:
                return True
            route_path.pop()
            return False

        if entry.handler:
            await self._execute_handler(
                entry.handler,
                entry.model,
                entry.middlewares,
                payload,
                record,
                context,
                ctx,
                root_payload,
                parent_middlewares,
            )
            return True

        route_path.pop()
        return False

    async def _execute_handler(
        self,
        handler: Handler,
        model: Optional[type[BaseModel]],
        route_middlewares: List[Middleware],
        payload: dict,
        record: dict,
        context: Any,
        ctx: "Context",
        root_payload: dict,
        parent_middlewares: List[Middleware],
    ) -> None:
        """Execute a handler with the middleware chain.

        Args:
            handler: Handler function to execute
            model: Optional Pydantic model for validation
            route_middlewares: Route-specific middlewares
            payload: Message payload
            record: SQS record
            context: Lambda context
            ctx: Per-record processing Context
            root_payload: Original root payload
            parent_middlewares: Parent router middlewares

        Raises:
            InvalidMessageError: If model validation fails
        """
        all_mws = parent_middlewares + self._middlewares + route_middlewares
        handler_payload = root_payload

        async def _invoke() -> Any:
            if model is not None:
                try:
                    msg = model.model_validate(payload)
                except ValidationError as e:
                    raise InvalidMessageError(
                        f"Validation failed for {self.discriminator}: {e}"
                    ) from e
            else:
                msg = SQSEvent.model_validate(payload)

            # invoke_handler matches kwargs to the handler's signature by name,
            # so a single call covers every supported handler shape
            # (msg, ctx, payload, record, context — in any combination/order).
            result = await invoke_handler(
                handler,
                msg=msg,
                payload=handler_payload,
                record=record,
                context=context,
                ctx=ctx,
            )
            ctx.handler_result = result
            return result

        # before -> invoke -> after, unwinding only middlewares that entered.
        await _run_middleware_stack(
            all_mws, handler_payload, record, context, ctx, _invoke
        )

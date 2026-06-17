from __future__ import annotations

import inspect
import warnings
from typing import Any, Callable, Dict, Iterable, List, Optional, Type, Union
from pydantic import BaseModel, ValidationError

from ..events import SQSEvent
from ..exceptions import InvalidMessage
from ..types import Handler, RouteValue
from ..middleware import Middleware, run_middlewares
from ..utils import invoke_handler
from .entry import RouteEntry


class SQSRouter:
    """Router for handling SQS messages with multiple routing strategies.
    
    Supports both key-value based routing and Pydantic model-based routing
    with flexible message type matching and nested routing capabilities.
    """
    
    def __init__(
        self,
        base_event_class: Optional[Type[BaseModel]] = None,
        *,
        key: str = "type",
        name: Optional[str] = None,
        payload_scope: str = "root",
        inherit_middlewares: bool = True,
        message_type_key: str = "type",
        flexible_matching: bool = True,
    ):
        """Initialize SQS router.
        
        Args:
            base_event_class: Optional base event class for validation
            key: Key to use for routing in payload
            name: Optional router name
            payload_scope: Payload scope ('current', 'root', or 'both')
            inherit_middlewares: Whether to inherit parent middlewares
            message_type_key: Key for message type identification
            flexible_matching: Enable flexible message type matching
            
        Raises:
            ValueError: If payload_scope is not valid
        """
        if payload_scope not in ("current", "root", "both"):
            raise ValueError("payload_scope must be 'current', 'root', or 'both'")

        self.base_event_class = base_event_class
        self.key = key
        self.name = name or key
        self.payload_scope = payload_scope
        self.inherit_middlewares = inherit_middlewares
        self.message_type_key = message_type_key
        self.flexible_matching = flexible_matching

        self._routes: Dict[str, RouteEntry] = {}
        self._middlewares: List[Middleware] = []
        self._default_handler: Optional[Handler] = None
        self._wildcard_handler: Optional[Handler] = None

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
            value: Route value(s) or Pydantic model class
            model: Optional Pydantic model for validation
            middlewares: Optional list of middlewares
            
        Returns:
            Decorator function for the handler
            
        Raises:
            ValueError: If event model is invalid or duplicate handler exists
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

                self._pydantic_routes[primary_type] = (
                    value,
                    handler,
                    list(middlewares or []),
                )

                if self.flexible_matching:
                    variants = value.get_message_type_variants()
                    for variant in variants:
                        existing = self._route_lookup.get(variant)
                        if existing is None:
                            self._route_lookup[variant] = primary_type
                        elif existing != primary_type:
                            warnings.warn(
                                f"fastsqs: message-type variant '{variant}' already maps to "
                                f"'{existing}'; ignoring collision from '{primary_type}'",
                                stacklevel=2,
                            )

                return handler

            return pydantic_decorator

        # Handle default route (no value)
        if value is None:

            def default_decorator(fn: Handler) -> Handler:
                self._default_handler = fn
                return fn

            return default_decorator

        # Handle string/int value routing
        values = [value] if isinstance(value, (str, int)) else list(value)

        def value_decorator(fn: Handler) -> Handler:
            for v in values:
                k = str(v)
                if k in self._routes:
                    existing = self._routes[k]
                    if existing.handler is not None:
                        raise ValueError(f"Duplicate handler for {self.key}={k}")
                    existing.handler = fn
                    existing.model = model
                    existing.middlewares = list(middlewares or [])
                else:
                    self._routes[k] = RouteEntry(
                        handler=fn, model=model, middlewares=list(middlewares or [])
                    )
            return fn

        return value_decorator

    def _find_pydantic_route(
        self, message_type: str
    ) -> Optional[tuple[Type[BaseModel], Handler, List[Middleware]]]:
        """Find a pydantic route by message type.
        
        Args:
            message_type: Message type to search for
            
        Returns:
            Tuple of (model_class, handler) if found, None otherwise
        """
        if message_type in self._pydantic_routes:
            return self._pydantic_routes[message_type]

        if self.flexible_matching and message_type in self._route_lookup:
            primary_type = self._route_lookup[message_type]
            return self._pydantic_routes[primary_type]

        return None

    def wildcard(
        self,
        model: Optional[type[BaseModel]] = None,
        middlewares: Optional[List[Middleware]] = None,
    ) -> Callable[[Handler], Handler]:
        """Register a wildcard handler for unmatched routes.
        
        Args:
            model: Optional Pydantic model for validation
            middlewares: Optional list of middlewares
            
        Returns:
            Decorator function for the handler
        """
        def decorator(fn: Handler) -> Handler:
            self._wildcard_handler = fn
            if "*" not in self._routes:
                self._routes["*"] = RouteEntry(
                    handler=fn, model=model, middlewares=list(middlewares or [])
                )
            return fn

        return decorator

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
                    self._routes[k] = RouteEntry(subrouter=router)
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
                    self._routes[k] = RouteEntry(subrouter=router_instance)
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
        ctx: dict,
        root_payload: Optional[dict] = None,
        parent_middlewares: Optional[List[Middleware]] = None,
    ) -> bool:
        """Dispatch a message to the appropriate handler.
        
        Args:
            payload: Message payload dictionary
            record: SQS record dictionary
            context: Lambda context object
            ctx: Processing context dictionary
            root_payload: Original root payload
            parent_middlewares: Middlewares from parent routers
            
        Returns:
            True if message was handled, False otherwise
            
        Raises:
            InvalidMessage: If message validation fails
        """
        if root_payload is None:
            root_payload = payload

        if parent_middlewares is None:
            parent_middlewares = []

        # First try pydantic-based routing (using message_type_key).
        # Route through _execute_handler so router-level and per-route
        # middlewares run for pydantic routes exactly as they do for
        # key-value routes (validation + InvalidMessage handling included).
        message_type = payload.get(self.message_type_key)
        if message_type:
            pydantic_route = self._find_pydantic_route(message_type)
            if pydantic_route:
                event_model, handler, route_middlewares = pydantic_route
                ctx["message_type"] = message_type
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

        # Then try key-value based routing (original logic)
        if self.key not in payload:
            return False

        key_value = payload.get(self.key)
        if key_value is None:
            return False

        str_value = str(key_value)

        route_path = ctx.setdefault("route_path", [])
        route_path.append(f"{self.key}={str_value}")

        entry = self._routes.get(str_value)

        if entry is None and self._wildcard_handler:
            entry = self._routes.get("*")

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
        ctx: dict,
        root_payload: dict,
        parent_middlewares: List[Middleware],
    ) -> None:
        """Execute a handler with middleware chain.
        
        Args:
            handler: Handler function to execute
            model: Optional Pydantic model for validation
            route_middlewares: Route-specific middlewares
            payload: Message payload
            record: SQS record
            context: Lambda context
            ctx: Processing context
            root_payload: Original root payload
            parent_middlewares: Parent router middlewares
            
        Raises:
            ValidationError: If model validation fails
        """
        all_mws = parent_middlewares + self._middlewares + route_middlewares

        if self.payload_scope == "root":
            handler_payload = root_payload
        elif self.payload_scope == "both":
            handler_payload = root_payload
        else:
            handler_payload = payload

        err: Optional[Exception] = None
        await run_middlewares(all_mws, "before", handler_payload, record, context, ctx)

        try:
            if model is not None:
                try:
                    msg = model.model_validate(payload)
                except ValidationError as e:
                    raise InvalidMessage(f"Validation failed for {self.key}: {e}")
            else:
                sig = inspect.signature(handler)
                params = list(sig.parameters.values())

                if params and hasattr(params[0].annotation, "model_validate"):
                    model_class = params[0].annotation
                    try:
                        msg = model_class.model_validate(payload)
                    except ValidationError as e:
                        raise InvalidMessage(
                            f"Validation failed for {model_class.__name__}: {e}"
                        )
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
            ctx["handler_result"] = result

        except Exception as e:
            err = e
            raise
        finally:
            await run_middlewares(
                all_mws, "after", handler_payload, record, context, ctx, err
            )

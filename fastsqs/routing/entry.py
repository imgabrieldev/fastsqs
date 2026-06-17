from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, List, TYPE_CHECKING
from pydantic import BaseModel

from ..types import Handler
from ..middleware import Middleware

if TYPE_CHECKING:
    from .router import SQSRouter


@dataclass
class _RouteEntry:
    """Internal route entry record held by SQSRouter (not part of the public API).

    Attributes:
        handler: Optional handler function for the route
        model: Optional Pydantic model for validation
        middlewares: List of middlewares to apply
        subrouter: Optional nested router
    """

    handler: Optional[Handler] = None
    model: Optional[type[BaseModel]] = None
    middlewares: List[Middleware] = field(default_factory=list)
    subrouter: Optional["SQSRouter"] = None

    @property
    def is_nested(self) -> bool:
        """Check if this route entry has a nested subrouter.

        Returns:
            True if subrouter is present, False otherwise
        """
        return self.subrouter is not None

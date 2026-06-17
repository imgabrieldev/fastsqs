"""Type definitions for FastSQS."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Awaitable, Callable, Dict, Iterator, List, Optional, Union

from pydantic import BaseModel
from typing import TypeVar


class QueueType(Enum):
    """SQS queue type. ``AUTO`` (the default) infers FIFO vs standard from the
    record's ``eventSourceARN`` (a ``.fifo`` suffix means FIFO)."""
    AUTO = "auto"
    STANDARD = "standard"
    FIFO = "fifo"


Handler = Callable[..., Union[None, Awaitable[None], Any]]
"""Type alias for message handler functions."""

RouteValue = Union[str, int]
"""Type alias for route values."""

T = TypeVar('T', bound=BaseModel)
"""Type variable bound to Pydantic BaseModel."""


@dataclass
class FifoInfo:
    """FIFO attributes for a record, parsed from the SQS message attributes."""
    message_group_id: Optional[str] = None
    message_deduplication_id: Optional[str] = None


class State:
    """Mutable per-record scratch namespace for middleware and handlers.

    Both ``ctx.state.foo`` and ``ctx.state["foo"]`` work (Litestar-style). This
    is the ONLY writable surface for arbitrary data — framework-owned fields live
    as typed attributes on :class:`Context` and cannot be clobbered from here.

    ``ctx.state.foo`` raises ``AttributeError`` if unset; use ``ctx.state.get(...)``
    for an optional read.
    """

    __slots__ = ("_data",)

    def __init__(self, data: Optional[Dict[str, Any]] = None) -> None:
        # _data is set via object.__setattr__ so __setattr__ below does not
        # recurse; refactoring this to a plain attribute breaks copy/pickle.
        object.__setattr__(self, "_data", dict(data or {}))

    def __getattr__(self, key: str) -> Any:
        # Only called on attribute miss (never for _data, which __slots__ owns).
        try:
            return self._data[key]
        except KeyError:
            raise AttributeError(key)

    def __setattr__(self, key: str, value: Any) -> None:
        self._data[key] = value

    def __delattr__(self, key: str) -> None:
        try:
            del self._data[key]
        except KeyError:
            raise AttributeError(key)

    def __getitem__(self, key: str) -> Any:
        return self._data[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self._data[key] = value

    def __contains__(self, key: str) -> bool:
        return key in self._data

    def get(self, key: str, default: Any = None) -> Any:
        return self._data.get(key, default)

    def setdefault(self, key: str, default: Any = None) -> Any:
        return self._data.setdefault(key, default)

    def __iter__(self) -> Iterator[str]:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def __repr__(self) -> str:
        return f"State({self._data!r})"


@dataclass(eq=False)
class Context:
    """Per-record processing context passed to handlers and middleware.

    Framework-owned fields are typed, single-read-path attributes
    (``ctx.message_id``, ``ctx.queue_type``, ...). Arbitrary middleware/handler
    scratch goes in the separate :attr:`state` namespace (``ctx.state.foo``),
    so scratch can never collide with or clobber a framework field. Annotate a
    handler param ``ctx: Context`` to get the typing.

    ``eq=False`` gives identity semantics (cheap; avoids deep-equality over
    ``record``/``lambda_context``). Never ``deepcopy`` a Context — ``record`` and
    the Lambda ``lambda_context`` are not safely copyable; thread the one instance.
    """

    message_id: str
    record: dict
    lambda_context: Any
    queue_type: QueueType
    route_path: List[str] = field(default_factory=list)
    message_type: Optional[str] = None
    fifo_info: Optional[FifoInfo] = None
    handler_result: Any = None
    state: State = field(default_factory=State)

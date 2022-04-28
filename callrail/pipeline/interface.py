from typing import Any, Callable
from dataclasses import dataclass, field


@dataclass
class Pipeline:
    name: str
    uri: str
    res_fn: Callable[[dict[str, Any]], list[dict[str, Any]]]
    transform: Callable[[list[dict[str, Any]]], list[dict[str, Any]]]
    schema: list[dict[str, Any]]
    params: dict[str, Any] = field(default_factory=dict)
    params_fn: Callable[..., Callable[[Any], dict]] = lambda *args: lambda _: {}
    id_key: list[str] = field(default_factory=lambda: ["id"])
    cursor_key: str = "_batched_at"

from typing import Callable, Generator, Optional, TypeVar

from psqlgraph.edge import AbstractEdge
from psqlgraph.node import AbstractNode

E = TypeVar("E", bound=AbstractEdge)
N = TypeVar("N", bound=AbstractNode)

def traverse(
    root: N,
    mode: Optional[str] = ...,
    max_depth: Optional[int] = ...,
    edge_pointer: Optional[str] = ...,
    edge_predicate: Callable[[E], bool] | None = ...,
) -> Generator[N, None, None]: ...

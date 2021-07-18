import datetime
from typing import (
    Any,
    Generator,
    Generic,
    List,
    Mapping,
    Optional,
    Protocol,
    Tuple,
    Type,
    TypeVar,
    Union,
)

from mypy_extensions import TypedDict
from sqlalchemy.orm.query import Query
from sqlalchemy.orm.session import Session
from sqlalchemy.sql import schema

from psqlgraph import attributes, base, edge

DST_SRC_ASSOC: str
SRC_DST_ASSOC: str

# E = TypeVar("E", bound=edge.AbstractEdge)
E = edge.AbstractEdge
N = TypeVar("N", bound="AbstractNode")
VT = Union[str, bool, int, float, None, List[str]]
Props = attributes.PropertiesDict
Sysan = attributes.SystemAnnotationDict

class NodeData(TypedDict):
    node_id: str
    label: str
    acl: List[str]
    properties: Props
    system_annotations: Sysan

class EdgePredicate(Protocol):
    def __call__(self, edge: E) -> bool: ...

def reverse_lookup(
    dictionary: Mapping[str, Any], search_val: Any
) -> Generator[Any, None, None]: ...

class NodeAssociationProxyMixin:
    edges_in: List[E]
    edges_out: List[E]
    def get_edges(self) -> Generator[E, None, None]: ...
    @classmethod
    def get_edge_class(cls) -> Type[E]: ...

class AbstractNode(NodeAssociationProxyMixin, base.ExtMixin):
    node_id: str
    _defaults: Props
    __table_args__: Tuple[schema.Constraint, ...]
    def traverse(
        self,
        mode: Optional[str] = ...,
        max_depth: Optional[int] = ...,
        edge_pointer: Optional[str] = ...,
        edge_predicate: Optional[EdgePredicate] = ...,
    ) -> N: ...
    def bfs_children(
        self,
        edge_predicate: Optional[EdgePredicate] = ...,
        max_depth: Optional[int] = ...,
    ) -> N: ...
    def dfs_children(
        self,
        edge_predicate: Optional[EdgePredicate] = ...,
        max_depth: Optional[int] = ...,
    ) -> N: ...
    def __init__(
        self,
        node_id: Optional[str] = ...,
        properties: Optional[Props] = ...,
        acl: List[str] = ...,
        system_annotations: Optional[Sysan] = ...,
        label: str = ...,
        **kwargs: VT
    ) -> None: ...
    def __eq__(self, other: Any) -> bool: ...
    def __ne__(self, other: Any) -> bool: ...
    def __hash__(self) -> int: ...
    def copy(self) -> N: ...
    def to_json(self) -> NodeData: ...
    @classmethod
    def from_json(cls, node_json: NodeData) -> N: ...
    @classmethod
    def get_subclass(cls, label: str) -> Type[N]: ...
    @classmethod
    def get_subclass_named(cls, name: str) -> Type[N]: ...
    def get_history(self, session: Session) -> Query: ...

class Node(base.LocalConcreteBase, AbstractNode, base.ORMBase): ...

class TmpNode:
    node_id: str
    acl: List[str]
    _props: Props
    system_annotations: Sysan
    label: str
    created: datetime.datetime
    def __init__(
        self,
        node_id: str,
        props: Props,
        acl: List[str],
        sysan: Sysan,
        label: str,
        created: datetime.datetime,
    ) -> None: ...

def poly_node(
    node_id: Optional[str] = ...,
    label: Optional[str] = ...,
    acl: Optional[List[str]] = ...,
    system_annotations: Optional[Sysan] = ...,
    properties: Optional[Props] = ...,
) -> N: ...

PolyNode = poly_node

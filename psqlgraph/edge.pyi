from typing import Any, Dict, Generic, List, Optional, Tuple, Type, TypeVar, Union

from mypy_extensions import TypedDict
from sqlalchemy.sql import schema

from psqlgraph import attributes, base, node

KT = str
VT = Union[str, bool, int, float, None, List[str]]
E = TypeVar("E", bound="AbstractEdge")
N = node.AbstractNode
M = node.AbstractNode

Props = Dict[KT, VT]
Sysan = Dict[KT, Any]

class EdgeData(TypedDict):
    src_id: str
    dst_id: str
    src_label: str
    dst_label: str
    label: str
    acl: List[str]
    properties: Props
    system_annotations: Sysan

def id_column(tablename: str) -> schema.Column: ...

class DeclareLastEdgeMixin:
    @classmethod
    def is_abstract_base(cls) -> bool: ...
    @classmethod
    def __declare_last__(cls) -> None: ...

class AbstractEdge(DeclareLastEdgeMixin, base.ExtMixin):
    __dst_class__: Type[M]
    __src_class__: Type[N]
    __src_table__: str
    __dst_table__: str
    __table_args__: Tuple[schema.Constraint, ...]
    dst_id: str
    src_id: str
    src: N
    dst: M
    def to_json(self) -> EdgeData: ...
    @classmethod
    def from_json(cls: Type[E], edge_json: EdgeData) -> E: ...
    def __eq__(self, other: Any) -> bool: ...
    def __ne__(self, other: Any) -> bool: ...
    def __hash__(self) -> int: ...
    @classmethod
    def get_subclass(cls, label: str) -> Type[E]: ...
    @classmethod
    def _get_subclasses_labeled(cls, label: str) -> List[E]: ...
    @classmethod
    def get_unique_subclass(
        cls, src_label: str, label: str, dst_label: str
    ) -> Type[E]: ...
    @classmethod
    def get_node_class(cls) -> Type[N]: ...
    def __init__(
        self,
        src_id: Optional[str] = ...,
        dst_id: Optional[str] = ...,
        acl: Optional[List[str]] = ...,
        properties: attributes.PropertiesDict = ...,
        system_annotations: attributes.SystemAnnotationDict = ...,
        label: Optional[str] = ...,
        src: N = ...,
        dst: M = ...,
        **kwargs: VT
    ) -> None: ...

class Edge(base.LocalConcreteBase, AbstractEdge, base.CommonBase): ...

def poly_edge(
    src_id: Optional[str] = ...,
    dst_id: Optional[str] = ...,
    label: Optional[str] = ...,
    acl: Optional[List[str]] = ...,
    system_annotations: Optional[Sysan] = ...,
    properties: Optional[Props] = ...,
) -> N: ...

PolyEdge = poly_edge

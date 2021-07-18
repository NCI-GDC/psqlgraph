from typing import Any, Dict, Generator, Iterable, List, Optional, Type, Union

from sqlalchemy import engine

# from sqlalchemy
from xlocal import xlocal

from psqlgraph import edge, node
from psqlgraph.attributes import PropertiesDict, SystemAnnotationDict
from psqlgraph.query import GraphQuery
from psqlgraph.session import GraphSession
from psqlgraph.voided_edge import VoidedEdge
from psqlgraph.voided_node import VoidedNode

VT = Union[bool, float, int, str]
E = edge.AbstractEdge
N = node.AbstractNode
DEFAULT_RETRIES: int

class PsqlGraphDriver:
    acceptable_isolation_levels: List[str]
    package_namespace: Optional[str]
    set_flush_timestamps: bool
    engine: engine.Engine
    context: xlocal
    def __init__(
        self, host: str, user: str, password: str, database: str, **kwargs: VT
    ) -> None: ...
    def _new_session(self) -> GraphSession: ...
    def has_session(self) -> bool: ...
    def current_session(self) -> GraphSession: ...
    def session_scope(
        self,
        session: Optional[GraphSession] = ...,
        can_inherit: Optional[bool] = ...,
        must_inherit: Optional[bool] = ...,
    ) -> GraphSession: ...
    def nodes(self, query: Optional[Type[N]] = ...) -> GraphQuery: ...
    def __call__(self, *args: Any, **kwargs: Any) -> GraphQuery: ...
    def edges(self, query: Optional[Type[E]] = ...) -> GraphQuery: ...
    def _configure_driver_mappers(self) -> None: ...
    def voided_nodes(self, query: Type[VoidedNode] = ...) -> GraphQuery: ...
    def voided_edges(self, query: Type[VoidedEdge] = ...) -> GraphQuery: ...
    def set_node_validator(self, node_validator: Any) -> None: ...
    def set_edge_validator(self, edge_validator: Any) -> None: ...
    def get_nodes(
        self, session: Optional[GraphSession] = ..., batch_size: int = ...
    ) -> Generator[Iterable[N], None, None]: ...
    def get_edges(
        self, session: Optional[GraphSession] = ..., batch_size: int = ...
    ) -> Generator[Iterable[E], None, None]: ...
    def get_node_count(self, session: Optional[GraphSession] = ...) -> int: ...
    def get_edge_count(self, session: Optional[GraphSession] = ...) -> int: ...
    def node_merge(
        self,
        node_id: Optional[str] = ...,
        node: Optional[N] = ...,
        acl: Optional[List[str]] = ...,
        label: Optional[str] = ...,
        system_annotations: Optional[Dict[str, Any]] = ...,
        properties: Optional[Dict[str, VT]] = ...,
        session: Optional[GraphSession] = ...,
        max_retries: int = ...,
        backoff: int = ...,
    ) -> N: ...
    def node_insert(self, node: N, session: Optional[GraphSession] = ...) -> None: ...
    def node_update(
        self,
        node: N,
        system_annotations: Optional[Dict[str, Any]] = ...,
        acl: Optional[List[str]] = ...,
        properties: Optional[Dict[str, VT]] = ...,
        session: Optional[GraphSession] = ...,
    ) -> None: ...
    def node_lookup(
        self,
        node_id: str = ...,
        property_matches: Optional[Dict[str, VT]] = ...,
        label: Optional[str] = ...,
        system_annotation_matches: Optional[Dict[str, Any]] = ...,
        voided: bool = ...,
        session: Optional[GraphSession] = ...,
    ) -> N: ...
    def node_lookup_one(self, *args: Any, **kwargs: Any) -> N: ...
    def node_lookup_by_id(
        self, node_id: str, voided: bool = ..., session: Optional[GraphSession] = ...
    ) -> GraphQuery: ...
    def node_lookup_by_matches(
        self,
        property_matches: Optional[Dict[str, VT]] = ...,
        system_annotation_matches: Optional[Dict[str, Any]] = ...,
        label: Optional[str] = ...,
        voided: bool = ...,
        session: Optional[GraphSession] = ...,
    ) -> GraphQuery: ...
    def node_clobber(
        self,
        node_id: Optional[str] = ...,
        node: Optional[N] = ...,
        acl: Optional[List[str]] = ...,
        system_annotations: Optional[Dict[str, Any]] = ...,
        properties: Optional[Dict[str, VT]] = ...,
        session: Optional[GraphSession] = ...,
        max_retries: int = ...,
        backoff: int = ...,
    ) -> None: ...
    def node_delete_property_keys(
        self,
        property_keys: Optional[Iterable[str]],
        node_id: Optional[str] = ...,
        node: Optional[N] = ...,
        session: Optional[GraphSession] = ...,
        max_retries: Optional[int] = ...,
        backoff: Optional[int] = ...,
    ) -> None: ...
    def node_delete_system_annotation_keys(
        self,
        system_annotation_keys: Iterable[str],
        node_id: Optional[str] = ...,
        node: Optional[N] = ...,
        session: Optional[GraphSession] = ...,
        max_retries: Optional[int] = ...,
        backoff: Optional[int] = ...,
    ) -> None: ...
    def node_delete(
        self,
        node_id: Optional[str] = ...,
        node: Optional[N] = ...,
        session: Optional[GraphSession] = ...,
        max_retries: Optional[int] = ...,
        backoff: Optional[int] = ...,
    ) -> None: ...
    def edge_insert(
        self,
        edge: E,
        max_retries: Optional[int] = ...,
        backoff: Optional[int] = ...,
        session: Optional[GraphSession] = ...,
    ) -> E: ...
    def edge_update(
        self,
        edge: E,
        system_annotations: Optional[Dict[str, Any]] = ...,
        properties: Optional[Dict[str, VT]] = ...,
        session: Optional[GraphSession] = ...,
    ) -> E: ...
    def edge_lookup_one(
        self,
        src_id: Optional[str] = ...,
        dst_id: Optional[str] = ...,
        label: Optional[str] = ...,
        voided: bool = ...,
        session: Optional[GraphSession] = ...,
    ) -> E: ...
    def edge_lookup(
        self,
        src_id: Optional[str] = ...,
        dst_id: Optional[str] = ...,
        label: Optional[str] = ...,
        voided: bool = ...,
        session: Optional[GraphSession] = ...,
    ) -> GraphQuery: ...
    def edge_lookup_voided(
        self,
        src_id: Optional[str] = ...,
        dst_id: Optional[str] = ...,
        label: Optional[str] = ...,
        session: Optional[GraphSession] = ...,
    ) -> E: ...
    def edge_delete(self, edge: E, session: Optional[GraphSession] = ...) -> None: ...
    def edge_delete_by_node_id(
        self, node_id: str, session: Optional[GraphSession] = ...
    ) -> None: ...
    def get_edge_by_labels(
        self, src_label: str, edge_label: str, dst_label: str
    ) -> Type[E]: ...
    def get_PsqlEdge(
        self,
        src_id: Optional[str] = ...,
        dst_id: Optional[str] = ...,
        label: Optional[str] = ...,
        acl: Optional[List[str]] = ...,
        system_annotations: Optional[SystemAnnotationDict] = ...,
        properties: Optional[PropertiesDict] = ...,
        src_label: Optional[str] = ...,
        dst_label: Optional[str] = ...,
    ) -> E: ...
    def reload(self, *entities: Union[E, N]) -> List[N]: ...

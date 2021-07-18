from typing import (
    Any,
    Dict,
    Generic,
    Iterable,
    List,
    Optional,
    Protocol,
    Set,
    Tuple,
    TypeVar,
    Union,
)

from mypy_extensions import TypedDict

from psqlgraph import edge, node

KT = Union[bool, float, int, list, str, None]
VT = TypeVar("VT", bool, float, int, List[str], str, None)
# E = TypeVar("E", bound=AbstractEdge)
# N = TypeVar("N", bound=AbstractNode)

E = edge.AbstractEdge
N = node.AbstractNode
# R = TypeVar("R", bound="Randomizer")

class DataModel(Protocol):
    Edge: E
    Node: N

class SchemaMeta(TypedDict): ...

class Dictionary(Protocol):
    schema: SchemaMeta

class EdgeMeta(TypedDict):
    src: str
    dst: str

class NodeMeta(TypedDict):
    node_id: str
    label: str
    acl: List[str]
    system_annotations: Dict[str, Any]

class Randomizer(Generic[VT]):
    exhausted: bool
    def __init__(self) -> None: ...
    def next(self) -> VT: ...
    def exhaust(self) -> None: ...
    def random_value(self, override: Optional[VT] = ...) -> VT: ...
    def validate_value(self, value: VT) -> bool: ...

class EnumRand(Randomizer[str]):
    values: List[str]
    def __init__(self, values: List[str]) -> None: ...

class NumberRand(Randomizer[Union[float, int]]):
    minimum: Union[float, int]
    maximum: Union[float, int]
    def __init__(self, type_def: Dict[str, Any]) -> None: ...

class StringRand(Randomizer[str]):
    pattern: str
    def __init__(self, type_def: Dict[str, Any]) -> None: ...

class BooleanRand(Randomizer[bool]): ...

class ArrayRand(Randomizer[List[str]]):
    item_randomizer: Randomizer[Any]
    def __init__(self, item_randomizer: Randomizer[Any]) -> None: ...

class TypeRandFactory:
    @staticmethod
    def get_randomizer(type_def: Dict[str, Any]) -> Randomizer[Any]: ...
    @staticmethod
    def resolve_type(definition: Dict[str, Any]) -> Randomizer[Any]: ...

class PropertyFactory:
    properties: Dict[str, KT]
    type_factories: Dict[str, Randomizer[Any]]
    def __init__(self, properties: Dict[str, KT]) -> None: ...
    def create(self, name: str, override: Optional[KT] = ...) -> Tuple[str, KT]: ...

class NodeFactory:
    models: DataModel
    schema: SchemaMeta
    property_factories: Dict[str, PropertyFactory]
    graph_globals: Dict[str, KT]
    def __init__(
        self,
        models: DataModel,
        schema: SchemaMeta,
        graph_globals: Optional[Dict[str, KT]] = ...,
    ) -> None: ...
    def create(
        self, label: str, override: Optional[Dict[str, KT]] = ..., all_props: bool = ...
    ) -> N: ...
    def get_global_value(self, prop: str) -> KT: ...
    def validate_override_value(self, prop: str, label: str, override: KT) -> bool: ...

class GraphFactory:
    models: DataModel
    dictionary: Dictionary
    node_factory: NodeFactory
    relation_cache: Dict[str, Set[str]]
    def __init__(
        self,
        models: DataModel,
        dictionary: Dictionary,
        graph_globals: Optional[Dict[str, KT]] = ...,
    ) -> None: ...
    @staticmethod
    def validate_nodes_metadata(nodes: Iterable[NodeMeta], unique_key: str) -> None: ...
    @staticmethod
    def validate_edges_metadata(edges: Iterable[EdgeMeta]) -> None: ...
    def create_from_nodes_and_edges(
        self,
        nodes: Iterable[NodeMeta],
        edges: Iterable[EdgeMeta],
        unique_key: str = ...,
        all_props: bool = ...,
    ) -> List[N]: ...
    def create_random_subgraph(
        self,
        label: str,
        max_depth: int = ...,
        leaf_labels: Iterable[str] = ...,
        skip_relations: Iterable[str] = ...,
        all_props: bool = ...,
    ) -> List[N]: ...
    def is_parent_relation(self, label: str, relation: str) -> bool: ...
    @staticmethod
    def make_association(node1: N, node2: N) -> None: ...

from typing import DefaultDict, Optional, Tuple, Type, TypeVar, Union

from mypy_extensions import TypedDict
from sqlalchemy.ext import declarative

from psqlgraph import edge, node

E = TypeVar("E", bound=edge.AbstractEdge)
N = TypeVar("N", bound=node.AbstractNode)

class BaseType(TypedDict):
    node: Type[node.AbstractNode]
    edge: Type[edge.AbstractEdge]

BASE_CLASSES: DefaultDict[Union[str, None], BaseType]
ORM_BASES: DefaultDict[str, declarative.DeclarativeMeta]

def get_orm_base(package_namespace: str) -> declarative.DeclarativeMeta: ...
def get_abstract_edge(package_namespace: Optional[str] = ...) -> Type[E]: ...
def get_abstract_node(package_namespace: Optional[str] = ...) -> Type[N]: ...
def get_class_prefix(pkg_namespace: str) -> str: ...
def create_base_class(pkg_namespace: str, is_node: bool = ...) -> Type[Union[E, N]]: ...
def register_base_class(
    package_namespace: Optional[str] = ...,
) -> Tuple[Type[N], Type[E]]: ...

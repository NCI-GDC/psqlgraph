import datetime
from typing import Any, Dict, List, Mapping, Optional, Tuple, Type, TypeVar, Union

from sqlalchemy.engine import Engine
from sqlalchemy.ext import declarative

from psqlgraph.attributes import PropertiesDict, SystemAnnotationDict
from psqlgraph.session import GraphSession

NODE_TABLENAME_SCHEME: str
EDGE_TABLENAME_SCHEME: str

KT = str
VT = Union[bool, float, int, list, str, None]
C = TypeVar("C", bound="CommonBase")
Props = Dict[KT, VT]
Sysan = SystemAnnotationDict

class CommonBase:
    __pg_properties__: Dict[str, Tuple[Type[VT]]]
    __tablename__: str

    _props: Dict[KT, VT]
    props: PropertiesDict
    properties: PropertiesDict

    _sysan: Dict[KT, Any]
    sysan: SystemAnnotationDict
    system_annotations: SystemAnnotationDict

    label: str
    acl: List[str]
    created: datetime.datetime
    @classmethod
    def __mapper_args__(cls) -> Mapping[str, VT]: ...
    def __getitem__(self, key: KT) -> VT: ...
    def __setitem__(self, key: KT, val: VT) -> None: ...
    def property_template(self, properties: Optional[Props] = ...) -> Props: ...
    @classmethod
    def get_property_list(cls) -> List[str]: ...
    @classmethod
    def has_property(cls, key: str) -> bool: ...
    def get_name(self) -> str: ...
    def get_session(self) -> Optional[GraphSession]: ...
    def merge(
        self,
        acl: Optional[List[str]] = ...,
        system_annotations: Optional[Sysan] = ...,
        properties: Optional[Props] = ...,
    ) -> None: ...
    @classmethod
    def get_pg_properties(cls) -> Dict[str, Tuple[Type[VT]]]: ...
    @classmethod
    def get_label(cls) -> str: ...

class VoidedBaseClass:
    props: Props
    sysan: Sysan

VoidedBase: declarative.DeclarativeMeta
ORMBase: declarative.DeclarativeMeta

def create_all(engine: Engine, base: Optional[declarative.DeclarativeMeta]) -> None: ...
def drop_all(engine: Engine, base: Optional[declarative.DeclarativeMeta]) -> None: ...

class ExtMixin(CommonBase):
    @classmethod
    def is_subclass_loaded(cls, name: str) -> bool: ...
    @classmethod
    def add_subclass(cls, subclass: Type[C]) -> None: ...
    @classmethod
    def get_subclasses(cls) -> List[Type[C]]: ...
    @classmethod
    def get_subclass_table_names(cls) -> List[str]: ...
    @classmethod
    def is_abstract_base(cls) -> bool: ...

class LocalConcreteBase(declarative.AbstractConcreteBase): ...

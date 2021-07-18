import datetime
from typing import Any, Dict, List, Mapping, TypeVar, Union

from psqlgraph.base import VoidedBaseClass
from psqlgraph.node import AbstractNode

KT = str
VT = Union[str, bool, int, float, None, List[str]]

class VoidedNode(VoidedBaseClass):

    __tablename__: str
    key: int
    node_id: str
    src_id: str
    dst_id: str
    acl: List[str]
    label: str
    system_annotations: Dict[KT, Any]
    properties: Dict[KT, VT]
    created: datetime.datetime
    voided: datetime.datetime
    def __init__(self, node: AbstractNode) -> None: ...

import datetime
from typing import Any, Dict, List, Union

from psqlgraph.base import VoidedBaseClass
from psqlgraph.edge import AbstractEdge

KT = str
VT = Union[str, bool, int, float, None, List[str]]

class VoidedEdge(VoidedBaseClass):

    __tablename__: str
    key: int
    src_id: str
    dst_id: str
    acl: List[str]
    label: str
    system_annotations: Dict[KT, Any]
    properties: Dict[KT, VT]
    created: datetime.datetime
    voided: datetime.datetime
    def __init__(self, edge: AbstractEdge) -> None: ...

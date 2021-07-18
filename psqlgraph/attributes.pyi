from abc import abstractmethod
from typing import Any, Dict, Union

from psqlgraph.base import CommonBase

KT = str
VT = Union[bool, float, int, list, str, None]
C = CommonBase

class PropertiesDictError(Exception): ...

class JsonProperty(Dict[KT, VT]):
    def __setitem__(self, key: KT, value: VT) -> None: ...
    @abstractmethod
    def set_item(self, key: KT, value: VT) -> None: ...

class SystemAnnotationDict(JsonProperty):
    source: C
    def __init__(self, source: C) -> None: ...
    def set_item(self, key: KT, value: Any) -> None: ...

class PropertiesDict(JsonProperty):
    source: C
    def __init__(self, source: C) -> None: ...
    def set_item(self, key: KT, value: VT) -> None: ...

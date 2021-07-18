from typing import Any, Callable, Type

from sqlalchemy.orm.session import Session

from psqlgraph import exc as exc
from psqlgraph.query import GraphQuery

def inherit_docstring_from(cls: Type[Any]) -> Callable[..., Any]: ...

class GraphSession(Session):
    __doc__: str
    package_namespace: str
    _psqlgraph_closed: bool
    def __init__(self, *args: Any, **kwargs: Any) -> None: ...
    def connection(self, *args: Any, **kwargs: Any) -> Any: ...
    def close(self, *args: Any, **kwargs: Any) -> None: ...
    def query(self, *entities: Any, **kwargs: Any) -> GraphQuery: ...

"""A module for managing various attributes for graph entities."""

from __future__ import annotations

from abc import abstractmethod
from collections.abc import Callable, Container, Sequence
from typing import Any, Generic, Protocol, TypeVar

from sqlalchemy.ext import hybrid
from typing_extensions import override

from psqlgraph import util

T = TypeVar("T")


class _HasProps(Protocol):
    @property
    def _props(self) -> dict[str, Any]: ...


class PGProperty(Generic[T], hybrid.hybrid_property):
    __slots__ = ("_enum", "_name", "_setter", "_types")

    def __init__(
        self,
        setter: Callable[[_HasProps, T | None], None],
        types: tuple[type[T], ...],
        enum: Container[T],
    ) -> None:
        """An extension of sqlalchemy's hybrid properties for values stored in _props.

        This class generates a hybrid property with:
            - A setter based on the given setter. The given setter is wrapped by a
              validation call to insure any value passed to it is of the given types
              and/or one of the values in the enum if either is set.
            - A getter for retrieving values stored in _props is based on the name of
              the setter.
            - An expression statement is also included which ensures values of a known
              type can be used in sqlalchemy/psqlgraph queries. In cases where multiple
              types are specified, an explicit casting must be performed. EXAMPLES:
                - `g.nodes(Node.foo).filter(Node.bar == "x")` where Node.bar is a
                   string.
                - `g.nodes(Node.foo).filter(Node.bar.as_integer() == 1)` where Node.bar
                   is float or int.

        Args:
            setter: The setter based upon which a getter and expression statement can be
                generated.
            types: A tuple of valid types that the property can be set to. `None` is
                always a valid value and an empty sequence is considered to be `Any`.
            enum: A sequence of values which represent the ONLY valid values to which
                the property may be set. If the value is None then it is considered
        """
        self._name = setter.__name__
        self._setter = setter
        self._types = types
        self._enum = enum

        super().__init__(self.__pg_getter__, self.__pg_setter__, expr=self.__pg_expression__)

    @override
    def __get__(self, instance, owner) -> T | None:
        return super().__get__(instance, owner)

    @override
    def __set__(self, instance, value: T | None) -> None:
        return super().__set__(instance, value)

    @property
    def types(self) -> tuple[type[T], ...]:
        return self._types

    def __pg_getter__(self, instance: _HasProps) -> T | None:
        """A getter for the property to use in the base class.

        Args:
            instance: The instance of the parent object.

        Returns:
            The value of the property or None if not set.
        """
        return instance._props.get(self._name)

    def __pg_setter__(self, instance: _HasProps, value: T | None) -> None:
        """A setter for the property to use in the base class.

        Args:
            instance: The instance of the parent object.
            value: The value to which the property should be set.
        """
        util.validate(self._setter, value, self._types, self._enum)
        self._setter(instance, value)

    def __pg_expression__(self, entity: type[_HasProps]):
        """An expression function for interacting with the class level property.

        Args:
            entity: The class which is the parent of the property.

        Returns:
            An expression statement which can be used in select & filter statements.
        """
        expr = entity._props[self._name]  # type: ignore

        if self.types == (str,):
            expr = expr.as_string()
        elif self.types == (float,):
            expr = expr.as_float()
        elif self.types == (int,):
            expr = expr.as_integer()
        elif self.types == (bool,):
            expr = expr.as_boolean()

        return expr


def pg_property(
    *types: type[T], enum: Sequence[T] = ()
) -> Callable[[Callable[[Any, T | None], None]], PGProperty[T]]:
    """Wraps a property in a PGProperty/hybrid_property.

    See `PGProperty` for full details of functionality.

    Args:
        types: The valid types to which the property can be set. None is always
            considered a valid type.
        enum: A sequence of values which are considered the only valid options to which
            the property can be set. None is always considered a valid option.

    Returns:
        A wrapper for the property method.
    """

    def wrapper(setter: Callable[[_HasProps, T | None], None]) -> PGProperty[T]:
        return PGProperty(setter, types, enum)

    return wrapper


class PropertiesDictError(Exception):
    pass


class JsonProperty(dict):
    """Handles unicode to str conversion while retrieving properties"""

    def __setitem__(self, key, value):
        self.set_item(key, value)
        super().__setitem__(key, value)

    @abstractmethod
    def set_item(self, key, value):
        pass


class SystemAnnotationDict(JsonProperty):
    """Transparent wrapper for _sysan so you can update it as
    if it were a dict and the changes get pushed to the sqlalchemy object

    """

    def __init__(self, source):
        self.source = source
        super().__init__(util.sanitize(source._sysan))

    def update(self, system_annotations=None, **kwargs):
        if system_annotations == self:
            return

        system_annotations = system_annotations or {}
        system_annotations = util.sanitize(system_annotations)
        temp = util.sanitize(self.source._sysan)
        temp.update(system_annotations)
        self.source._sysan = temp
        super().update(self.source._sysan)

    def set_item(self, key, val):
        temp = dict(self.source._sysan)
        temp[key] = val
        self.source.system_annotations = temp

    def __delitem__(self, key):
        del self.source._sysan[key]
        self.update()


class PropertiesDict(JsonProperty):
    """Transparent wrapper for _props so you can update it as
    if it were a dict and the changes get pushed to the sqlalchemy object

    """

    def __init__(self, source):
        self.source = source
        super().__init__(source.property_template(source._props))

    def update(self, properties=None, **kwargs):
        if properties == self:
            return

        properties = properties or {}
        properties = util.sanitize(properties)
        for key, val in properties.items():
            if not self.source.has_property(key):
                raise AttributeError(f"{self.source} has no property {key}")
            setattr(self.source, key, val)
        super().update(self.source._props)

    def set_item(self, key, val):
        setattr(self.source, key, val)

    def __delitem__(self, key):
        raise RuntimeError("You cannot delete ORM properties, only void them.")

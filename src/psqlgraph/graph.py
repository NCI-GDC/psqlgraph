import copy
import inspect
import types
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from typing import Any, ClassVar, Optional, Protocol, TypeVar, Union, cast

import more_itertools
import sqlalchemy
from sqlalchemy import event, orm, sql
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext import associationproxy, declarative, hybrid
from sqlalchemy.sql import schema, sqltypes
from typing_extensions import Self

from psqlgraph import attributes, session, traversals, util, voided

# TODO: What are the "Any"s?
Hook = Callable[[object, session.Session, Any, Any], None]

Base = orm.declarative_base()


def _set_properties(instance: "CommonBase", value: Mapping[str, Any]) -> None:
    for item in util.sanitize(value).items():
        setattr(instance, *item)


T = TypeVar("T")


class HybridProperty(Protocol[T]):
    def __get__(self, obj, objtype=None) -> T: ...

    def __set__(self, value: T) -> None:
        pass


class HybridDictProperty:
    def __get__(self, obj, objtype=None) -> dict[str, Any]: ...

    def __set__(self, value: Mapping[str, Any]) -> None:
        pass


def _create_hybrid_property(name, fset) -> HybridProperty:
    def getter(instance):
        # Note: this does not use an 'in' clause or a .get() with a
        # default because that doesn't allow you to use
        # Node.property_key in a filter on a query.
        try:
            return instance._props[name]
        except KeyError:
            return None

    def setter(instance, value):
        util.validate(fset, value, fset.__pg_types__, fset.__pg_enum__)
        fset(instance, value)

    return cast(HybridProperty, hybrid.hybrid_property(getter, setter))


@orm.declarative_mixin
class CommonBase:
    __pg_properties__: ClassVar[dict[str, tuple[type, ...]]]
    __tablename_scheme__: ClassVar[str]
    __is_abstract_base__: ClassVar[bool]

    _session_hooks_before_insert: ClassVar[Iterable[Hook]] = ()
    _session_hooks_before_update: ClassVar[Iterable[Hook]] = ()
    _session_hooks_before_delete: ClassVar[Iterable[Hook]] = ()

    def __init_subclass__(cls, *, is_abstract_base: bool = False) -> None:
        cls.__pg_properties__ = {}
        cls.__is_abstract_base__ = is_abstract_base

    @classmethod
    def __declare_last__(cls) -> None:
        pg_properties = (
            (k, v) for k, v in vars(cls).items() if getattr(v, "__pg_setter__", False)
        )

        for name, property in pg_properties:
            h_prop = _create_hybrid_property(name, property)
            setattr(cls, name, h_prop)
            cls.__pg_properties__[name] = property.__pg_types__

    @classmethod
    def _get_tablename(cls, name: str) -> str:
        if not name:
            return ""

        return cls.__tablename_scheme__.format(class_name=name.lower())

    @declarative.declared_attr
    def __tablename__(cls) -> str:
        return cls._get_tablename(cls.__name__)

    # ======== Columns ========
    """
    TODO: In sqlalchemy 2.0, these can be updated e.g.:

    created: orm.Mapped[str] = orm.mapped_column(
        sqltypes.DateTime(timezone=True),
        nullable=False,
        server_default=expression.text("now()"),
    )

    """
    created = sqlalchemy.Column(
        sqltypes.DateTime(timezone=True),
        nullable=False,
        server_default=sql.func.now(),
    )
    acl = cast(
        list[str],
        schema.Column(
            postgresql.ARRAY(sqltypes.Text),
            default=list,
        ),
    )
    _sysan = cast(
        dict[str, Any],
        schema.Column(
            # WARNING: Do not update this column directly. See
            # `.system_annotations`
            postgresql.JSONB,
            server_default="{}",
        ),
    )
    _props = cast(
        dict[str, Any],
        schema.Column(
            # WARNING: Do not update this column directly.
            # See `.properties` or `.props`
            postgresql.JSONB,
            server_default="{}",
        ),
    )

    def __init__(self) -> None:
        self._props = {}
        self._sysan = {}
        self.acl = []

    # === Table Attributes ===
    @declarative.declared_attr
    def __mapper_args__(cls):
        return {
            "polymorphic_identity": cls.__tablename__,
            "concrete": True,
        }

    # === Hybrid Properties ===

    properties = cast(
        HybridDictProperty,
        hybrid.hybrid_property(
            lambda self: attributes.PropertiesDict(self),
            lambda self, value: _set_properties(self, value),
        ),
    )
    props = cast(
        HybridDictProperty,
        hybrid.hybrid_property(
            lambda self: self.properties, lambda self, value: setattr(self, "properties", value)
        ),
    )
    system_annotations = cast(
        HybridDictProperty,
        hybrid.hybrid_property(
            lambda self: attributes.SystemAnnotationDict(self),
            lambda self, value: setattr(self, "_sysan", util.sanitize(value)),
        ),
    )
    sysan = cast(
        HybridDictProperty,
        hybrid.hybrid_property(
            lambda self: self.system_annotations,
            lambda self, value: setattr(self, "system_annotations", value),
        ),
    )

    # === Properties ===
    def _set_property(self, key, val):
        """Property dict is cloned (to make sure that SQLAlchemy flushes it)
        before setting the key value pair.

        """
        if not self.has_property(key):
            raise KeyError(f"{type(self)} has no property {key}")

        # Force the object to be marked as dirty.
        self._props = copy.copy(self._props)
        self._props[key] = val

    def _get_property(self, key):
        """If the property is defined in the model but not present on the
        instance, return None, else return the value associated with key.

        """
        if not self.has_property(key):
            raise KeyError(f"{type(self)} has no property {key}")
        if key not in self._props:
            return None
        return self._props[key]

    def property_template(
        self, properties: Mapping[str, Any] = types.MappingProxyType({})
    ) -> dict[str, Any]:
        """Returns a dictionary of {key: None} templating all of the
        properties defined on the model.

        """
        properties = properties or {}
        temp = {k: None for k in self.get_properties()}
        temp.update(properties)
        return temp

    def __getitem__(self, key: str) -> Any:
        """Returns value corresponding to key in _props"""
        return getattr(self, key)

    def __setitem__(self, key: str, val: Any) -> None:
        """Sets value corresponding to key in _props.  This calls the model's
        hybrid_property setter method in the instance's model class.

        """
        setattr(self, key, val)

    @classmethod
    def get_properties(cls) -> Sequence[str]:
        """Returns a list of hybrid_properties defined on the subclass model"""
        return tuple(cls.__pg_properties__)

    @classmethod
    def has_property(cls, key: str) -> bool:
        """Returns boolean if key is a property defined on the subclass model"""
        return key in cls.__pg_properties__

    # === LABEL ===

    @classmethod
    def get_label(cls) -> str:
        return getattr(cls, "__label__", cls.__name__.lower())

    @declarative.declared_attr
    def label(cls) -> str:
        return cls.get_label()

    # ======== System Annotations ========

    def get_name(self) -> str:
        """Convenience wrapper for getting class name"""
        return type(self).__name__

    def get_session(self) -> orm.Session:
        """Returns the session an object is bound to if bound to a session"""
        return orm.object_session(self)

    def merge(
        self,
        acl: Iterable[str] = (),
        system_annotations: Mapping[str, Any] = types.MappingProxyType({}),
        properties: Mapping[str, Any] = types.MappingProxyType({}),
    ) -> None:
        """Merge the model's system_annotations and properties.

        .. note: acl will be overwritten, merging acls is not supported
        """
        self.system_annotations.update(system_annotations)

        for item in properties.items():
            setattr(self, *item)

        if acl := list(acl):
            self.acl = acl

    def _merge_onto_existing(
        self, old_props: Mapping[str, Any], old_sysan: Mapping[str, Any]
    ) -> None:
        # properties
        temp: dict[str, Any] = {}
        temp.update(old_props)
        temp.update(self._props)
        self._props = temp

        # system annotations
        temp: dict[str, Any] = {}
        temp.update(old_sysan)
        temp.update(self._sysan)
        self._sysan = temp

    def _get_clean_session(self, session=None):
        """Create a new session from an objects session using the same
        connection to allow for clean queries against the database

        """
        if not session:
            session = self.get_session()
        Clean = orm.sessionmaker()
        Clean.configure(bind=session.bind)
        return Clean()

    def _validate(self, session: Optional[orm.Session] = None) -> None:
        """Final validation currently only includes checking nonnull
        properties

        """
        for key in getattr(self, "__nonnull_properties__", []):
            assert self.properties[key] is not None, (
                "Null value in key '{}' violates non-null constraint for {}."
            ).format(key, self)

    @classmethod
    def get_pg_properties(cls) -> Mapping[str, tuple[type, ...]]:
        return cls.__pg_properties__

    @classmethod
    def is_subclass_loaded(cls, name: str) -> bool:
        return name in frozenset(c.__name__ for c in cls.get_subclasses())

    @classmethod
    def get_subclasses(cls) -> Sequence[type[Self]]:
        """Limits the scope of subclasses to only those manually specified, else defaults to actual subclasses"""
        return cls.__subclasses__()

    @classmethod
    def get_subclass_table_names(cls) -> Sequence[str]:
        return tuple(s.__tablename__ for s in cls.get_subclasses())

    @classmethod
    def is_abstract_base(cls) -> bool:
        return cls.__is_abstract_base__


def reverse_lookup(dictionary, search_val):
    for key, val in dictionary.items():
        if val == search_val:
            yield key


@orm.declarative_mixin
class AbstractNode(CommonBase):
    __tablename_scheme__ = "node_{class_name}"

    _edges_out: ClassVar[Sequence[str]] = ()
    _edges_in: ClassVar[Sequence[str]] = ()

    @hybrid.hybrid_property
    def edges_in(self) -> Sequence:
        return tuple(e for rel in self._edges_in for e in getattr(self, rel))

    @hybrid.hybrid_property
    def edges_out(self) -> Sequence:
        return tuple(e for rel in self._edges_out for e in getattr(self, rel))

    def get_edges(self) -> Sequence:
        return (*self.edges_in, *self.edges_out)

    @classmethod
    def get_edge_class(cls):
        return Edge

    node_id = cast(
        str,
        sqlalchemy.Column(
            sqlalchemy.Text,
            primary_key=True,
            nullable=False,
        ),
    )

    @orm.declared_attr
    def __table_args__(cls):
        return (
            sqlalchemy.UniqueConstraint("node_id", name=f"_{cls.__name__.lower()}_id_uc"),
            sqlalchemy.Index(
                f"{cls.__tablename__}__props_idx",
                "_props",
                postgresql_using="gin",
            ),
            sqlalchemy.Index(
                f"{cls.__tablename__}__sysan__props_idx",
                "_sysan",
                "_props",
                postgresql_using="gin",
            ),
            sqlalchemy.Index(
                f"{cls.__tablename__}__sysan_idx",
                "_sysan",
                postgresql_using="gin",
            ),
            sqlalchemy.Index(f"{cls.__tablename__}_node_id_idx", "node_id"),
        )

    def traverse(self, mode="bfs", max_depth=None, edge_pointer="in", edge_predicate=None):
        """
        Performs a traversal starting at the current node
        Args:
            mode (str): type of traversal, defaults to breadth first search
            max_depth (int): maximum distance to traverse
            edge_pointer (str): Determines what edge direction to use, possible values are `in`, `out`
                            `in`: use node.edges_in, default behavior
            edge_predicate (func): a predicate performed on an `edge` object in
            order to decided whether to walk that edge or not

        Returns:
            generator: nodes found in the sub tree
        """
        return traversals.traverse(self, mode, max_depth, edge_pointer, edge_predicate)

    def bfs_children(self, edge_predicate=None, max_depth=None):
        return self.traverse(edge_predicate=edge_predicate, max_depth=max_depth)

    def dfs_children(self, edge_predicate=None, max_depth=None):
        return self.traverse(mode="dfs", edge_predicate=edge_predicate, max_depth=max_depth)

    def __init__(
        self,
        node_id: Optional[str] = None,
        properties: Mapping[str, Any] = types.MappingProxyType({}),
        system_annotations: Mapping[str, Any] = types.MappingProxyType({}),
        acl: Any = None,
        label: Any = None,
        **kwargs: Any,
    ):
        super().__init__()

        self.system_annotations = system_annotations or {}
        self.properties = self._defaults
        self.properties.update(properties)
        self.properties.update(kwargs)
        self.node_id = node_id

    @property
    def _defaults(self) -> dict[str, Any]:
        return {}

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}({self.node_id})>"

    def __eq__(self, other: object) -> bool:
        return bool(isinstance(other, self.__class__) and self.node_id == other.node_id)

    def __hash__(self) -> int:
        return hash((self.node_id, self.__class__))

    def copy(self) -> Self:
        node = self.__class__(
            node_id=self.node_id,
            acl=self.acl,
            _system_annotations=self.system_annotations,
            label=self.label,
        )
        return node

    def to_json(self):
        return {
            "node_id": self.node_id,
            "label": self.label,
            "acl": self.acl,
            "properties": self.properties,
            "system_annotations": self.system_annotations,
        }

    @classmethod
    def from_json(cls, node_json: dict) -> "AbstractNode":
        """loads a node instance from a json object"""

        if cls.is_abstract_base():
            abstract_node_cls = cls.get_edge_class().get_node_class()
            Type = abstract_node_cls.get_subclass(node_json["label"])

            if not Type:
                raise KeyError("Node has no subclass named {}".format(node_json["label"]))
        else:
            Type = cls

        return Type(
            node_id=node_json["node_id"],
            properties=node_json["properties"],
            acl=node_json["acl"],
            system_annotations=node_json["system_annotations"],
            label=node_json["label"],
        )

    @classmethod
    def get_subclass(cls, label: str) -> Optional[type["AbstractNode"]]:
        for c in cls.get_subclasses():
            if c.get_label() == label:
                return c
        return None

    @classmethod
    def get_subclass_named(cls, name):
        for c in cls.get_subclasses():
            if c.__name__ == name:
                return c
        raise KeyError(f"Node has no subclass named {name}")

    @property
    def _history(self):
        session = self.get_session()
        if not session:
            raise RuntimeError(f"{self} not bound to a session. Try get_history(session).")
        return self.get_history(session)

    def get_history(self, session):
        assert self.label, "Specify label for node history"
        return (
            session.query(voided.VoidedNode)
            .filter(voided.VoidedNode.node_id == self.node_id)
            .filter(voided.VoidedNode.label == self.label)
            .order_by(voided.VoidedNode.voided.desc())
        )

    def _snapshot_existing(self, session, old_props, old_sysan):
        temp = TmpNode(self.node_id, old_props, self.acl, old_sysan, self.label, self.created)
        voided_node = voided.VoidedNode(temp)
        session.add(voided_node)


class Node(AbstractNode, declarative.AbstractConcreteBase, Base, is_abstract_base=True): ...


class TmpNode:
    """
    Temporary object to hold a node information
    """

    def __init__(self, node_id, props, acl, sysan, label, created):
        self.node_id = node_id
        self._props = props
        self.acl = acl
        self.system_annotations = sysan
        self.label = label
        self.created = created


def id_column(tablename):
    if not tablename:
        # only happens for abstract classes
        return schema.Column(sqltypes.Text, nullable=True)

    return schema.Column(
        sqltypes.Text,
        sqlalchemy.ForeignKey(
            f"{tablename}.node_id",
            ondelete="CASCADE",
            deferrable=True,
            initially="DEFERRED",
        ),
        primary_key=True,
        nullable=False,
    )


@orm.declarative_mixin
class AbstractEdge(CommonBase):
    __tablename_scheme__ = "edge_{class_name}"
    __src_class__: ClassVar[str]
    __src_table__: ClassVar[str] = ""
    __dst_class__: ClassVar[str]
    __dst_table__: ClassVar[str] = ""
    __src_dst_assoc__: ClassVar[str]
    __dst_src_assoc__: ClassVar[str]
    __name_in__: ClassVar[str]
    __name_out__: ClassVar[str]

    def __init_subclass__(cls, *, is_abstract_base: bool = False) -> None:
        super().__init_subclass__(is_abstract_base=is_abstract_base)

        if not cls.is_abstract_base():
            assert cls.__src_table__, "Concrete edge must have a src table."
            assert cls.__dst_table__, "Concrete edge must have a dst table."

        cls.__name_in__ = f"_{cls.__name__}_in"
        cls.__name_out__ = f"_{cls.__name__}_out"

    @orm.declared_attr
    def src_id(self):
        return id_column(self.__src_table__)

    @orm.declared_attr
    def dst_id(self):
        return id_column(self.__dst_table__)

    @orm.declared_attr
    def src(self):
        if self.is_abstract_base():
            return None

        join = f"{self.__name__}.src_id == {self.__src_class__}.node_id"

        return orm.relationship(
            self.__src_class__,
            primaryjoin=join,
            backref=orm.backref(
                self.__name_out__, primaryjoin=join, cascade="all, delete, delete-orphan"
            ),
        )

    @orm.declared_attr
    def dst(self):
        if self.is_abstract_base():
            return None

        join = f"{self.__name__}.dst_id == {self.__dst_class__}.node_id"

        return orm.relationship(
            self.__dst_class__,
            primaryjoin=join,
            backref=orm.backref(
                self.__name_in__, primaryjoin=join, cascade="all, delete, delete-orphan"
            ),
        )

    @orm.declared_attr
    def __table_args__(cls):
        return (
            schema.Index(f"{cls.__tablename__}_dst_id_src_id_idx", "src_id", "dst_id"),
            schema.Index(f"{cls.__tablename__}_dst_id", "dst_id"),
            schema.Index(f"{cls.__tablename__}_src_id", "src_id"),
        )

    def __init__(
        self,
        src_id: Optional[str] = None,
        dst_id: Optional[str] = None,
        properties: Mapping[str, Any] = types.MappingProxyType({}),
        acl: Iterable[str] = (),
        system_annotations: Mapping[str, Any] = types.MappingProxyType({}),
        label: Optional[str] = None,
        src: Optional["Node"] = None,
        dst: Optional["Node"] = None,
        **kwargs,
    ):
        super().__init__()

        self.acl.extend(acl)
        self.system_annotations = dict(system_annotations)
        self.properties = dict(properties)
        self.properties.update(kwargs)

        if src is not None:
            if src_id is not None:
                assert src.node_id == src_id, (
                    "Edge initialized with src.node_id and src_id" "that don't match."
                )
            self.src = src
            self.src_id = src.node_id
        else:
            self.src_id = src_id

        if dst is not None:
            if dst_id is not None:
                assert dst.node_id == dst_id, (
                    "Edge initialized with dst.node_id and dst_id" "that don't match."
                )
            self.dst = dst
            self.dst_id = dst.node_id
        else:
            self.dst_id = dst_id

    def to_json(self):
        assert self.src and self.dst, (
            "src or dst is not set on the edge. Sync with the database first "
            "to set the src and dst association proxy."
        )

        return {
            "src_id": self.src_id,
            "dst_id": self.dst_id,
            "src_label": self.src.label,
            "dst_label": self.dst.label,
            "label": self.label,
            "acl": self.acl,
            "properties": self.properties,
            "system_annotations": self.system_annotations,
        }

    @classmethod
    def from_json(cls, edge_json):

        if cls.is_abstract_base():
            abstract_edge_cls = cls.get_node_class().get_edge_class()
            Type = abstract_edge_cls.get_unique_subclass(
                edge_json["src_label"], edge_json["label"], edge_json["dst_label"]
            )

            if not Type:
                raise KeyError("Edge has no subclass named {}".format(edge_json["label"]))
        else:
            Type = cls

        return Type(
            src_id=edge_json["src_id"],
            dst_id=edge_json["dst_id"],
            label=edge_json["label"],
            acl=edge_json["acl"],
            properties=edge_json["properties"],
            system_annotations=edge_json["system_annotations"],
        )

    def __repr__(self):
        return "<{}(({})-[{}]->({})>".format(
            self.__class__.__name__, self.src_id, self.label, self.dst_id
        )

    def __eq__(self, other):
        return (
            isinstance(other, self.__class__)
            and self.src_id == other.src_id
            and self.dst_id == other.dst_id
            and self.label == other.label
        )

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash((self.src_id, self.dst_id, self.label))

    @classmethod
    def get_subclass(cls, label):
        """Tries to resolve an edge subclass by label, this will fail if
        there are multiple edge subclass that use the same label.
        """
        scls = cls._get_subclasses_labeled(label)
        if len(scls) > 1:
            raise KeyError(
                "More than one Edge with label {} found, try get_unique_subclass()"
                "to resolve type using src and dst labels: {}".format(label, scls)
            )
        if not scls:
            return None
        return scls[0]

    @classmethod
    def get_unique_subclass(cls, src_label, label, dst_label):
        """Determines a subclass based on the src and dst."""
        src_class = cls.get_node_class().get_subclass(src_label).__name__
        dst_class = cls.get_node_class().get_subclass(dst_label).__name__
        scls = [
            c
            for c in cls.get_subclasses()
            if c.get_label() == label
            and c.__src_class__ == src_class
            and c.__dst_class__ == dst_class
        ]
        if len(scls) > 1:
            raise KeyError(f"More than one Edge with label {label} found: {scls}")
        if not scls:
            return None
        return scls[0]

    @classmethod
    def _get_subclasses_labeled(cls, label):
        return [c for c in cls.get_subclasses() if c.get_label() == label]

    @classmethod
    def _get_edges_with_src(cls, src_class_name):
        return [c for c in cls.get_subclasses() if c.__src_class__ == src_class_name]

    @classmethod
    def _get_edges_with_dst(cls, dst_class_name):
        return [c for c in cls.get_subclasses() if c.__dst_class__ == dst_class_name]

    def _snapshot_existing(self, session, old_props, old_sysan):
        temp = self.__class__(
            self.src_id, self.dst_id, old_props, self.acl, old_sysan, self.label
        )
        voided_edge = voided.VoidedEdge(temp)
        session.add(voided_edge)

    @classmethod
    def get_node_class(cls) -> type[AbstractNode]:
        return Node


class Edge(AbstractEdge, declarative.AbstractConcreteBase, Base, is_abstract_base=True): ...


def _get_orm_base(cls: type[Union[AbstractEdge, AbstractNode]]) -> type:
    return more_itertools.one(
        filter(
            lambda b: isinstance(b, declarative.DeclarativeMeta) and b is not cls,
            inspect.getmro(cls),
        )
    )


def _get_orm_bases() -> Iterable[tuple[type[AbstractEdge], type[AbstractNode]]]:
    edges = {_get_orm_base(c): c for c in AbstractEdge.get_subclasses()}
    nodes = {_get_orm_base(c): c for c in AbstractNode.get_subclasses()}

    assert (
        edges.keys() == nodes.keys()
    ), "For each ORM base there must be both a node and edge base."

    for k in edges.keys():
        yield edges[k], nodes[k]


TGraphEntity = TypeVar("TGraphEntity", AbstractEdge, AbstractNode)


def _get_subclasses(*cls: type[TGraphEntity]) -> Iterator[type[TGraphEntity]]:
    if cls:
        if not cls[0].is_abstract_base():
            yield cls[0]
        yield from _get_subclasses(*cls[1:], *cls[0].get_subclasses())


def _create_proxy(
    src: type[AbstractNode], edge: type[AbstractEdge], dst: type[AbstractNode]
) -> None:
    setattr(
        dst,
        edge.__dst_src_assoc__,
        associationproxy.association_proxy(
            edge.__name_in__, "src", creator=lambda node: edge(src=node)
        ),
    )
    setattr(
        src,
        edge.__src_dst_assoc__,
        associationproxy.association_proxy(
            edge.__name_out__, "dst", creator=lambda node: edge(dst=node)
        ),
    )


def _relate_nodes(abstract_edge: type[AbstractEdge], abstract_node: type[AbstractNode]) -> None:
    edges = tuple(_get_subclasses(abstract_edge))
    nodes = {c.__name__: c for c in _get_subclasses(abstract_node)}

    for edge in edges:
        src_node = nodes[edge.__src_class__]
        dst_node = nodes[edge.__dst_class__]

        _create_proxy(src_node, edge, dst_node)

    for node in nodes.values():
        node._edges_out = tuple(e.__name_out__ for e in edges if e.__src_class__ == node.__name__)
        node._edges_in = tuple(e.__name_in__ for e in edges if e.__dst_class__ == node.__name__)


@event.listens_for(orm.mapper, "after_configured")
def relate_nodes() -> None:
    for edge, node in _get_orm_bases():
        _relate_nodes(edge, node)

"""A module for the base functionality of graph entity logic including Edges & Nodes."""

from __future__ import annotations

import types
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from typing import Any, ClassVar, Literal, TypedDict

import more_itertools
import sqlalchemy
from sqlalchemy import orm
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext import associationproxy, declarative, hybrid
from typing_extensions import NotRequired, Self

from psqlgraph import attributes, traversals, util, voided


@orm.declarative_mixin
class AbstractEntity:
    __edge_class__: ClassVar[type[AbstractEdge]]
    __node_class__: ClassVar[type[AbstractNode]]
    __is_abstract__: ClassVar[bool] = True
    __tablename__: ClassVar[str]
    __label__: ClassVar[str] = "entity"
    __nonnull_properties__: ClassVar[Sequence[str]] = ()

    _session_hooks_before_insert: ClassVar[list[Callable]] = []
    _session_hooks_before_update: ClassVar[list[Callable]] = []
    _session_hooks_before_delete: ClassVar[list[Callable]] = []

    # ======== Columns ========

    created = sqlalchemy.Column(
        sqlalchemy.DateTime(timezone=True),
        nullable=False,
        server_default=sqlalchemy.text("now()"),
    )

    acl = sqlalchemy.Column(
        sqlalchemy.ARRAY(sqlalchemy.Text),
        default=list(),
    )

    _sysan = sqlalchemy.Column(
        # WARNING: Do not update this column directly. See
        # `.system_annotations`
        postgresql.JSONB,
        server_default="{}",
    )

    _props = sqlalchemy.Column(
        # WARNING: Do not update this column directly.
        # See `.properties` or `.props`
        postgresql.JSONB,
        server_default="{}",
    )

    def __init__(
        self,
        acl: Iterable[str],
        properties: Mapping[str, Any],
        system_annotations: Mapping[str, Any],
    ) -> None:
        """The base for all graph entities.

        Args:
            acl: The acl associated with the entity.
            properties: The property values for the entity.
            system_annotations: The system annotations for the entity.
        """
        self.acl = []
        self._props = {}
        self._sysan = {}

        self.acl.extend(acl)
        self.properties.update(properties)
        self.system_annotations.update(system_annotations)

    # ================================= Class Methods ==================================

    def __init_subclass__(cls, is_abstract: bool = False) -> None:
        cls.__is_abstract__ = is_abstract
        cls.__pg_properties__ = types.MappingProxyType(
            {
                name: prop.types
                for name, prop in vars(cls).items()
                if isinstance(prop, attributes.PGProperty)
            }
        )

    @declarative.declared_attr
    def __mapper_args__(self) -> Mapping[str, Any]:
        if self.is_abstract_base():
            return {}

        return {
            "polymorphic_identity": self.__tablename__,
            "concrete": True,
        }

    @classmethod
    def __declare_first__(cls) -> None:
        if issubclass(cls, declarative.AbstractConcreteBase) and cls.get_subclasses():
            cls._sa_decl_prepare_nocascade()

    @classmethod
    def get_edge_class(cls) -> type[AbstractEdge]:
        return cls.__edge_class__

    @classmethod
    def get_node_class(cls) -> type[AbstractNode]:
        return cls.__node_class__

    @classmethod
    def get_name(cls) -> str:
        """Convenience wrapper for getting class name"""
        return cls.__name__

    @classmethod
    def get_label(cls) -> str: ...

    @declarative.declared_attr
    def label(self) -> str:
        return self.get_label()

    @classmethod
    def is_subclass_loaded(cls, name: str) -> bool:
        return any(name == c.__name__ for c in cls.get_subclasses())

    @classmethod
    def add_subclass(cls, subclass: type[Self]) -> None:
        if not issubclass(subclass, cls):
            raise AttributeError(f"{subclass} is not a subclass of {cls}")

    @classmethod
    def get_subclasses(cls) -> Sequence[type[Self]]:
        """
        Limits the scope of subclasses to only those manually specified, else
        defaults to actual subclasses
        """
        return cls.__subclasses__()

    @classmethod
    def get_subclass_table_names(cls) -> Sequence[str]:
        return tuple(s.__tablename__ for s in cls.get_subclasses())

    @classmethod
    def is_abstract_base(cls):
        return cls.__is_abstract__

    # =================================== Properties ===================================

    @hybrid.hybrid_property
    def properties(self) -> dict[str, Any]:
        return attributes.PropertiesDict(self)

    @properties.setter
    def properties(self, properties: dict[str, Any]) -> None:
        """To set each property, _set_property is called, which calls
        __setitem__ which calls setattr(). The final call to setattr
        will pass through any validation defined in a subclass
        property setter.

        """
        for key, val in util.sanitize(properties).items():
            setattr(self, key, val)

    @hybrid.hybrid_property
    def props(self) -> dict[str, Any]:
        """Alias of properties"""
        return self.properties

    @props.setter
    def props(self, properties: dict[str, Any]) -> None:
        """Alias of properties"""
        self.properties = properties

    def _set_property(self, key: str, val: Any) -> None:
        """Property dict is cloned (to make sure that SQLAlchemy flushes it)
        before setting the key value pair.

        """
        if not self.has_property(key):
            raise KeyError(f"{type(self)} has no property {key}")

        self._props = {**self._props, key: val}

    def _get_property(self, key: str) -> Any:
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
        defaults = {k: None for k in self.get_property_list()}

        return {**defaults, **properties}

    def __getitem__(self, key: str) -> Any:
        """Returns value corresponding to key in _props"""
        return getattr(self, key)

    def __setitem__(self, key: str, val: Any) -> None:
        """Sets value corresponding to key in _props.  This calls the model's
        hybrid_property setter method in the instance's model class.

        """
        setattr(self, key, val)

    @classmethod
    def get_property_list(cls) -> Iterable[str]:
        """Returns a list of hybrid_properties defined on the subclass model"""
        return cls.__pg_properties__.keys()

    @classmethod
    def has_property(cls, key: str) -> bool:
        """Returns boolean if key is a property defined on the subclass model"""
        return key in cls.__pg_properties__

    @classmethod
    def get_pg_properties(cls) -> Mapping[str, tuple[type, ...]]:
        return cls.__pg_properties__

    # =============================== System Annotations ===============================

    @hybrid.hybrid_property
    def sysan(self) -> dict[str, Any]:
        """Alias of properties"""
        return self.system_annotations

    @sysan.setter
    def sysan(self, sysan: dict[str, Any]) -> None:
        """Alias of properties"""
        self.system_annotations = sysan

    @hybrid.hybrid_property
    def system_annotations(self) -> dict[str, Any]:
        """Returns a system annotation proxy pointing to _sysan.  Any updates
        to this dict will be proxied to the model's _sysan JSONB
        column.

        """
        return attributes.SystemAnnotationDict(self)

    @system_annotations.setter
    def system_annotations(self, sysan: dict[str, Any]) -> None:
        """Directly set the model's _sysan column with dict sysan."""
        self._sysan = util.sanitize(sysan)

    # ======== Misc ========

    def get_session(self) -> orm.Session | None:
        """Returns the session an object is bound to if bound to a session"""
        return orm.object_session(self)

    def merge(
        self,
        acl: str | None = None,
        system_annotations: Mapping[str, Any] = types.MappingProxyType({}),
        properties: Mapping[str, Any] = types.MappingProxyType({}),
    ) -> None:
        """Merge the model's system_annotations and properties.

        .. note: acl will be overwritten, merging acls is not supported
        """
        self.system_annotations.update(system_annotations)
        for key, value in properties.items():
            setattr(self, key, value)
        if acl is not None:
            self.acl = acl

    def _merge_onto_existing(
        self, old_props: Mapping[str, Any], old_sysan: Mapping[str, Any]
    ) -> None:
        self._props = {**old_props, **self._props}
        self._sysan = {**old_sysan, **self._sysan}

    def _get_clean_session(self, session: orm.Session | None = None) -> orm.Session:
        """Create a new session from an objects session using the same
        connection to allow for clean queries against the database

        """
        if not session:
            session = self.get_session()

        assert session, "No valid session found to base new session on."

        clean = orm.sessionmaker(session.bind)
        return clean()

    def _validate(self, _: orm.Session | None = None) -> None:
        """Final validation currently only includes checking nonnull
        properties

        """
        for key in self.__nonnull_properties__:
            assert self.properties[key] is not None, (
                f"Null value in key '{key}' violates non-null constraint for {self}."
            )

    def __snapshot_existing__(
        self, session: orm.Session, old_props: dict[str, Any], old_sysan: dict[str, Any]
    ) -> None:
        raise NotImplementedError()


class AbstractEdge(AbstractEntity, is_abstract=True):
    __label__ = "edge"

    __dst_class__: ClassVar[str]
    __dst_src_assoc__: ClassVar[str]
    __dst_table__: ClassVar[str]
    __src_class__: ClassVar[str]
    __src_dst_assoc__: ClassVar[str]
    __src_table__: ClassVar[str]
    __name_in__: ClassVar[str]
    __name_out__: ClassVar[str]

    # ================================= Class Methods ==================================

    def __init_subclass__(cls, is_abstract: bool = False) -> None:
        super().__init_subclass__(is_abstract)

        cls.__name_in__ = f"_{cls.__name__}_in"
        cls.__name_out__ = f"_{cls.__name__}_out"

    @declarative.declared_attr
    def __table_args__(self) -> tuple[sqlalchemy.Constraint, ...]:
        if self.is_abstract_base():
            return ()

        return (
            sqlalchemy.Index(f"{self.__tablename__}_dst_id_src_id_idx", "src_id", "dst_id"),
            sqlalchemy.Index(f"{self.__tablename__}_dst_id", "dst_id"),
            sqlalchemy.Index(f"{self.__tablename__}_src_id", "src_id"),
        )

    @classmethod
    def _id_column(cls, node: Literal["src", "dst"]) -> sqlalchemy.Column:
        if cls.is_abstract_base():
            return sqlalchemy.Column(sqlalchemy.Text, nullable=True)

        node_table = cls.__src_table__ if node == "src" else cls.__dst_table__

        return sqlalchemy.Column(
            sqlalchemy.Text,
            sqlalchemy.ForeignKey(
                f"{node_table}.node_id",
                ondelete="CASCADE",
                deferrable=True,
                initially="DEFERRED",
            ),
            primary_key=True,
            nullable=False,
        )

    @classmethod
    def _get_subclasses_labeled(cls, label) -> Iterator[type[Self]]:
        return (c for c in cls.get_subclasses() if c.get_label() == label)

    @classmethod
    def _get_edges_with_src(cls, src_class_name) -> Iterator[type[Self]]:
        return (c for c in cls.get_subclasses() if c.__src_class__ == src_class_name)

    @classmethod
    def _get_edges_with_dst(cls, dst_class_name) -> Iterator[type[Self]]:
        return (c for c in cls.get_subclasses() if c.__dst_class__ == dst_class_name)

    @classmethod
    def get_subclass(cls, label: str) -> type[Self] | None:
        """Tries to resolve an edge subclass by label, this will fail if
        there are multiple edge subclass that use the same label.
        """
        return more_itertools.only(
            cls._get_subclasses_labeled(label),
            default=None,
            too_long=KeyError(
                f"More than one Edge with label {label} found, try get_unique_subclass()"
                "to resolve type using src and dst labels."
            ),
        )

    @classmethod
    def get_unique_subclass(
        cls, src_label: str, label: str, dst_label: str
    ) -> type[Self] | None:
        """Determines a subclass based on the src and dst."""
        base_node = cls.get_node_class()
        src_class = base_node.get_subclass(src_label)
        dst_class = base_node.get_subclass(dst_label)

        assert src_class, f"No valid src class found with label: {src_label}."
        assert dst_class, f"No valid src class found with label: {dst_label}."

        scls = (
            c
            for c in cls.get_subclasses()
            if c.get_label() == label
            and c.__src_class__ == src_class.__name__
            and c.__dst_class__ == dst_class.__name__
        )

        return more_itertools.only(
            scls,
            default=None,
            too_long=KeyError(f"More than one Edge with label {label} found."),
        )

    @classmethod
    def get_label(cls) -> str:
        return cls.__label__

    # ==================================== Columns =====================================

    @declarative.declared_attr
    def src_id(self) -> str | None:
        return self._id_column("src")

    @declarative.declared_attr
    def src(self) -> AbstractNode:
        if self.is_abstract_base():
            return None  # type: ignore

        return orm.relationship(
            self.__src_class__,
            back_populates=self.__name_out__,
            foreign_keys=[self.src_id],
        )

    @declarative.declared_attr
    def dst(self) -> AbstractNode:
        if self.is_abstract_base():
            return None  # type: ignore

        return orm.relationship(
            self.__dst_class__, back_populates=self.__name_in__, foreign_keys=[self.dst_id]
        )

    @declarative.declared_attr
    def dst_id(self) -> str | None:
        return self._id_column("dst")

    def __init__(
        self,
        src_id: str | None = None,
        dst_id: str | None = None,
        properties: Mapping[str, Any] | None = None,
        acl: list[str] | None = None,
        system_annotations: Mapping[str, Any] | None = None,
        src: AbstractNode | None = None,
        dst: AbstractNode | None = None,
        **kwargs,
    ):
        properties = properties or {}
        properties = {**properties, **kwargs}

        super().__init__(acl or (), properties, system_annotations or {})

        if src is not None:
            if src_id is not None:
                assert src.node_id == src_id, (
                    "Edge initialized with src.node_id and src_idthat don't match."
                )
            self.src = src
            self.src_id = src.node_id
        else:
            self.src_id = src_id

        if dst is not None:
            if dst_id is not None:
                assert dst.node_id == dst_id, (
                    "Edge initialized with dst.node_id and dst_idthat don't match."
                )
            self.dst = dst
            self.dst_id = dst.node_id
        else:
            self.dst_id = dst_id

    # =================================== Build-ins ====================================

    def __repr__(self) -> str:
        return f"<{self.get_name()}(({self.src_id})-[{self.get_label()}]->({self.dst_id})>"

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, self.__class__)
            and self.src_id == other.src_id
            and self.dst_id == other.dst_id
            and self.label == other.label
        )

    def __ne__(self, other: Any) -> bool:
        return not self.__eq__(other)

    def __hash__(self) -> int:
        return hash((self.src_id, self.dst_id, self.label))

    # ===================================== Utils ======================================

    class ToJSON(TypedDict):
        src_id: str | None
        dst_id: str | None
        acl: list[str]
        properties: dict[str, Any]
        system_annotations: dict[str, Any]
        label: str
        src_label: str
        dst_label: str

    def to_json(self) -> ToJSON:
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

    class FromJSON(TypedDict):
        src_id: str | None
        dst_id: str | None
        acl: list[str] | None
        properties: Mapping[str, Any] | None
        system_annotations: Mapping[str, Any] | None

        label: NotRequired[str]
        src_label: NotRequired[str]
        dst_label: NotRequired[str]

    @classmethod
    def from_json(cls, json: FromJSON) -> AbstractEdge:
        if cls.is_abstract_base():
            assert "label" in json, "Must provide a label to resolve edge cls."
            assert "src_label" in json, "Must provide a src label to resolve edge cls."
            assert "dst_label" in json, "Must provide a dst label to resolve edge cls."

            abstract_edge_cls = cls.get_edge_class()
            edge_cls = abstract_edge_cls.get_unique_subclass(
                json["src_label"], json["label"], json["dst_label"]
            )

            if not edge_cls:
                raise KeyError(f"Edge has no subclass named {json['label']}")
        else:
            edge_cls = cls

        return edge_cls(
            src_id=json["src_id"],
            dst_id=json["dst_id"],
            acl=json["acl"],
            properties=json["properties"],
            system_annotations=json["system_annotations"],
        )

    # ==================================== History =====================================

    def __snapshot_existing__(
        self,
        session: orm.Session,
        old_props: Mapping[str, Any],
        old_sysan: Mapping[str, Any],
    ):
        voided_edge = voided.VoidedEdge(
            src_id=self.src_id,
            dst_id=self.dst_id,
            label=self.get_label(),
            created=None,
            acl=self.acl,
            system_annotations=old_sysan,
            properties=self.property_template(old_props),
        )

        session.add(voided_edge)


class AbstractNode(AbstractEntity, is_abstract=True):
    __label__ = "node"

    _defaults: ClassVar[Mapping[str, Any]] = types.MappingProxyType({})
    _edges_out: ClassVar[Sequence[str]] = ()
    _edges_in: ClassVar[Sequence[str]] = ()

    # ==================================== Columns =====================================

    node_id = sqlalchemy.Column(
        sqlalchemy.Text,
        primary_key=True,
        nullable=False,
    )

    def __init__(
        self,
        node_id: str | None = None,
        properties: Mapping[str, Any] | None = None,
        acl: list[str] | None = None,
        system_annotations: Mapping[str, Any] | None = None,
        **kwargs,
    ) -> None:
        properties = properties or {}
        properties = {**self._defaults, **properties, **kwargs}

        super().__init__(acl or (), properties, system_annotations or {})

        self.node_id = node_id

    # ================================= Class Methods ==================================

    def __init_subclass__(cls, is_abstract: bool = False) -> None:
        super().__init_subclass__(is_abstract)

        if not hasattr(cls, "__label__"):
            # This has to be here as a hold over b/c nodes currently override the
            # `get_label` method vs edges which set `__label__`. TODO: DEV-TODO
            cls.__label__ = cls.get_label()

    @declarative.declared_attr
    def __table_args__(self) -> tuple[sqlalchemy.Constraint, ...]:
        if self.is_abstract_base():
            return ()

        return (
            sqlalchemy.UniqueConstraint("node_id", name=f"_{self.get_name().lower()}_id_uc"),
            sqlalchemy.Index(
                f"{self.__tablename__}__props_idx",
                "_props",
                postgresql_using="gin",
            ),
            sqlalchemy.Index(
                f"{self.__tablename__}__sysan__props_idx",
                "_sysan",
                "_props",
                postgresql_using="gin",
            ),
            sqlalchemy.Index(
                f"{self.__tablename__}__sysan_idx",
                "_sysan",
                postgresql_using="gin",
            ),
            sqlalchemy.Index(f"{self.__tablename__}_node_id_idx", "node_id"),
        )

    @classmethod
    def get_subclass(cls, label: str) -> type[Self] | None:
        return more_itertools.first_true(
            cls.get_subclasses(), default=None, pred=lambda c: c.get_label() == label
        )

    @classmethod
    def get_subclass_named(cls, name: str) -> type[Self]:
        sub = more_itertools.first_true(
            cls.get_subclasses(), default=None, pred=lambda c: c.get_name() == name
        )

        if sub is None:
            raise KeyError(f"Node has no subclass named {name}")

        return sub

    @classmethod
    def get_label(cls) -> str:
        return cls.__label__

    @classmethod
    def __add_edge_out__(cls, edge: type[AbstractEdge]) -> None:
        """Adds the edge as an outbound relationship to another node.

        This method will add both the edge as a property to the class which is a SQL
        relationship to the edge table and the proxy relationship to the the node which
        is the destination of the associated edge. To do this, the node will have a new
        attribute added with the edge's configured `__name_out__` value and an attribute
        with the the edge's configured `__src_dst_assoc__` which will link this node to
        the node related to the edge's `dst`.

        NOTE: This method is *ONLY* for internal usage to psqlgraph.

        Args:
            edge: The edge which forms and outbound relationship to another node.
        """
        if hasattr(cls, edge.__name_out__):
            return

        setattr(
            cls,
            edge.__name_out__,
            orm.relationship(
                edge,
                back_populates="src",
                cascade="all,delete,delete-orphan",
                foreign_keys=[edge.src_id],
            ),
        )
        setattr(
            cls,
            edge.__src_dst_assoc__,
            associationproxy.association_proxy(
                edge.__name_out__, "dst", creator=lambda node: edge(dst=node)
            ),
        )

    @classmethod
    def __add_edge_in__(cls, edge: type[AbstractEdge]) -> None:
        """Adds the edge as an inbound relationship from another node.

        This method will add both the edge as a property to the class which is a SQL
        relationship to the edge table and the proxy relationship to the the node which
        is the source of the associated edge. To do this, the node will have a new
        attribute added with the edge's configured `__name_in__` value and an attribute
        with the the edge's configured `__dst_src_assoc__` which will link this node to
        the node related to the edge's `src`.

        NOTE: This method is *ONLY* for internal usage to psqlgraph.

        Args:
            edge: The edge which forms and inbound relationship from another node.
        """
        if hasattr(cls, edge.__name_in__):
            return

        setattr(
            cls,
            edge.__name_in__,
            orm.relationship(
                edge,
                back_populates="dst",
                cascade="all,delete,delete-orphan",
                foreign_keys=[edge.dst_id],
            ),
        )
        setattr(
            cls,
            edge.__dst_src_assoc__,
            associationproxy.association_proxy(
                edge.__name_in__, "src", creator=lambda node: edge(src=node)
            ),
        )

    # =================================== Build-ins ====================================

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}({self.node_id})>"

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, self.__class__) and self.node_id == other.node_id

    def __ne__(self, other: Any) -> bool:
        return not self.__eq__(other)

    def __hash__(self) -> int:
        return hash((self.node_id, self.__class__))

    # ===================================== Edges ======================================

    @hybrid.hybrid_property
    def edges_in(self) -> Sequence[AbstractEdge]:
        return tuple(e for rel in self._edges_in for e in getattr(self, rel))

    @hybrid.hybrid_property
    def edges_out(self) -> Sequence[AbstractEdge]:
        return tuple(e for rel in self._edges_out for e in getattr(self, rel))

    def get_edges(self) -> Iterator[AbstractEdge]:
        yield from self.edges_in
        yield from self.edges_out

    # =================================== Traversals ===================================

    def traverse(
        self,
        mode: Literal["bfs", "dfs"] = "bfs",
        max_depth: int | None = None,
        edge_pointer: Literal["in", "out"] = "in",
        edge_predicate: Callable[[AbstractEdge], bool] | None = None,
    ) -> Iterator[AbstractNode]:
        """
        Performs a traversal starting at the current node
        Args:
            mode (str): type of traversal, defaults to breadth first search
            max_depth (int): maximum distance to traverse
            edge_pointer (str): Determines what edge direction to use, possible
                                values are `in`, `out`
                            `in`: use node.edges_in, default behavior
            edge_predicate (func): a predicate performed on an `edge` object in
            order to decided whether to walk that edge or not

        Returns:
            generator: nodes found in the sub tree
        """
        return traversals.traverse(self, mode, max_depth, edge_pointer, edge_predicate)

    def bfs_children(
        self,
        edge_predicate: Callable[[AbstractEdge], bool] | None = None,
        max_depth: int | None = None,
    ) -> Iterator[AbstractNode]:
        return self.traverse(edge_predicate=edge_predicate, max_depth=max_depth)

    def dfs_children(
        self,
        edge_predicate: Callable[[AbstractEdge], bool] | None = None,
        max_depth: int | None = None,
    ) -> Iterator[AbstractNode]:
        return self.traverse(mode="dfs", edge_predicate=edge_predicate, max_depth=max_depth)

    # ==================================== History =====================================

    @property
    def _history(self) -> orm.Query:
        session = self.get_session()

        if not session:
            raise RuntimeError(f"{self} not bound to a session. Try get_history(session).")

        return self.get_history(session)

    def get_history(self, session: orm.Session) -> orm.Query:
        return (
            session.query(voided.VoidedNode)
            .filter(voided.VoidedNode.node_id == self.node_id)
            .filter(voided.VoidedNode.label == self.get_label())
            .order_by(voided.VoidedNode.voided.desc())
        )

    def __snapshot_existing__(
        self, session: orm.Session, old_props: dict[str, Any], old_sysan: dict[str, Any]
    ) -> None:
        voided_node = voided.VoidedNode(
            node_id=self.node_id,
            label=self.get_label(),
            created=self.created,
            acl=self.acl,
            system_annotations=old_sysan,
            properties=old_props,
        )

        session.add(voided_node)

    # ===================================== Utils ======================================

    def copy(self) -> Self:
        node = self.__class__(
            node_id=self.node_id,
            acl=self.acl,
            system_annotations=self.system_annotations,
        )

        return node

    class ToJSON(TypedDict):
        node_id: str | None
        acl: list[str]
        properties: dict[str, Any]
        system_annotations: dict[str, Any]
        label: str

    def to_json(self) -> ToJSON:
        return {
            "node_id": self.node_id,
            "label": self.label,
            "acl": self.acl,
            "properties": self.properties,
            "system_annotations": self.system_annotations,
        }

    class FromJSON(TypedDict):
        node_id: str | None
        acl: list[str] | None
        properties: Mapping[str, Any] | None
        system_annotations: Mapping[str, Any] | None

        label: NotRequired[str]

    @classmethod
    def from_json(cls, json: FromJSON) -> AbstractNode:
        """loads a node instance from a json object"""

        if cls.is_abstract_base():
            assert "label" in json, "Must provide label in JSON to load node from JSON."

            node_cls = cls.get_node_class().get_subclass(json["label"])

            if not node_cls:
                raise KeyError(f"Node has no subclass named {json['label']}")
        else:
            node_cls = cls

        return node_cls(
            node_id=json["node_id"],
            properties=json["properties"],
            acl=json["acl"],
            system_annotations=json["system_annotations"],
        )


AbstractEdge.__edge_class__ = AbstractEdge
AbstractNode.__edge_class__ = AbstractEdge
AbstractEdge.__node_class__ = AbstractNode
AbstractNode.__node_class__ = AbstractNode

from sqlalchemy import Column, Index, Text, UniqueConstraint
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship

from psqlgraph import base, traversals
from psqlgraph.edge import Edge
from psqlgraph.voided_node import VoidedNode

DST_SRC_ASSOC = "__dst_src_assoc__"
SRC_DST_ASSOC = "__src_dst_assoc__"


def reverse_lookup(dictionary, search_val):
    for key, val in dictionary.items():
        if val == search_val:
            yield key


class NodeAssociationProxyMixin:
    @declared_attr
    def _edges_out(self):
        return list()

    @declared_attr
    def _edges_in(self):
        return list()

    @classmethod
    def __declare_last__(cls):
        """
        Execute once after all mappings has been configured by sqlalchemy
        Maps edges to nodes based on definitions.

        NOTE: Had to be moved outside of Node class, because of:
        https://github.com/zzzeek/sqlalchemy/blob/master/lib/sqlalchemy/ext/declarative/base.py#L87-L97
        The actual change:
        https://github.com/zzzeek/sqlalchemy/commit/4c931b2ec7e0f09ac8c3ebe28c794f5858d54efb
        """

        edge_cls = cls.get_edge_class()
        for scls in edge_cls.get_subclasses():

            if scls.is_abstract_base():
                continue

            name = scls.__name__
            name_in = f"_{name}_in"
            name_out = f"_{name}_out"
            src_assoc = getattr(scls, SRC_DST_ASSOC)
            dst_assoc = getattr(scls, DST_SRC_ASSOC)

            if scls.__dst_class__ == cls.__name__:
                if not hasattr(cls, name_in):
                    edge_in = relationship(
                        name,
                        foreign_keys=[scls.dst_id],
                        backref="dst",
                        cascade="all, delete, delete-orphan",
                    )
                    setattr(cls, name_in, edge_in)
                    cls._edges_in.append(name_in)
                cls._set_association_proxy(scls, dst_assoc, name_in, "src")
            if scls.__src_class__ == cls.__name__:
                if not hasattr(cls, name_out):
                    edge_out = relationship(
                        name,
                        foreign_keys=[scls.src_id],
                        backref="src",
                        cascade="all, delete, delete-orphan",
                    )
                    setattr(cls, name_out, edge_out)
                    cls._edges_out.append(name_out)
                cls._set_association_proxy(scls, src_assoc, name_out, "dst")

    @hybrid_property
    def edges_in(self):
        return [e for rel in self._edges_in for e in getattr(self, rel)]

    @hybrid_property
    def edges_out(self):
        return [e for rel in self._edges_out for e in getattr(self, rel)]

    @classmethod
    def _set_association_proxy(cls, edge_cls, attr_name, edge_name, direction):
        rel = association_proxy(
            edge_name, direction, creator=lambda node: edge_cls(**{direction: node})
        )
        setattr(cls, attr_name, rel)

    def get_edges(self):
        yield from self.edges_in
        yield from self.edges_out

    @classmethod
    def get_edge_class(cls):
        return Edge


class AbstractNode(NodeAssociationProxyMixin, base.ExtMixin):

    node_id = Column(
        Text,
        primary_key=True,
        nullable=False,
    )

    @declared_attr
    def __tablename__(cls):
        return base.NODE_TABLENAME_SCHEME.format(class_name=cls.__name__.lower())

    @declared_attr
    def __table_args__(cls):
        return (
            UniqueConstraint("node_id", name=f"_{cls.__name__.lower()}_id_uc"),
            Index(
                f"{cls.__tablename__}__props_idx",
                "_props",
                postgresql_using="gin",
            ),
            Index(
                f"{cls.__tablename__}__sysan__props_idx",
                "_sysan",
                "_props",
                postgresql_using="gin",
            ),
            Index(
                f"{cls.__tablename__}__sysan_idx",
                "_sysan",
                postgresql_using="gin",
            ),
            Index(f"{cls.__tablename__}_node_id_idx", "node_id"),
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
        node_id=None,
        properties=None,
        acl=None,
        system_annotations=None,
        label=None,
        **kwargs,
    ):
        self._props = {}
        self.system_annotations = system_annotations or {}
        self.acl = acl or []
        self.properties = self._defaults
        self.properties.update(properties)
        self.properties.update(kwargs)
        self.node_id = node_id

    @property
    def _defaults(self):
        return {}

    def __repr__(self):
        return f"<{self.__class__.__name__}({self.node_id})>"

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.node_id == other.node_id

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash((self.node_id, self.__class__))

    def copy(self):
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
    def from_json(cls, node_json):
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
    def get_subclass(cls, label):
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
            session.query(VoidedNode)
            .filter(VoidedNode.node_id == self.node_id)
            .filter(VoidedNode.label == self.label)
            .order_by(VoidedNode.voided.desc())
        )

    def _snapshot_existing(self, session, old_props, old_sysan):
        temp = TmpNode(self.node_id, old_props, self.acl, old_sysan, self.label, self.created)
        voided = VoidedNode(temp)
        session.add(voided)


class Node(base.LocalConcreteBase, AbstractNode, base.ORMBase):
    pass


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


def PolyNode(node_id=None, label=None, acl=None, system_annotations=None, properties=None):
    assert label, "You cannot create a PolyNode without a label."
    Type = Node.get_subclass(label)
    return Type(
        node_id=node_id,
        properties=properties or {},
        acl=acl or [],
        system_annotations=system_annotations or {},
        label=label,
    )

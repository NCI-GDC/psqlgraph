import sqlalchemy
from sqlalchemy.ext.declarative import AbstractConcreteBase, declared_attr

from psqlgraph.base import ORMBase, EDGE_TABLENAME_SCHEME, NODE_TABLENAME_SCHEME
from psqlgraph.voided_edge import VoidedEdge


def id_column(tablename, class_name):
    if class_name == "Edge":
        return sqlalchemy.Column(sqlalchemy.Text, nullable=True)

    return sqlalchemy.Column(
        sqlalchemy.Text,
        sqlalchemy.ForeignKey(
            '{}.node_id'.format(tablename),
            ondelete="CASCADE",
            deferrable=True,
            initially="DEFERRED"),
        primary_key=True,
        nullable=False,
    )


class DeclareLastEdgeMixin(object):
    @classmethod
    def __declare_last__(cls):
        if cls.__name__ == 'Edge':
            return

        assert hasattr(cls, '__src_class__'), \
            'You must declare __src_class__ for {}'.format(cls)
        assert hasattr(cls, '__dst_class__'), \
            'You must declare __dst_class__ for {}'.format(cls)
        assert hasattr(cls, '__src_dst_assoc__'), \
            'You must declare __src_dst_assoc__ for {}'.format(cls)
        assert hasattr(cls, '__dst_src_assoc__'), \
            'You must declare __dst_src_assoc__ for {}'.format(cls)


class Edge(AbstractConcreteBase, ORMBase, DeclareLastEdgeMixin):
    __src_table__ = None
    __dst_table__ = None

    src_id, dst_id, src, dst = None, None, None, None

    @declared_attr
    def src_id(cls):
        src_table = cls.__src_table__

        if not src_table and hasattr(cls, "__src_class__"):
            src_table = NODE_TABLENAME_SCHEME.format(class_name=cls.__src_class__.lower())

        return id_column(src_table, cls.__name__)

    @declared_attr
    def dst_id(cls):

        dst_table = cls.__dst_table__

        if not dst_table and hasattr(cls, "__dst_class__"):
            dst_table = NODE_TABLENAME_SCHEME.format(class_name=cls.__dst_class__.lower())
        return id_column(dst_table, cls.__name__)

    @declared_attr
    def __table_args__(cls):
        return (
            sqlalchemy.Index('{}_dst_id_src_id_idx'.format(cls.__tablename__),
                             "src_id", "dst_id"),
            sqlalchemy.Index('{}_dst_id'.format(cls.__tablename__), "dst_id"),
            sqlalchemy.Index('{}_src_id'.format(cls.__tablename__), "src_id"),
        )

    @declared_attr
    def __tablename__(cls):
        return EDGE_TABLENAME_SCHEME.format(class_name=cls.__name__.lower())

    def __init__(self, src_id=None, dst_id=None, properties=None,
                 acl=None, system_annotations=None, label=None,
                 src=None, dst=None, **kwargs):
        self._props = {}
        self.system_annotations = system_annotations or {}
        self.acl = acl or []
        self.properties = properties or {}
        self.properties.update(kwargs)

        if src is not None:
            if src_id is not None:
                assert src.node_id == src_id, (
                    "Edge initialized with src.node_id and src_id"
                    "that don't match.")
            self.src = src
            self.src_id = src.node_id
        else:
            self.src_id = src_id

        if dst is not None:
            if dst_id is not None:
                assert dst.node_id == dst_id, (
                    "Edge initialized with dst.node_id and dst_id"
                    "that don't match.")
            self.dst = dst
            self.dst_id = dst.node_id
        else:
            self.dst_id = dst_id

    def to_json(self):
        assert self.src and self.dst, (
            "src or dst is not set on the edge. Sync with the database first "
            "to set the src and dst association proxy.")

        return {
            'src_id': self.src_id,
            'dst_id': self.dst_id,
            'src_label': self.src.label,
            'dst_label': self.dst.label,
            'label': self.label,
            'acl': self.acl,
            'properties': self.properties,
            'system_annotations': self.system_annotations,
        }

    @classmethod
    def from_json(cls, edge_json):
        if cls is Edge:
            Type = Edge.get_unique_subclass(edge_json['src_label'],
                                            edge_json['label'],
                                            edge_json['dst_label'])
            if not Type:
                raise KeyError('Edge has no subclass named {}'
                               .format(edge_json['label']))
        else:
            Type = cls

        return Type(src_id=edge_json['src_id'],
                    dst_id=edge_json['dst_id'],
                    label=edge_json['label'],
                    acl=edge_json['acl'],
                    properties=edge_json['properties'],
                    system_annotations=edge_json['system_annotations'])

    def __repr__(self):
        return '<{}(({})-[{}]->({})>'.format(
            self.__class__.__name__, self.src_id, self.label, self.dst_id)

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
                'More than one Edge with label {} found, try get_unique_subclass()'
                'to resolve type using src and dst labels: {}'.format(
                    label, scls))
        if not scls:
            return None
        return scls[0]

    @classmethod
    def get_unique_subclass(cls, src_label, label, dst_label):
        """Determines a subclass based on the src and dst.
        """
        src_class = Node.get_subclass(src_label).__name__
        dst_class = Node.get_subclass(dst_label).__name__
        scls = [c for c in cls.__subclasses__()
                if c.get_label() == label
                and c.__src_class__ == src_class
                and c.__dst_class__ == dst_class]
        if len(scls) > 1:
            raise KeyError(
                'More than one Edge with label {} found: {}'.format(
                    label, scls))
        if not scls:
            return None
        return scls[0]

    @classmethod
    def _get_subclasses_labeled(cls, label):
        return [c for c in cls.__subclasses__()
                if c.get_label() == label]

    @classmethod
    def _get_edges_with_src(cls, src_class_name):
        return [c for c in cls.__subclasses__()
                if c.__src_class__ == src_class_name]

    @classmethod
    def _get_edges_with_dst(cls, dst_class_name):
        return [c for c in cls.__subclasses__()
                if c.__dst_class__ == dst_class_name]

    @staticmethod
    def get_subclass_table_names():
        return [s.__tablename__ for s in Edge.__subclasses__()]

    @classmethod
    def get_subclasses(cls):
        return [s for s in cls.__subclasses__()]

    def _snapshot_existing(self, session, old_props, old_sysan):
        temp = self.__class__(self.src_id, self.dst_id, old_props, self.acl,
                              old_sysan, self.label)
        voided = VoidedEdge(temp)
        session.add(voided)


def PolyEdge(src_id=None, dst_id=None, label=None, acl=None, system_annotations=None, properties=None):
    if not label:
        raise AttributeError('You cannot create a PolyEdge without a label.')
    try:
        edge_type_class = Edge.get_subclass(label)
    except Exception as e:
        raise RuntimeError((
                               "{}: Unable to determine edge type. If there are more than one "
                               "edges with label {}, you need to specify src_label and dst_label"
                               "using the PsqlGraphDriver.get_PolyEdge())"
                           ).format(e, label))

    return edge_type_class(
        src_id=src_id,
        dst_id=dst_id,
        properties=properties or {},
        acl=acl or [],
        system_annotations=system_annotations or {},
        label=label
    )


# Node and Edge classes depend on each other so this needs to be done down here
from psqlgraph.node import Node

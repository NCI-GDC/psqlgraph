# from edge import Edge, PsqlEdge, PsqlVoidedEdge
from node import Node
from edge import Edge
from sqlalchemy.orm import Query, joinedload
from sqlalchemy import or_, not_


class GraphQuery(Query):
    """Query subclass implementing graph specific operations."""

    def _iterable(self, val):
        if hasattr(val, '__iter__'):
            return val
        else:
            return (val,)

    def entity(self):
        return self._entity_zero().type

    # ======== Edges ========
    def with_edge_to_node(self, edge_label, target_node):
        return self.filter(Node.node_id == target_node.node_id)\
                   .filter(Edge._label == edge_label)

    def with_edge_from_node(self, edge_label, source_node):
        # first we construct a subquery for edges of the correct label
        # to the target_node
        session = self.session
        sq = session.query(Edge).filter(Edge.label == edge_label)\
                                .filter(Edge.src_id == source_node.node_id)\
                                .subquery()
        return self.filter(Node.node_id == sq.c.dst_id)

    def src(self, src_id):
        raise NotImplemented()

    def dst(self, dst_id):
        raise NotImplemented()

    # ====== Nodes ========
    def has_edges(self):
        raise NotImplemented()

    def has_no_edges(self):
        raise NotImplemented()

    def _path(self, labels, edges, node, reset=False):
        for label in self._iterable(labels):
            self = self.outerjoin(
                edges, node, aliased=True, from_joinpoint=True
            ).filter(Node.label == label)
            if reset:
                self = self.reset_joinpoint()
            return self

    def path_out(self, labels, reset=False):
        return self._path(labels, 'edges_out', 'dst', reset)

    def path_in(self, labels, reset=False):
        raise NotImplemented()

    def path_in_end(self, labels):
        raise NotImplemented()

    def path_out_end(self, labels):
        raise NotImplemented()

    def path_end(self, labels):
        return self

    def path(self, *entities):
        r = relationships[0]
        entity = self.entity()
        assert entity != Node, (
            'Path walk not supported on generic node class')

    def path2(self, *classes, **kwargs):
        if not classes:
            return self
        last_entity = kwargs.pop('start', None)
        if not last_entity:
            last_entity = self.entity()
        next_entity = classes[0]
        edges = Edge._get_edges_between(
            last_entity.__name__, next_entity.__name__)
        if not edges:
            raise RuntimeError('No edge between {} and {}'.format(
                last_entity, next_entity))
        edge = edges[0]

        session = self.session
        print last_entity, edge, next_entity
        sq = session.query(edge).filter(edge.src_id == last_entity.node_id)\
                                .subquery()
        return self.filter(next_entity.node_id == sq.c.dst_id)

    def ids_path_end(self, ids, labels):
        raise NotImplemented()

    def ids(self, ids):
        _id = self.entity().node_id
        if hasattr(ids, '__iter__'):
            return self.filter(_id.in_(ids))
        else:
            return self.filter(_id == str(ids))

    def src_ids(self, ids):
        assert hasattr(Edge, 'src_id')
        if hasattr(ids, '__iter__'):
            return self.filter(Edge.src_id.in_(ids))
        else:
            return self.filter(Edge.src_id == str(ids))

    def dst_ids(self, ids):
        assert hasattr(Edge, 'src_id')
        if hasattr(ids, '__iter__'):
            return self.filter(Edge.dst_id.in_(ids))
        else:
            return self.filter(Edge.dst_id == str(ids))

    def neighbors(self):
        raise NotImplemented()

    def load_edges(self):
        raise NotImplemented()

    def load_neighbors(self):
        raise NotImplemented()

    def _flatten_tree(self, tree):
        raise NotImplemented()

    @staticmethod
    def _reconstruct_tree(node, nodes, doc, tree, visited):
        raise NotImplemented()

    def tree(self, root_id, tree):
        raise NotImplemented()

    def path_whole(self, path):
        raise NotImplemented()

    def path_linked(self, src_id, path):
        raise NotImplemented()

    def not_ids(self, ids):
        _id = self.entity().node_id
        if hasattr(ids, '__iter__'):
            return self.filter(not_(_id.in_(ids)))
        else:
            return self.filter(not_(_id == str(ids)))

    # ======== Labels ========
    def labels(self, labels):
        entity = self.entity()
        if hasattr(labels, '__iter__'):
            return self.filter(entity._label.in_(labels))
        else:
            return self.filter(entity._label == labels)

    def not_labels(self, labels):
        raise NotImplemented()

    # ======== Properties ========
    def props(self, props):
        assert isinstance(props, dict)
        return self.filter(self.entity()._props.contains(props))

    def not_props(self, props):
        raise NotImplemented()

    def prop_in(self, key, values):
        raise NotImplemented()

    def prop(self, key, value):
        raise NotImplemented()

    def has_props(self, keys):
        raise NotImplemented()

    def null_props(self, keys):
        raise NotImplemented()

    # ======== System Annotations ========
    def sysan(self, sysans):
        assert isinstance(sysans, dict)
        return self.filter(self.entity()._sysan.contains(sysans))

    def not_sysan(self, sysans):
        raise NotImplemented()

    def not_sysan_in(self, key, values):
        raise NotImplemented()

    def has_sysan(self, keys):
        if isinstance(keys, str):
            keys = [keys]
        for key in keys:
            self = self.filter(self.entity()._sysan.has_key(key))
        return self

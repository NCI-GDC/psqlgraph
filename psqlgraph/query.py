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
    def with_edge_to_node(self, edge_type, target_node):
        session = self.session
        sq = session.query(edge_type).filter(
            edge_type.dst_id == target_node.node_id).subquery()
        return self.filter(self.entity().node_id == sq.c.src_id)

    def with_edge_from_node(self, edge_type, source_node):
        session = self.session
        sq = session.query(edge_type).filter(
            edge_type.src_id == source_node.node_id).subquery()
        return self.filter(self.entity().node_id == sq.c.src_id)

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
        raise NotImplemented()

    def spath(self, *entities):
        """g.nodes(Participant).path('samples', 'aliquots').filter()"""
        last = self.entity()
        assert last != Node, 'Please narrow your search.'
        for e in entities:
            self = self.join(*getattr(last, e).attr)
            last = self._join_entities[-1].class_
        return self

    def path(self, *entities):
        """ g.nodes(Participant).path(
                Participant.samples, Sample.aliquots).filter()
        """
        for e in entities:
            self = self.join(*e.attr)
        return self

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

    def _construct_any(self, path, final, props):
        if len(path) == 0:
            return final._props.contains(props)
        return path[0].any(self._construct_any(path[1:], final, props))

    def any(self, path, final, props):
        return self.filter(self._construct_any(path, final, props))

    def _construct_id(self, path, final, node_id):
        if not path:
            return (final.node_id == node_id)
        print path[0]
        return path[0].any(self._construct_id(path[1:], final, node_id))

    def path_to(self, path, final, node_id):
        return self.filter(self._construct_id(path, final, node_id))

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
        return self.filter(self.entity()._props.contains({key: value}))

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
